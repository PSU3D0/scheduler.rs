use rand::prelude::*;
#[cfg(not(feature = "crypto_rand"))]
use rand::rngs::StdRng;
#[cfg(feature = "crypto_rand")]
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use threadpool::ThreadPool;
use uuid::Uuid;

/// Error type for scheduler operations
#[derive(Debug)]
pub enum SchedulerError {
    /// Error acquiring lock for internal data
    LockError(String),
    /// Error with time source
    TimeError(String),
    /// Error during serialization
    SerializationError(String),
    /// Other error
    Other(String),
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchedulerError::LockError(msg) => write!(f, "Lock error: {}", msg),
            SchedulerError::TimeError(msg) => write!(f, "Time error: {}", msg),
            SchedulerError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            SchedulerError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl Error for SchedulerError {}

// Convert poison errors to scheduler errors
impl<T> From<PoisonError<T>> for SchedulerError {
    fn from(err: PoisonError<T>) -> Self {
        SchedulerError::LockError(format!("Lock poisoned: {}", err))
    }
}

// ----------------- SECTION 1: Core Types -----------------

/// A lock-free store for schedule data
pub struct LockFreeScheduleStore {
    ptr: AtomicPtr<HashMap<String, Schedule>>,
}

impl Default for LockFreeScheduleStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LockFreeScheduleStore {
    /// Creates a new empty schedule store.
    pub fn new() -> Self {
        let map = Box::new(HashMap::new());
        Self {
            ptr: AtomicPtr::new(Box::into_raw(map)),
        }
    }

    /// Creates a new schedule store with the given initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let map = Box::new(HashMap::with_capacity(capacity));
        Self {
            ptr: AtomicPtr::new(Box::into_raw(map)),
        }
    }

    /// Gets a schedule by its ID, if it exists.
    pub fn get(&self, id: &str) -> Option<Schedule> {
        // Get a consistent view of the current map
        let ptr = self.ptr.load(Ordering::Acquire);
        let map = unsafe { &*ptr };
        map.get(id).cloned()
    }

    /// Gets all schedules in the store.
    pub fn get_all(&self) -> Vec<Schedule> {
        let ptr = self.ptr.load(Ordering::Acquire);
        let map = unsafe { &*ptr };
        map.values().cloned().collect()
    }

    /// Inserts a schedule into the store.
    pub fn insert(&self, id: String, schedule: Schedule) {
        // Create a new map with the updated data
        let current_ptr = self.ptr.load(Ordering::Acquire);
        let current_map = unsafe { &*current_ptr };

        // Create new map with inserted value
        let mut new_map = current_map.clone();
        new_map.insert(id, schedule);
        let new_ptr = Box::into_raw(Box::new(new_map));

        // Try to swap in the new map
        match self
            .ptr
            .compare_exchange(current_ptr, new_ptr, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                // Success - free the old map
                unsafe {
                    drop(Box::from_raw(current_ptr));
                }
            }
            Err(_) => {
                // Another thread updated the map, discard our update
                unsafe {
                    drop(Box::from_raw(new_ptr));
                }
            }
        }
    }

    /// Removes a schedule from the store, returning the removed schedule if it existed.
    pub fn remove(&self, id: &str) -> Option<Schedule> {
        // Get current map
        let current_ptr = self.ptr.load(Ordering::Acquire);
        let current_map = unsafe { &*current_ptr };

        // Check if the key exists
        if !current_map.contains_key(id) {
            return None;
        }

        // Create new map without the removed schedule
        let mut new_map = current_map.clone();
        let removed = new_map.remove(id);
        let new_ptr = Box::into_raw(Box::new(new_map));

        // Try to swap in the new map
        match self
            .ptr
            .compare_exchange(current_ptr, new_ptr, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                // Success - free the old map
                unsafe {
                    drop(Box::from_raw(current_ptr));
                }
                removed
            }
            Err(_) => {
                // Another thread updated the map, discard our update
                unsafe {
                    drop(Box::from_raw(new_ptr));
                }
                // Try again (could be optimized with a retry limit)
                self.remove(id)
            }
        }
    }

    /// Checks if the store contains a schedule with the given ID.
    pub fn contains_key(&self, id: &str) -> bool {
        let ptr = self.ptr.load(Ordering::Acquire);
        let map = unsafe { &*ptr };
        map.contains_key(id)
    }

    /// Returns the number of schedules in the store.
    pub fn len(&self) -> usize {
        let ptr = self.ptr.load(Ordering::Acquire);
        let map = unsafe { &*ptr };
        map.len()
    }

    /// Returns true if the store contains no schedules.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes all schedules from the store.
    pub fn clear(&self) {
        let current_ptr = self.ptr.load(Ordering::Acquire);
        let new_map = Box::new(HashMap::new());
        let new_ptr = Box::into_raw(new_map);

        // Try to swap in the empty map
        match self
            .ptr
            .compare_exchange(current_ptr, new_ptr, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                // Success - free the old map
                unsafe {
                    drop(Box::from_raw(current_ptr));
                }
            }
            Err(_) => {
                // Another thread updated the map, discard our update
                unsafe {
                    drop(Box::from_raw(new_ptr));
                }
                // Try again
                self.clear();
            }
        }
    }
}

impl Drop for LockFreeScheduleStore {
    fn drop(&mut self) {
        let ptr = self.ptr.load(Ordering::Acquire);
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }
}

/// A compact ID type to reduce allocations
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CompactId([u8; 16]);

impl Default for CompactId {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactId {
    pub fn new() -> Self {
        let uuid = Uuid::new_v4();
        let bytes = uuid.into_bytes();
        Self(bytes)
    }

    pub fn from_string(s: &str) -> Result<Self, SchedulerError> {
        match Uuid::parse_str(s) {
            Ok(uuid) => Ok(Self(uuid.into_bytes())),
            Err(e) => Err(SchedulerError::Other(format!("Invalid ID format: {}", e))),
        }
    }
}

impl From<Uuid> for CompactId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid.into_bytes())
    }
}

impl From<CompactId> for Uuid {
    fn from(id: CompactId) -> Self {
        Uuid::from_bytes(id.0)
    }
}

impl TryFrom<&str> for CompactId {
    type Error = SchedulerError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_string(s)
    }
}

impl TryFrom<String> for CompactId {
    type Error = SchedulerError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_string(&s)
    }
}

impl Display for CompactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let uuid = Uuid::from_bytes(self.0);
        write!(f, "{}", uuid)
    }
}

impl Serialize for CompactId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CompactId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        CompactId::from_string(&s).map_err(serde::de::Error::custom)
    }
}

/// Configuration options for customizing scheduler behavior.
pub struct SchedulerConfig {
    /// Number of threads to use in the thread pool
    pub thread_count: usize,
    /// Maximum capacity for the schedule store
    pub store_capacity: usize,
    /// Custom time source, if not specified a default will be used
    pub time_source: Option<Box<dyn TimeSource>>,
    /// RNG seed for deterministic jitter (if None, uses system random)
    pub rng_seed: Option<u64>,
}

impl Clone for SchedulerConfig {
    fn clone(&self) -> Self {
        Self {
            thread_count: self.thread_count,
            store_capacity: self.store_capacity,
            time_source: None, // We don't clone time sources
            rng_seed: self.rng_seed,
        }
    }
}

impl std::fmt::Debug for SchedulerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerConfig")
            .field("thread_count", &self.thread_count)
            .field("store_capacity", &self.store_capacity)
            .field("time_source", &"<TimeSource>")
            .field("rng_seed", &self.rng_seed)
            .finish()
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            thread_count: std::cmp::max(4, num_cpus::get()),
            store_capacity: 64, // Default capacity for collections
            time_source: None,
            rng_seed: None,
        }
    }
}

/// Wrapper for scheduler functions that need to be accessible in the worker thread
/// Type for calculating the next execution time of a schedule
type NextExecutionCalculator = Box<dyn Fn(&Schedule, u64) -> u64 + Send + Sync>;

/// Type for checking if a schedule has reached its execution limits
type LimitsChecker = Box<dyn Fn(&Schedule, u64) -> bool + Send + Sync>;

/// Wrapper for scheduler functions that need to be accessible in the worker thread
struct SchedulerFunctions {
    calculate_next_execution: NextExecutionCalculator,
    has_reached_limits: LimitsChecker,
}

/// A scheduler for managing operations across multiple time scales.
pub struct Scheduler {
    schedules: Arc<LockFreeScheduleStore>,
    callback_registry: Arc<Mutex<HashMap<String, Box<dyn CallbackHandler>>>>,
    event_buses: Arc<Mutex<HashMap<String, Box<dyn EventBus>>>>,
    time_source: Arc<Box<dyn TimeSource>>,
    running: Arc<AtomicBool>,
    last_tick: Arc<AtomicU64>,
    thread_pool: Arc<ThreadPool>,
    #[cfg(feature = "crypto_rand")]
    rng: Arc<Mutex<Option<ChaCha8Rng>>>,
    #[cfg(not(feature = "crypto_rand"))]
    rng: Arc<Mutex<Option<StdRng>>>,
}

/// Defines the type of scheduling pattern to use
#[derive(Clone, Debug)]
pub enum ScheduleType {
    /// Fixed interval schedule
    Fixed(Duration),
    /// Jittered schedule (base interval + random jitter)
    Jitter { base: Duration, jitter: Duration },
    /// Exponential backoff (increases duration after each execution)
    Exponential {
        initial: Duration,
        factor: f64,
        max: Option<Duration>,
    },
    /// Decaying schedule (gradually changing interval)
    Decay {
        initial: Duration,
        target: Duration,
        half_life: Duration,
    },
}

impl ScheduleType {
    /// Gets the base interval for this schedule type
    pub fn base_interval(&self) -> Duration {
        match self {
            ScheduleType::Fixed(interval) => *interval,
            ScheduleType::Jitter { base, .. } => *base,
            ScheduleType::Exponential { initial, .. } => *initial,
            ScheduleType::Decay { initial, .. } => *initial,
        }
    }
}

/// Limits for schedule execution
#[derive(Clone, Debug, Default)]
pub struct ScheduleLimits {
    /// Maximum number of executions (None = unlimited)
    pub max_executions: Option<u64>,
    /// Maximum total runtime for this schedule (None = unlimited)
    pub max_runtime: Option<Duration>,
}

/// An individual schedule definition with timing and callback information.
#[derive(Clone, Debug)]
pub struct Schedule {
    id: String,
    name: String,
    schedule_type: ScheduleType,
    next_execution: Option<u64>,
    callback_id: Option<String>,
    event_buses: Vec<String>,
    execution_count: u64,
    created_at: u64,
    limits: ScheduleLimits,
}

/// Event structure for schedule ticks.
#[derive(Clone, Debug)]
pub struct TickEvent {
    pub id: String,
    pub schedule_name: String,
    pub timestamp: u64,
    pub metadata: HashMap<String, String>,
}

/// Serializable representation of a schedule type
#[derive(Serialize, Deserialize)]
enum SerializedScheduleType {
    Fixed {
        interval_ms: u64,
    },
    Jitter {
        base_ms: u64,
        jitter_ms: u64,
    },
    Exponential {
        initial_ms: u64,
        factor: f64,
        max_ms: Option<u64>,
    },
    Decay {
        initial_ms: u64,
        target_ms: u64,
        half_life_ms: u64,
    },
}

/// Serializable representation of schedule limits
#[derive(Serialize, Deserialize)]
struct SerializedLimits {
    max_executions: Option<u64>,
    max_runtime_ms: Option<u64>,
}

/// Serializable representation of a schedule.
#[derive(Serialize, Deserialize)]
struct SerializedSchedule {
    id: String,
    name: String,
    schedule_type: SerializedScheduleType,
    next_execution: Option<u64>,
    callback_id: Option<String>,
    event_buses: Vec<String>,
    execution_count: u64,
    created_at: u64,
    limits: SerializedLimits,
}

/// Serializable scheduler state for persistence.
#[derive(Serialize, Deserialize)]
pub struct SchedulerState {
    schedules: Vec<SerializedSchedule>,
    last_tick_time: u64,
}

// ----------------- SECTION 2: Traits -----------------

/// Handler trait for callbacks triggered by schedules.
pub trait CallbackHandler: Send + Sync {
    /// Handle a tick event synchronously.
    fn handle(&self, event: TickEvent);

    /// Handle a tick event asynchronously.
    fn handle_async<'a>(
        &'a self,
        event: TickEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// Event bus for emitting tick events.
pub trait EventBus: Send + Sync {
    /// Emit a tick event to this bus.
    fn emit(&self, event: TickEvent);

    /// Get the name of this event bus.
    fn name(&self) -> &str;
}

/// Source of time for the scheduler.
pub trait TimeSource: Send + Sync {
    /// Get the current time in milliseconds since the UNIX epoch.
    fn now(&self) -> Result<u64, SchedulerError>;

    /// Whether this source is monotonic
    fn is_monotonic(&self) -> bool;
}

// ----------------- SECTION 3: Implementations -----------------

impl Scheduler {
    /// Create a new scheduler with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new scheduler with custom configuration.
    pub fn with_config(config: SchedulerConfig) -> Self {
        // Create time source
        let time_source: Box<dyn TimeSource> = match config.time_source {
            Some(source) => source,
            None => match MonotonicTimeSource::new() {
                Ok(source) => Box::new(source),
                Err(e) => {
                    eprintln!(
                        "Failed to create monotonic time source, falling back to system time: {}",
                        e
                    );
                    Box::new(SystemTimeSource)
                }
            },
        };

        // Initialize RNG if a seed is provided
        #[cfg(feature = "crypto_rand")]
        let rng = config.rng_seed.map(ChaCha8Rng::seed_from_u64);
        #[cfg(not(feature = "crypto_rand"))]
        let rng = config.rng_seed.map(StdRng::seed_from_u64);

        Scheduler {
            schedules: Arc::new(LockFreeScheduleStore::new()),
            callback_registry: Arc::new(Mutex::new(HashMap::with_capacity(config.store_capacity))),
            event_buses: Arc::new(Mutex::new(HashMap::with_capacity(config.store_capacity))),
            time_source: Arc::new(time_source),
            running: Arc::new(AtomicBool::new(false)),
            last_tick: Arc::new(AtomicU64::new(0)),
            thread_pool: Arc::new(ThreadPool::new(config.thread_count)),
            rng: Arc::new(Mutex::new(rng)),
        }
    }

    /// Add a schedule using a schedule definition
    pub fn add_schedule(&self, definition: ScheduleDefinition) -> Result<String, SchedulerError> {
        let id = Uuid::new_v4().to_string();
        let name = definition
            .name
            .unwrap_or_else(|| format!("schedule_{}", id));

        // Get current time for created_at timestamp
        let now = self.time_source.now()?;

        let schedule = Schedule {
            id: id.clone(),
            name,
            schedule_type: definition.schedule_type,
            next_execution: None,
            callback_id: definition.callback_id,
            event_buses: definition.event_buses,
            execution_count: 0,
            created_at: now,
            limits: definition.limits,
        };

        self.schedules.insert(id.clone(), schedule);

        Ok(id)
    }

    /// Calculate the next execution time based on the schedule type and execution count
    pub fn calculate_next_execution(&self, schedule: &Schedule, now: u64) -> u64 {
        match &schedule.schedule_type {
            ScheduleType::Fixed(interval) => now + interval.as_millis() as u64,
            ScheduleType::Jitter { base, jitter } => {
                let jitter_ms = if let Ok(mut rng_guard) = self.rng.lock() {
                    if let Some(rng) = rng_guard.as_mut() {
                        // Use deterministic RNG if configured
                        rng.gen_range(0..jitter.as_millis() as u64)
                    } else {
                        // Use thread-local random if no seed provided
                        thread_rng().gen_range(0..jitter.as_millis() as u64)
                    }
                } else {
                    // Fallback if lock fails
                    thread_rng().gen_range(0..jitter.as_millis() as u64)
                };

                now + base.as_millis() as u64 + jitter_ms
            }
            ScheduleType::Exponential {
                initial,
                factor,
                max,
            } => {
                let exec_count = schedule.execution_count as f64;
                let interval = initial.as_millis() as f64 * factor.powf(exec_count);

                // Respect the maximum if provided
                if let Some(max_interval) = max {
                    let capped_interval = (interval as u64).min(max_interval.as_millis() as u64);
                    now + capped_interval
                } else {
                    now + interval as u64
                }
            }
            ScheduleType::Decay {
                initial,
                target,
                half_life,
            } => {
                let elapsed = now.saturating_sub(schedule.created_at);
                let half_life_ms = half_life.as_millis() as f64;

                if half_life_ms == 0.0 {
                    now + target.as_millis() as u64
                } else {
                    let initial_ms = initial.as_millis() as f64;
                    let target_ms = target.as_millis() as f64;

                    // Exponential decay formula: initial + (target - initial) * (1 - e^(-elapsed/half_life))
                    let decay_factor = 1.0 - (-(elapsed as f64) / half_life_ms).exp();
                    let current_interval = initial_ms + (target_ms - initial_ms) * decay_factor;

                    now + current_interval as u64
                }
            }
        }
    }

    /// Check if a schedule has reached its execution limits
    pub fn has_reached_limits(&self, schedule: &Schedule, now: u64) -> bool {
        // Check max executions
        if let Some(max_exec) = schedule.limits.max_executions {
            if schedule.execution_count >= max_exec {
                return true;
            }
        }

        // Check max runtime
        if let Some(max_runtime) = schedule.limits.max_runtime {
            let elapsed_since_creation = now.saturating_sub(schedule.created_at);
            if elapsed_since_creation >= max_runtime.as_millis() as u64 {
                return true;
            }
        }

        false
    }

    /// Start a schedule definition with the specified interval.
    pub fn every<T: Into<Duration>>(&mut self, duration: T) -> ScheduleBuilder {
        ScheduleBuilder::new(self, duration.into())
    }

    /// Schedule a pre-defined schedule
    pub fn schedule(&self, definition: ScheduleDefinition) -> Result<String, SchedulerError> {
        self.add_schedule(definition)
    }

    /// Register a callback handler with the specified ID.
    pub fn register_callback(
        &mut self,
        id: impl Into<String>,
        handler: Box<dyn CallbackHandler>,
    ) -> Result<&mut Self, SchedulerError> {
        let id = id.into();
        self.callback_registry.lock()?.insert(id, handler);
        Ok(self)
    }

    /// Register an event bus with the specified ID.
    pub fn register_event_bus(
        &mut self,
        id: impl Into<String>,
        bus: Box<dyn EventBus>,
    ) -> Result<&mut Self, SchedulerError> {
        let id = id.into();
        self.event_buses.lock()?.insert(id, bus);
        Ok(self)
    }

    /// Start the scheduler.
    pub fn start(&self) {
        self.running.store(true, Ordering::Release);

        // Create clones of all the Arc references for the thread
        let schedules = Arc::clone(&self.schedules);
        let callback_registry = Arc::clone(&self.callback_registry);
        let event_buses = Arc::clone(&self.event_buses);
        let time_source = Arc::clone(&self.time_source);
        let running = Arc::clone(&self.running);
        let last_tick = Arc::clone(&self.last_tick);
        let thread_pool = Arc::clone(&self.thread_pool);
        // Create copies of methods we need to use in the thread
        let calculate_func = {
            let rng_arc = Arc::clone(&self.rng);
            Box::new(move |schedule: &Schedule, now: u64| -> u64 {
                match &schedule.schedule_type {
                    ScheduleType::Fixed(interval) => now + interval.as_millis() as u64,
                    ScheduleType::Jitter { base, jitter } => {
                        let jitter_ms = if let Ok(mut rng_guard) = rng_arc.lock() {
                            if let Some(rng) = rng_guard.as_mut() {
                                // Use deterministic RNG if configured
                                rng.gen_range(0..jitter.as_millis() as u64)
                            } else {
                                // Use thread-local random if no seed provided
                                thread_rng().gen_range(0..jitter.as_millis() as u64)
                            }
                        } else {
                            // Fallback if lock fails
                            thread_rng().gen_range(0..jitter.as_millis() as u64)
                        };

                        now + base.as_millis() as u64 + jitter_ms
                    }
                    ScheduleType::Exponential {
                        initial,
                        factor,
                        max,
                    } => {
                        let exec_count = schedule.execution_count as f64;
                        let interval = initial.as_millis() as f64 * factor.powf(exec_count);

                        // Respect the maximum if provided
                        if let Some(max_interval) = max {
                            let capped_interval =
                                (interval as u64).min(max_interval.as_millis() as u64);
                            now + capped_interval
                        } else {
                            now + interval as u64
                        }
                    }
                    ScheduleType::Decay {
                        initial,
                        target,
                        half_life,
                    } => {
                        let elapsed = now.saturating_sub(schedule.created_at);
                        let half_life_ms = half_life.as_millis() as f64;

                        if half_life_ms == 0.0 {
                            now + target.as_millis() as u64
                        } else {
                            let initial_ms = initial.as_millis() as f64;
                            let target_ms = target.as_millis() as f64;

                            // Exponential decay formula: initial + (target - initial) * (1 - e^(-elapsed/half_life))
                            let decay_factor = 1.0 - (-(elapsed as f64) / half_life_ms).exp();
                            let current_interval =
                                initial_ms + (target_ms - initial_ms) * decay_factor;

                            now + current_interval as u64
                        }
                    }
                }
            }) as Box<dyn Fn(&Schedule, u64) -> u64 + Send + Sync>
        };

        // Create limits checking function
        let limits_func = Box::new(move |schedule: &Schedule, now: u64| -> bool {
            // Check max executions
            if let Some(max_exec) = schedule.limits.max_executions {
                if schedule.execution_count >= max_exec {
                    return true;
                }
            }

            // Check max runtime
            if let Some(max_runtime) = schedule.limits.max_runtime {
                let elapsed_since_creation = now.saturating_sub(schedule.created_at);
                if elapsed_since_creation >= max_runtime.as_millis() as u64 {
                    return true;
                }
            }

            false
        }) as Box<dyn Fn(&Schedule, u64) -> bool + Send + Sync>;

        // Create scheduler functions bundle
        let scheduler_funcs = Arc::new(SchedulerFunctions {
            calculate_next_execution: calculate_func,
            has_reached_limits: limits_func,
        });

        thread::spawn(move || {
            while running.load(Ordering::Acquire) {
                // Get current time, handling errors
                let now = match time_source.now() {
                    Ok(time) => time,
                    Err(e) => {
                        eprintln!("Error getting time: {}", e);
                        // Sleep a bit to avoid spinning the CPU on error
                        thread::sleep(Duration::from_millis(100));
                        continue;
                    }
                };

                last_tick.store(now, Ordering::Release);

                let mut due_schedules = Vec::with_capacity(16); // Preallocate

                // Collect schedules that are due
                for schedule in schedules.get_all() {
                    // Skip schedules that have reached their limits
                    if (scheduler_funcs.has_reached_limits)(&schedule, now) {
                        continue;
                    }

                    if let Some(next_execution) = schedule.next_execution {
                        if next_execution <= now {
                            // Calculate the next execution time based on the schedule type
                            let next_time =
                                (scheduler_funcs.calculate_next_execution)(&schedule, now);

                            // Mark the schedule for execution with updated execution count
                            let updated_schedule = Schedule {
                                id: schedule.id.clone(),
                                name: schedule.name.clone(),
                                schedule_type: schedule.schedule_type.clone(),
                                next_execution: Some(next_time),
                                callback_id: schedule.callback_id.clone(),
                                event_buses: schedule.event_buses.clone(),
                                execution_count: schedule.execution_count + 1,
                                created_at: schedule.created_at,
                                limits: schedule.limits.clone(),
                            };

                            schedules.insert(schedule.id.clone(), updated_schedule.clone());
                            due_schedules.push((schedule.id.clone(), updated_schedule));
                        }
                    } else {
                        // First-time execution - calculate initial next execution time
                        let next_time = (scheduler_funcs.calculate_next_execution)(&schedule, now);

                        let updated_schedule = Schedule {
                            id: schedule.id.clone(),
                            name: schedule.name.clone(),
                            schedule_type: schedule.schedule_type.clone(),
                            next_execution: Some(next_time),
                            callback_id: schedule.callback_id.clone(),
                            event_buses: schedule.event_buses.clone(),
                            execution_count: 0, // First execution happens on next tick
                            created_at: schedule.created_at,
                            limits: schedule.limits.clone(),
                        };

                        schedules.insert(schedule.id.clone(), updated_schedule);
                    }
                }

                // Process due schedules
                for (id, schedule) in due_schedules {
                    let metadata = HashMap::with_capacity(4); // Preallocate with expected size
                    let event = TickEvent {
                        id: id.clone(),
                        schedule_name: schedule.name.clone(),
                        timestamp: now,
                        metadata,
                    };

                    // Execute callback if configured
                    if let Some(callback_id) = &schedule.callback_id {
                        // Clone the Arc reference to the callback registry
                        let callback_registry_clone = Arc::clone(&callback_registry);
                        let callback_id = callback_id.clone();
                        let event_clone = event.clone();

                        // Execute the callback in the thread pool
                        thread_pool.execute(move || {
                            // Access the handler from the registry, handling lock errors
                            match callback_registry_clone.lock() {
                                Ok(registry) => {
                                    if let Some(handler) = registry.get(&callback_id) {
                                        // Catch and log panics in handlers to prevent thread pool depletion
                                        let result = std::panic::catch_unwind(
                                            std::panic::AssertUnwindSafe(|| {
                                                handler.handle(event_clone);
                                            }),
                                        );

                                        if let Err(panic) = result {
                                            eprintln!(
                                                "Handler panicked for schedule '{}': {:?}",
                                                callback_id, panic
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error acquiring callback registry lock: {}", e);
                                }
                            }
                        });
                    }

                    // Emit to configured event buses
                    for bus_id in &schedule.event_buses {
                        let event_clone = event.clone();
                        let event_buses_clone = Arc::clone(&event_buses);
                        let bus_id_clone = bus_id.clone();

                        // Execute the event bus emission in the thread pool
                        thread_pool.execute(move || {
                            // Access the event bus, handling lock errors
                            match event_buses_clone.lock() {
                                Ok(buses) => {
                                    if let Some(bus) = buses.get(&bus_id_clone) {
                                        // Catch and log panics in event buses
                                        let result = std::panic::catch_unwind(
                                            std::panic::AssertUnwindSafe(|| {
                                                bus.emit(event_clone);
                                            }),
                                        );

                                        if let Err(panic) = result {
                                            eprintln!(
                                                "Event bus '{}' panicked: {:?}",
                                                bus_id_clone, panic
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error acquiring event buses lock: {}", e);
                                }
                            }
                        });
                    }
                }

                // Calculate how long to sleep until the next scheduled task
                // This makes the timing more deterministic than a fixed sleep
                let next_execution = schedules
                    .get_all()
                    .into_iter()
                    .filter_map(|schedule| schedule.next_execution)
                    .min();

                if let Some(next_time) = next_execution {
                    if next_time > now {
                        let sleep_duration = Duration::from_millis(
                            std::cmp::min(next_time - now, 100), // Cap at 100ms to allow for periodic checking
                        );
                        thread::sleep(sleep_duration);
                    } else {
                        // Something is due, continue immediately
                        continue;
                    }
                } else {
                    // No scheduled tasks, sleep for a reasonable interval
                    thread::sleep(Duration::from_millis(50));
                }
            }
        });
    }

    /// Stop the scheduler.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Check if the scheduler is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Get a schedule by its ID.
    pub fn get_schedule(&self, id: &str) -> Option<Schedule> {
        self.schedules.get(id)
    }

    /// Get all schedules managed by this scheduler.
    pub fn get_all_schedules(&self) -> Vec<Schedule> {
        self.schedules.get_all()
    }

    /// Remove a schedule by its ID.
    pub fn remove_schedule(&self, id: &str) -> Option<Schedule> {
        self.schedules.remove(id)
    }

    /// Clear all schedules from the scheduler.
    pub fn clear_schedules(&self) {
        self.schedules.clear();
    }

    /// Freeze the scheduler state for persistence.
    pub fn freeze(&self) -> Result<SchedulerState, SchedulerError> {
        let serialized_schedules = self
            .schedules
            .get_all()
            .into_iter()
            .map(|schedule| {
                // Convert ScheduleType to SerializedScheduleType
                let schedule_type = match schedule.schedule_type {
                    ScheduleType::Fixed(interval) => SerializedScheduleType::Fixed {
                        interval_ms: interval.as_millis() as u64,
                    },
                    ScheduleType::Jitter { base, jitter } => SerializedScheduleType::Jitter {
                        base_ms: base.as_millis() as u64,
                        jitter_ms: jitter.as_millis() as u64,
                    },
                    ScheduleType::Exponential {
                        initial,
                        factor,
                        max,
                    } => SerializedScheduleType::Exponential {
                        initial_ms: initial.as_millis() as u64,
                        factor,
                        max_ms: max.map(|d| d.as_millis() as u64),
                    },
                    ScheduleType::Decay {
                        initial,
                        target,
                        half_life,
                    } => SerializedScheduleType::Decay {
                        initial_ms: initial.as_millis() as u64,
                        target_ms: target.as_millis() as u64,
                        half_life_ms: half_life.as_millis() as u64,
                    },
                };

                // Serialize limits
                let limits = SerializedLimits {
                    max_executions: schedule.limits.max_executions,
                    max_runtime_ms: schedule.limits.max_runtime.map(|d| d.as_millis() as u64),
                };

                SerializedSchedule {
                    id: schedule.id.clone(),
                    name: schedule.name.clone(),
                    schedule_type,
                    next_execution: schedule.next_execution,
                    callback_id: schedule.callback_id.clone(),
                    event_buses: schedule.event_buses.clone(),
                    execution_count: schedule.execution_count,
                    created_at: schedule.created_at,
                    limits,
                }
            })
            .collect();

        Ok(SchedulerState {
            schedules: serialized_schedules,
            last_tick_time: self.last_tick.load(Ordering::Acquire),
        })
    }

    /// Restore a scheduler from a serialized state.
    pub fn restore(state: SchedulerState) -> Result<Self, SchedulerError> {
        let scheduler = Self::default();

        // Restore schedules
        for serialized in state.schedules {
            // Convert SerializedScheduleType to ScheduleType
            let schedule_type = match serialized.schedule_type {
                SerializedScheduleType::Fixed { interval_ms } => {
                    ScheduleType::Fixed(Duration::from_millis(interval_ms))
                }
                SerializedScheduleType::Jitter { base_ms, jitter_ms } => ScheduleType::Jitter {
                    base: Duration::from_millis(base_ms),
                    jitter: Duration::from_millis(jitter_ms),
                },
                SerializedScheduleType::Exponential {
                    initial_ms,
                    factor,
                    max_ms,
                } => ScheduleType::Exponential {
                    initial: Duration::from_millis(initial_ms),
                    factor,
                    max: max_ms.map(Duration::from_millis),
                },
                SerializedScheduleType::Decay {
                    initial_ms,
                    target_ms,
                    half_life_ms,
                } => ScheduleType::Decay {
                    initial: Duration::from_millis(initial_ms),
                    target: Duration::from_millis(target_ms),
                    half_life: Duration::from_millis(half_life_ms),
                },
            };

            // Deserialize limits
            let limits = ScheduleLimits {
                max_executions: serialized.limits.max_executions,
                max_runtime: serialized.limits.max_runtime_ms.map(Duration::from_millis),
            };

            let schedule = Schedule {
                id: serialized.id.clone(),
                name: serialized.name,
                schedule_type,
                next_execution: serialized.next_execution,
                callback_id: serialized.callback_id,
                event_buses: serialized.event_buses,
                execution_count: serialized.execution_count,
                created_at: serialized.created_at,
                limits,
            };

            scheduler.schedules.insert(serialized.id.clone(), schedule);
        }

        // Restore last tick time
        scheduler
            .last_tick
            .store(state.last_tick_time, Ordering::Release);

        Ok(scheduler)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::with_config(SchedulerConfig::default())
    }
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("schedules_count", &self.schedules.len())
            .field("running", &self.running.load(Ordering::Acquire))
            .field("last_tick", &self.last_tick.load(Ordering::Acquire))
            .finish()
    }
}

// ----------------- SECTION 4: Utility Functions and Types -----------------

/// Create a callback handler from a simple closure function.
///
/// This helper function reduces boilerplate when creating callback handlers from closures.
///
/// # Example
/// ```
/// # use schedules::{Scheduler, callback_fn, TickEvent};
/// # use std::time::Duration;
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut scheduler = Scheduler::new();
///
/// // Create a handler from a simple closure
/// scheduler.register_callback(
///     "simple_logger",
///     callback_fn(|event| println!("Event: {}", event.schedule_name))
/// )?;
/// # Ok(())
/// # }
/// ```
pub fn callback_fn<F>(f: F) -> Box<dyn CallbackHandler>
where
    F: Fn(TickEvent) + Send + Sync + Clone + 'static,
{
    Box::new(ClosureHandler { handler: f })
}

/// Default system time source using SystemTime.
struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    fn now(&self) -> Result<u64, SchedulerError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| SchedulerError::TimeError(format!("Failed to get time: {}", e)))
            .map(|duration| duration.as_millis() as u64)
    }

    fn is_monotonic(&self) -> bool {
        false // SystemTime is not monotonic
    }
}

/// Monotonic time source using std::time::Instant
pub struct MonotonicTimeSource {
    // Store origin time and a reference point to convert between epoch time and monotonic time
    origin_instant: Instant,
    origin_millis: u64,
}

impl MonotonicTimeSource {
    pub fn new() -> Result<Self, SchedulerError> {
        // Record the current time in both SystemTime and Instant
        let now_system = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| SchedulerError::TimeError(format!("Failed to get time: {}", e)))?
            .as_millis() as u64;

        let now_instant = Instant::now();

        Ok(Self {
            origin_instant: now_instant,
            origin_millis: now_system,
        })
    }
}

impl TimeSource for MonotonicTimeSource {
    fn now(&self) -> Result<u64, SchedulerError> {
        // Calculate current time by adding the elapsed duration to the origin
        let elapsed = self.origin_instant.elapsed().as_millis() as u64;
        Ok(self.origin_millis + elapsed)
    }

    fn is_monotonic(&self) -> bool {
        true // Instant is monotonic
    }
}

/// Structure for implementing CallbackHandler from closures.
struct ClosureHandler<F: Fn(TickEvent) + Send + Sync + Clone + 'static> {
    handler: F,
}

impl<F: Fn(TickEvent) + Send + Sync + Clone + 'static> Clone for ClosureHandler<F> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
        }
    }
}

impl<F: Fn(TickEvent) + Send + Sync + Clone + 'static> CallbackHandler for ClosureHandler<F> {
    fn handle(&self, event: TickEvent) {
        (self.handler)(event)
    }

    fn handle_async<'a>(
        &'a self,
        event: TickEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        // Simple async wrapper for sync handler
        Box::pin(async move {
            self.handle(event);
        })
    }
}

// ----------------- SECTION 5: Builder APIs -----------------

/// Schedulable can be used to create schedules in a fluent way
/// without needing a mutable reference to a Scheduler instance
#[derive(Clone)]
pub struct ScheduleDefinition {
    schedule_type: ScheduleType,
    name: Option<String>,
    callback_id: Option<String>,
    event_buses: Vec<String>,
    limits: ScheduleLimits,
}

impl ScheduleDefinition {
    /// Create a new schedule definition with a fixed interval
    pub fn every<T: Into<Duration>>(duration: T) -> Self {
        Self {
            schedule_type: ScheduleType::Fixed(duration.into()),
            name: None,
            callback_id: None,
            event_buses: Vec::new(),
            limits: ScheduleLimits::default(),
        }
    }

    /// Set the name for this schedule definition
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Add an additional duration to the schedule interval
    pub fn plus<T: Into<Duration>>(mut self, additional: T) -> Self {
        match &mut self.schedule_type {
            ScheduleType::Fixed(interval) => {
                *interval += additional.into();
            }
            ScheduleType::Jitter { base, .. } => {
                *base += additional.into();
            }
            ScheduleType::Exponential { initial, .. } => {
                *initial += additional.into();
            }
            ScheduleType::Decay {
                initial, target, ..
            } => {
                let add = additional.into();
                *initial += add;
                *target += add;
            }
        }
        self
    }

    /// Set the callback ID for this schedule definition
    pub fn with_callback_id(mut self, id: impl Into<String>) -> Self {
        self.callback_id = Some(id.into());
        self
    }

    /// Add an event bus to emit events to
    pub fn emit_to(mut self, bus_id: impl Into<String>) -> Self {
        self.event_buses.push(bus_id.into());
        self
    }

    /// Add random jitter to the schedule
    pub fn with_jitter<T: Into<Duration>>(mut self, jitter: T) -> Self {
        let jitter_duration = jitter.into();

        // Convert the current schedule type to a jittered one
        match self.schedule_type {
            ScheduleType::Fixed(interval) => {
                self.schedule_type = ScheduleType::Jitter {
                    base: interval,
                    jitter: jitter_duration,
                };
            }
            ScheduleType::Jitter { base, .. } => {
                self.schedule_type = ScheduleType::Jitter {
                    base,
                    jitter: jitter_duration,
                };
            }
            _ => {
                // For other types, use the base interval
                let base = self.schedule_type.base_interval();
                self.schedule_type = ScheduleType::Jitter {
                    base,
                    jitter: jitter_duration,
                };
            }
        }

        self
    }

    /// Create an exponential backoff schedule
    pub fn exponential(mut self, factor: f64, max_interval: Option<Duration>) -> Self {
        let initial = self.schedule_type.base_interval();
        self.schedule_type = ScheduleType::Exponential {
            initial,
            factor,
            max: max_interval,
        };
        self
    }

    /// Create a decaying schedule that changes from the initial interval to the target
    pub fn decay_to<T: Into<Duration>>(mut self, target: T, half_life: T) -> Self {
        let initial = self.schedule_type.base_interval();
        self.schedule_type = ScheduleType::Decay {
            initial,
            target: target.into(),
            half_life: half_life.into(),
        };
        self
    }

    /// Limit the schedule to a maximum number of executions
    pub fn max_executions(mut self, count: u64) -> Self {
        self.limits.max_executions = Some(count);
        self
    }

    /// Limit the schedule to a maximum total runtime
    pub fn max_runtime<T: Into<Duration>>(mut self, duration: T) -> Self {
        self.limits.max_runtime = Some(duration.into());
        self
    }
}

/// Builder for creating schedules.
pub struct ScheduleBuilder<'a> {
    scheduler: &'a mut Scheduler,
    definition: ScheduleDefinition,
}

impl<'a> ScheduleBuilder<'a> {
    /// Create a new schedule builder.
    pub fn new(scheduler: &'a mut Scheduler, duration: Duration) -> Self {
        ScheduleBuilder {
            scheduler,
            definition: ScheduleDefinition::every(duration),
        }
    }

    /// Set the name for this schedule.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.definition = self.definition.with_name(name);
        self
    }

    /// Add an additional duration to the schedule interval.
    pub fn plus<T: Into<Duration>>(mut self, additional: T) -> Self {
        self.definition = self.definition.plus(additional);
        self
    }

    /// Set the callback ID for this schedule.
    pub fn with_callback_id(mut self, id: impl Into<String>) -> Self {
        self.definition = self.definition.with_callback_id(id);
        self
    }

    /// Add an event bus to emit events to.
    pub fn emit_to(mut self, bus_id: impl Into<String>) -> Self {
        self.definition = self.definition.emit_to(bus_id);
        self
    }

    /// Add random jitter to the schedule.
    pub fn with_jitter<T: Into<Duration>>(mut self, jitter: T) -> Self {
        self.definition = self.definition.with_jitter(jitter);
        self
    }

    /// Create an exponential backoff schedule.
    pub fn exponential(mut self, factor: f64, max_interval: Option<Duration>) -> Self {
        self.definition = self.definition.exponential(factor, max_interval);
        self
    }

    /// Create a decaying schedule that changes from the initial interval to the target.
    pub fn decay_to<T: Into<Duration>>(mut self, target: T, half_life: T) -> Self {
        self.definition = self.definition.decay_to(target, half_life);
        self
    }

    /// Limit the schedule to a maximum number of executions.
    pub fn max_executions(mut self, count: u64) -> Self {
        self.definition = self.definition.max_executions(count);
        self
    }

    /// Limit the schedule to a maximum total runtime.
    pub fn max_runtime<T: Into<Duration>>(mut self, duration: T) -> Self {
        self.definition = self.definition.max_runtime(duration);
        self
    }

    #[cfg(not(feature = "async"))]
    /// Execute a closure when this schedule fires.
    pub fn execute<F: Fn(TickEvent) + Send + Sync + Clone + 'static>(
        self,
        handler: F,
    ) -> Result<String, SchedulerError> {
        let id = format!("closure_{}", Uuid::new_v4());
        let handler = Box::new(ClosureHandler { handler });

        let mut registry = self.scheduler.callback_registry.lock()?;
        registry.insert(id.clone(), handler);
        drop(registry);

        self.with_callback_id(id).build()
    }

    #[cfg(feature = "async")]
    /// Execute a closure when this schedule fires, with async support.
    pub fn execute<F, Fut>(self, handler: F) -> Result<String, SchedulerError>
    where
        F: Fn(TickEvent) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        use crate::async_support::AsyncClosureHandler;

        let id = format!("async_closure_{}", Uuid::new_v4());
        let handler = Box::new(AsyncClosureHandler { handler });

        let mut registry = self.scheduler.callback_registry.lock()?;
        registry.insert(id.clone(), handler);
        drop(registry);

        self.with_callback_id(id).build()
    }

    /// Build the schedule and add it to the scheduler.
    pub fn build(self) -> Result<String, SchedulerError> {
        self.scheduler.add_schedule(self.definition)
    }
}

#[cfg(feature = "async")]
pub mod async_support {
    use super::*;

    /// Async handler for processing tick events with Tokio runtime.
    pub struct AsyncScheduler {
        scheduler: Scheduler,
    }

    impl std::fmt::Debug for AsyncScheduler {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("AsyncScheduler")
                .field("scheduler", &self.scheduler)
                .finish()
        }
    }

    impl AsyncScheduler {
        /// Create a new async scheduler with default settings.
        pub fn new() -> Self {
            AsyncScheduler {
                scheduler: Scheduler::new(),
            }
        }

        /// Create a new async scheduler with custom configuration.
        pub fn with_config(config: SchedulerConfig) -> Self {
            AsyncScheduler {
                scheduler: Scheduler::with_config(config),
            }
        }

        /// Register an async handler.
        pub fn register_async_handler<F, Fut>(
            &mut self,
            id: impl Into<String>,
            handler: F,
        ) -> Result<&mut Self, SchedulerError>
        where
            F: Fn(TickEvent) -> Fut + Send + Sync + Clone + 'static,
            Fut: Future<Output = ()> + Send + 'static,
        {
            let id = id.into();
            let async_handler = Box::new(AsyncClosureHandler { handler });
            self.scheduler.register_callback(id, async_handler)?;
            Ok(self)
        }

        /// Delegate to inner scheduler for schedule creation.
        pub fn every<T: Into<Duration>>(&mut self, duration: T) -> ScheduleBuilder {
            self.scheduler.every(duration)
        }

        /// Schedule a pre-defined schedule
        pub fn schedule(&self, definition: ScheduleDefinition) -> Result<String, SchedulerError> {
            self.scheduler.schedule(definition)
        }

        /// Start the async scheduler.
        pub fn start(&self) {
            self.scheduler.start();
        }

        /// Stop the async scheduler.
        pub fn stop(&self) {
            self.scheduler.stop();
        }

        /// Check if the scheduler is currently running.
        pub fn is_running(&self) -> bool {
            self.scheduler.is_running()
        }

        /// Get a schedule by its ID.
        pub fn get_schedule(&self, id: &str) -> Option<Schedule> {
            self.scheduler.get_schedule(id)
        }

        /// Get all schedules managed by this scheduler.
        pub fn get_all_schedules(&self) -> Vec<Schedule> {
            self.scheduler.get_all_schedules()
        }

        /// Remove a schedule by its ID.
        pub fn remove_schedule(&self, id: &str) -> Option<Schedule> {
            self.scheduler.remove_schedule(id)
        }

        /// Clear all schedules from the scheduler.
        pub fn clear_schedules(&self) {
            self.scheduler.clear_schedules();
        }

        /// Freeze the scheduler state for persistence.
        pub fn freeze(&self) -> Result<SchedulerState, SchedulerError> {
            self.scheduler.freeze()
        }
    }

    impl Default for AsyncScheduler {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Wrapper for async closures.
    pub struct AsyncClosureHandler<F, Fut>
    where
        F: Fn(TickEvent) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        pub handler: F,
    }

    impl<F, Fut> CallbackHandler for AsyncClosureHandler<F, Fut>
    where
        F: Fn(TickEvent) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        fn handle(&self, event: TickEvent) {
            // Create a runtime for the synchronous case
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => {
                    rt.block_on((self.handler)(event));
                }
                Err(e) => {
                    eprintln!("Failed to create Tokio runtime: {}", e);
                }
            }
        }

        fn handle_async<'a>(
            &'a self,
            event: TickEvent,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin((self.handler)(event))
        }
    }
}

// ----------------- SECTION 6: Unit Tests -----------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn test_schedule_creation() {
        // Test using Scheduler directly with ScheduleBuilder
        let mut scheduler = Scheduler::new();
        let schedule_id = scheduler
            .every(Duration::from_secs(5))
            .with_name("test_schedule")
            .build()
            .expect("Failed to build schedule");

        assert!(scheduler.schedules.contains_key(&schedule_id));

        // Test using ScheduleDefinition
        let scheduler = Scheduler::new();
        let definition =
            ScheduleDefinition::every(Duration::from_secs(5)).with_name("definition_schedule");

        let schedule_id = scheduler
            .schedule(definition)
            .expect("Failed to add schedule from definition");

        assert!(scheduler.schedules.contains_key(&schedule_id));
    }

    #[test]
    fn test_jittered_schedule() {
        // Test with ScheduleBuilder
        {
            // Create a scheduler with a fixed seed for deterministic tests
            let config = SchedulerConfig {
                thread_count: 2,
                store_capacity: 10,
                time_source: None,
                rng_seed: Some(12345),
            };

            let mut scheduler = Scheduler::with_config(config);

            // Build a jittered schedule
            let schedule_id = scheduler
                .every(Duration::from_secs(1))
                .with_name("jittered_test")
                .with_jitter(Duration::from_millis(500))
                .build()
                .expect("Failed to build schedule");

            let schedule = scheduler.get_schedule(&schedule_id).unwrap();

            // Check that the schedule has the expected type
            match schedule.schedule_type {
                ScheduleType::Jitter { base, jitter } => {
                    assert_eq!(base, Duration::from_secs(1));
                    assert_eq!(jitter, Duration::from_millis(500));
                }
                _ => panic!("Expected a jittered schedule"),
            }
        }

        // Test with ScheduleDefinition
        {
            let scheduler = Scheduler::new();

            // Build a jittered schedule definition
            let definition = ScheduleDefinition::every(Duration::from_secs(1))
                .with_name("jittered_definition")
                .with_jitter(Duration::from_millis(500));

            let schedule_id = scheduler
                .schedule(definition)
                .expect("Failed to add schedule from definition");

            let schedule = scheduler.get_schedule(&schedule_id).unwrap();

            // Check that the schedule has the expected type
            match schedule.schedule_type {
                ScheduleType::Jitter { base, jitter } => {
                    assert_eq!(base, Duration::from_secs(1));
                    assert_eq!(jitter, Duration::from_millis(500));
                }
                _ => panic!("Expected a jittered schedule"),
            }
        }
    }

    #[test]
    fn test_exponential_schedule() {
        // Test with ScheduleBuilder
        {
            let mut scheduler = Scheduler::new();

            // Build an exponential backoff schedule
            let schedule_id = scheduler
                .every(Duration::from_secs(1))
                .with_name("exponential_test")
                .exponential(2.0, Some(Duration::from_secs(8)))
                .build()
                .expect("Failed to build schedule");

            let schedule = scheduler.get_schedule(&schedule_id).unwrap();

            // Check that the schedule has the expected type
            match schedule.schedule_type {
                ScheduleType::Exponential {
                    initial,
                    factor,
                    max,
                } => {
                    assert_eq!(initial, Duration::from_secs(1));
                    assert_eq!(factor, 2.0);
                    assert_eq!(max, Some(Duration::from_secs(8)));
                }
                _ => panic!("Expected an exponential schedule"),
            }
        }

        // Test with ScheduleDefinition
        {
            let scheduler = Scheduler::new();

            // Build an exponential backoff schedule definition
            let definition = ScheduleDefinition::every(Duration::from_secs(1))
                .with_name("exponential_definition")
                .exponential(2.0, Some(Duration::from_secs(8)));

            let schedule_id = scheduler
                .schedule(definition)
                .expect("Failed to add schedule from definition");

            let schedule = scheduler.get_schedule(&schedule_id).unwrap();

            // Check that the schedule has the expected type
            match schedule.schedule_type {
                ScheduleType::Exponential {
                    initial,
                    factor,
                    max,
                } => {
                    assert_eq!(initial, Duration::from_secs(1));
                    assert_eq!(factor, 2.0);
                    assert_eq!(max, Some(Duration::from_secs(8)));
                }
                _ => panic!("Expected an exponential schedule"),
            }
        }
    }

    #[test]
    fn test_decay_schedule() {
        // Test with ScheduleBuilder
        {
            let mut scheduler = Scheduler::new();

            // Build a decay schedule
            let schedule_id = scheduler
                .every(Duration::from_secs(1))
                .with_name("decay_test")
                .decay_to(Duration::from_secs(10), Duration::from_secs(5))
                .build()
                .expect("Failed to build schedule");

            let schedule = scheduler.get_schedule(&schedule_id).unwrap();

            // Check that the schedule has the expected type
            match schedule.schedule_type {
                ScheduleType::Decay {
                    initial,
                    target,
                    half_life,
                } => {
                    assert_eq!(initial, Duration::from_secs(1));
                    assert_eq!(target, Duration::from_secs(10));
                    assert_eq!(half_life, Duration::from_secs(5));
                }
                _ => panic!("Expected a decay schedule"),
            }
        }

        // Test with ScheduleDefinition
        {
            let scheduler = Scheduler::new();

            // Build a decay schedule definition
            let definition = ScheduleDefinition::every(Duration::from_secs(1))
                .with_name("decay_definition")
                .decay_to(Duration::from_secs(10), Duration::from_secs(5));

            let schedule_id = scheduler
                .schedule(definition)
                .expect("Failed to add schedule from definition");

            let schedule = scheduler.get_schedule(&schedule_id).unwrap();

            // Check that the schedule has the expected type
            match schedule.schedule_type {
                ScheduleType::Decay {
                    initial,
                    target,
                    half_life,
                } => {
                    assert_eq!(initial, Duration::from_secs(1));
                    assert_eq!(target, Duration::from_secs(10));
                    assert_eq!(half_life, Duration::from_secs(5));
                }
                _ => panic!("Expected a decay schedule"),
            }
        }
    }

    #[test]
    fn test_execution_limits() {
        let mut scheduler = Scheduler::new();

        // Create a counter to track executions
        let counter: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
        let counter_clone: Arc<AtomicUsize> = Arc::clone(&counter);

        // Create a handler that increments the counter
        struct CounterHandler {
            counter: Arc<AtomicUsize>,
        }

        impl CallbackHandler for CounterHandler {
            fn handle(&self, _event: TickEvent) {
                self.counter.fetch_add(1, Ordering::SeqCst);
            }

            fn handle_async<'a>(
                &'a self,
                event: TickEvent,
            ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
                Box::pin(async move { self.handle(event) })
            }
        }

        // Register the handler
        scheduler
            .register_callback(
                "test_counter",
                Box::new(CounterHandler {
                    counter: counter_clone,
                }),
            )
            .expect("Failed to register callback");

        // Create a schedule with a max execution limit of 3
        scheduler
            .every(Duration::from_millis(10)) // Very short interval for quick test
            .with_name("limited_schedule")
            .with_callback_id("test_counter")
            .max_executions(3)
            .build()
            .expect("Failed to build schedule");

        // Start the scheduler and run it for a bit
        scheduler.start();
        thread::sleep(Duration::from_millis(100)); // Should be enough time for max executions
        scheduler.stop();

        // Check that it executed at most 3 times
        assert!(
            counter.load(Ordering::SeqCst) <= 3,
            "Schedule executed more than its max_executions limit"
        );
    }

    #[test]
    fn test_schedule_plus_method() {
        let mut scheduler = Scheduler::new();

        // Test plus on fixed schedule
        let fixed_id = scheduler
            .every(Duration::from_secs(1))
            .plus(Duration::from_millis(500))
            .build()
            .expect("Failed to build fixed schedule");

        let fixed_schedule = scheduler.get_schedule(&fixed_id).unwrap();
        match fixed_schedule.schedule_type {
            ScheduleType::Fixed(interval) => {
                assert_eq!(
                    interval,
                    Duration::from_secs(1) + Duration::from_millis(500)
                );
            }
            _ => panic!("Expected a fixed schedule"),
        }

        // Test plus on jittered schedule
        let jitter_id = scheduler
            .every(Duration::from_secs(1))
            .with_jitter(Duration::from_millis(200))
            .plus(Duration::from_millis(500))
            .build()
            .expect("Failed to build jittered schedule");

        let jitter_schedule = scheduler.get_schedule(&jitter_id).unwrap();
        match jitter_schedule.schedule_type {
            ScheduleType::Jitter { base, jitter } => {
                assert_eq!(base, Duration::from_secs(1) + Duration::from_millis(500));
                assert_eq!(jitter, Duration::from_millis(200));
            }
            _ => panic!("Expected a jittered schedule"),
        }
    }

    #[test]
    fn test_serialization_with_schedule_types() {
        let mut scheduler = Scheduler::new();

        // Add different schedule types
        scheduler
            .every(Duration::from_secs(1))
            .with_name("fixed_schedule")
            .build()
            .expect("Failed to build fixed schedule");

        scheduler
            .every(Duration::from_secs(2))
            .with_jitter(Duration::from_millis(500))
            .with_name("jittered_schedule")
            .build()
            .expect("Failed to build jittered schedule");

        scheduler
            .every(Duration::from_secs(1))
            .exponential(2.0, None)
            .with_name("exponential_schedule")
            .build()
            .expect("Failed to build exponential schedule");

        scheduler
            .every(Duration::from_secs(1))
            .decay_to(Duration::from_secs(5), Duration::from_secs(10))
            .with_name("decay_schedule")
            .build()
            .expect("Failed to build decay schedule");

        // Freeze the state
        let state = scheduler
            .freeze()
            .expect("Failed to freeze scheduler state");

        // Should have four schedules
        assert_eq!(state.schedules.len(), 4);

        // Restore and check
        let restored = Scheduler::restore(state).expect("Failed to restore scheduler");
        assert_eq!(restored.schedules.len(), 4);

        // Verify the schedule types were preserved
        let schedules = restored.get_all_schedules();

        // Count how many of each type we have
        let mut fixed_count = 0;
        let mut jitter_count = 0;
        let mut exponential_count = 0;
        let mut decay_count = 0;

        for schedule in schedules {
            match schedule.schedule_type {
                ScheduleType::Fixed(_) => fixed_count += 1,
                ScheduleType::Jitter { .. } => jitter_count += 1,
                ScheduleType::Exponential { .. } => exponential_count += 1,
                ScheduleType::Decay { .. } => decay_count += 1,
            }
        }

        assert_eq!(fixed_count, 1);
        assert_eq!(jitter_count, 1);
        assert_eq!(exponential_count, 1);
        assert_eq!(decay_count, 1);
    }

    #[test]
    fn test_thread_pool_execution() {
        let mut scheduler = Scheduler::new();

        // Create a counter to track executions
        let counter: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
        let counter_clone: Arc<AtomicUsize> = Arc::clone(&counter);

        // Create a handler that increments the counter
        struct CounterHandler {
            counter: Arc<AtomicUsize>,
        }

        impl CallbackHandler for CounterHandler {
            fn handle(&self, _event: TickEvent) {
                self.counter.fetch_add(1, Ordering::SeqCst);
            }

            fn handle_async<'a>(
                &'a self,
                event: TickEvent,
            ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
                Box::pin(async move {
                    self.handle(event);
                })
            }
        }

        // Register the handler
        scheduler
            .register_callback(
                "counter",
                Box::new(CounterHandler {
                    counter: counter_clone,
                }),
            )
            .expect("Failed to register callback");

        // Create a schedule with a very short interval
        scheduler
            .every(Duration::from_millis(10))
            .with_name("test_thread_pool")
            .with_callback_id("counter")
            .build()
            .expect("Failed to build schedule");

        // Start the scheduler
        scheduler.start();

        // Wait a bit for executions to occur
        thread::sleep(Duration::from_millis(100));

        // Stop the scheduler
        scheduler.stop();

        // The callback should have been executed multiple times
        assert!(counter.load(Ordering::SeqCst) > 0);
    }

    #[test]
    fn test_monotonic_time_source() {
        let time_source =
            MonotonicTimeSource::new().expect("Failed to create monotonic time source");

        // Check that time advances monotonically
        let t1 = time_source.now().expect("Failed to get time");
        thread::sleep(Duration::from_millis(10));
        let t2 = time_source.now().expect("Failed to get time");

        assert!(t2 > t1, "Time should advance monotonically");
        assert!(time_source.is_monotonic());
    }

    #[test]
    fn test_lock_free_schedule_store() {
        let store = LockFreeScheduleStore::new();

        // Insert an item
        let schedule = Schedule {
            id: "test".to_string(),
            name: "Test Schedule".to_string(),
            schedule_type: ScheduleType::Fixed(Duration::from_secs(10)),
            next_execution: None,
            callback_id: None,
            event_buses: Vec::new(),
            execution_count: 0,
            created_at: 0,
            limits: ScheduleLimits::default(),
        };

        store.insert("test".to_string(), schedule.clone());

        // Get the item
        let retrieved = store.get("test").expect("Schedule should exist");
        assert_eq!(retrieved.name, "Test Schedule");

        // Check contains_key
        assert!(store.contains_key("test"));
        assert!(!store.contains_key("nonexistent"));

        // Check len
        assert_eq!(store.len(), 1);

        // Get all
        let all = store.get_all();
        assert_eq!(all.len(), 1);

        // Remove item
        let removed = store
            .remove("test")
            .expect("Schedule should exist for removal");
        assert_eq!(removed.name, "Test Schedule");
        assert_eq!(store.len(), 0);

        // Re-insert and clear
        store.insert("test".to_string(), schedule);
        assert_eq!(store.len(), 1);
        store.clear();
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_lock_free_store_with_capacity() {
        let store = LockFreeScheduleStore::with_capacity(10);
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn test_scheduler_with_config() {
        let config = SchedulerConfig {
            thread_count: 2,
            store_capacity: 32,
            time_source: None,
            rng_seed: None,
        };

        let mut scheduler = Scheduler::with_config(config);
        assert!(!scheduler.is_running());

        // Add a schedule
        let schedule_id = scheduler
            .every(Duration::from_secs(1))
            .with_name("config_test")
            .build()
            .expect("Failed to build schedule");

        // Test additional Scheduler methods
        let schedule = scheduler
            .get_schedule(&schedule_id)
            .expect("Schedule should exist");
        assert_eq!(schedule.name, "config_test");

        let all_schedules = scheduler.get_all_schedules();
        assert_eq!(all_schedules.len(), 1);

        let removed = scheduler
            .remove_schedule(&schedule_id)
            .expect("Schedule should exist for removal");
        assert_eq!(removed.name, "config_test");
        assert_eq!(scheduler.get_all_schedules().len(), 0);

        // Re-add and test clear
        scheduler
            .every(Duration::from_secs(1))
            .with_name("clear_test")
            .build()
            .expect("Failed to build schedule");

        assert_eq!(scheduler.get_all_schedules().len(), 1);
        scheduler.clear_schedules();
        assert_eq!(scheduler.get_all_schedules().len(), 0);
    }

    #[test]
    fn test_callback_fn_helper() {
        let counter: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
        let counter_clone: Arc<AtomicUsize> = Arc::clone(&counter);

        // Create a handler with the helper function
        let handler = callback_fn(move |_event| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Use the handler
        let event = TickEvent {
            id: "test_id".to_string(),
            schedule_name: "test_schedule".to_string(),
            timestamp: 123456789,
            metadata: HashMap::new(),
        };

        handler.handle(event);
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        handler.handle(TickEvent {
            id: "test_id2".to_string(),
            schedule_name: "test_schedule2".to_string(),
            timestamp: 123456790,
            metadata: HashMap::new(),
        });

        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_compact_id_conversions() {
        // Test string conversion
        let id_str = "550e8400-e29b-41d4-a716-446655440000";
        let id = CompactId::try_from(id_str).expect("Valid UUID string should convert");
        assert_eq!(id.to_string(), id_str);

        // Test Uuid conversion
        let uuid = Uuid::parse_str(id_str).expect("Valid UUID string");
        let id_from_uuid: CompactId = uuid.into();
        assert_eq!(id_from_uuid.to_string(), id_str);

        // Test back to Uuid
        let uuid_from_id: Uuid = id.into();
        assert_eq!(uuid_from_id.to_string(), id_str);

        // Test String conversion
        let id_string = String::from(id_str);
        let id_from_string =
            CompactId::try_from(id_string).expect("Valid UUID string should convert");
        assert_eq!(id_from_string.to_string(), id_str);
    }
}
