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
}

impl Clone for SchedulerConfig {
    fn clone(&self) -> Self {
        Self {
            thread_count: self.thread_count,
            store_capacity: self.store_capacity,
            time_source: None, // We don't clone time sources
        }
    }
}

impl std::fmt::Debug for SchedulerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerConfig")
            .field("thread_count", &self.thread_count)
            .field("store_capacity", &self.store_capacity)
            .field("time_source", &"<TimeSource>")
            .finish()
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            thread_count: std::cmp::max(4, num_cpus::get()),
            store_capacity: 64, // Default capacity for collections
            time_source: None,
        }
    }
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
}

/// An individual schedule definition with timing and callback information.
#[derive(Clone, Debug)]
pub struct Schedule {
    id: String,
    name: String,
    interval: Duration,
    next_execution: Option<u64>,
    callback_id: Option<String>,
    event_buses: Vec<String>,
}

/// Event structure for schedule ticks.
#[derive(Clone, Debug)]
pub struct TickEvent {
    pub id: String,
    pub schedule_name: String,
    pub timestamp: u64,
    pub metadata: HashMap<String, String>,
}

/// Serializable representation of a schedule.
#[derive(Serialize, Deserialize)]
struct SerializedSchedule {
    id: String,
    name: String,
    interval_ms: u64,
    next_execution: Option<u64>,
    callback_id: Option<String>,
    event_buses: Vec<String>,
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

        Scheduler {
            schedules: Arc::new(LockFreeScheduleStore::new()),
            callback_registry: Arc::new(Mutex::new(HashMap::with_capacity(config.store_capacity))),
            event_buses: Arc::new(Mutex::new(HashMap::with_capacity(config.store_capacity))),
            time_source: Arc::new(time_source),
            running: Arc::new(AtomicBool::new(false)),
            last_tick: Arc::new(AtomicU64::new(0)),
            thread_pool: Arc::new(ThreadPool::new(config.thread_count)),
        }
    }

    /// Start a schedule definition with the specified interval.
    pub fn every<T: Into<Duration>>(&mut self, duration: T) -> ScheduleBuilder {
        ScheduleBuilder::new(self, duration.into())
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
                    if let Some(next_execution) = schedule.next_execution {
                        if next_execution <= now {
                            // Mark the schedule for execution
                            let updated_schedule = Schedule {
                                id: schedule.id.clone(),
                                name: schedule.name.clone(),
                                interval: schedule.interval,
                                next_execution: Some(now + schedule.interval.as_millis() as u64),
                                callback_id: schedule.callback_id.clone(),
                                event_buses: schedule.event_buses.clone(),
                            };

                            schedules.insert(schedule.id.clone(), updated_schedule.clone());
                            due_schedules.push((schedule.id.clone(), updated_schedule));
                        }
                    } else {
                        // First-time execution
                        let updated_schedule = Schedule {
                            id: schedule.id.clone(),
                            name: schedule.name.clone(),
                            interval: schedule.interval,
                            next_execution: Some(now + schedule.interval.as_millis() as u64),
                            callback_id: schedule.callback_id.clone(),
                            event_buses: schedule.event_buses.clone(),
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
            .map(|schedule| SerializedSchedule {
                id: schedule.id.clone(),
                name: schedule.name.clone(),
                interval_ms: schedule.interval.as_millis() as u64,
                next_execution: schedule.next_execution,
                callback_id: schedule.callback_id.clone(),
                event_buses: schedule.event_buses.clone(),
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
            let schedule = Schedule {
                id: serialized.id.clone(),
                name: serialized.name,
                interval: Duration::from_millis(serialized.interval_ms),
                next_execution: serialized.next_execution,
                callback_id: serialized.callback_id,
                event_buses: serialized.event_buses,
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
/// # use schedule_rs::{Scheduler, callback_fn, TickEvent};
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

/// Builder for creating schedules.
pub struct ScheduleBuilder<'a> {
    scheduler: &'a mut Scheduler,
    duration: Duration,
    name: Option<String>,
    callback_id: Option<String>,
    event_buses: Vec<String>,
}

impl<'a> ScheduleBuilder<'a> {
    /// Create a new schedule builder.
    pub fn new(scheduler: &'a mut Scheduler, duration: Duration) -> Self {
        ScheduleBuilder {
            scheduler,
            duration,
            name: None,
            callback_id: None,
            event_buses: Vec::new(),
        }
    }

    /// Set the name for this schedule.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Add an additional duration to the schedule interval.
    pub fn plus<T: Into<Duration>>(mut self, additional: T) -> Self {
        self.duration += additional.into();
        self
    }

    /// Set the callback ID for this schedule.
    pub fn with_callback_id(mut self, id: impl Into<String>) -> Self {
        self.callback_id = Some(id.into());
        self
    }

    /// Add an event bus to emit events to.
    pub fn emit_to(mut self, bus_id: impl Into<String>) -> Self {
        self.event_buses.push(bus_id.into());
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
        F: Fn(TickEvent) -> Fut + Send + Sync + 'static,
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
        let id = Uuid::new_v4().to_string();
        let name = self.name.unwrap_or_else(|| format!("schedule_{}", id));

        let schedule = Schedule {
            id: id.clone(),
            name,
            interval: self.duration,
            next_execution: None,
            callback_id: self.callback_id,
            event_buses: self.event_buses,
        };

        self.scheduler.schedules.insert(id.clone(), schedule);

        Ok(id)
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
        let mut scheduler = Scheduler::new();
        let schedule_id = scheduler
            .every(Duration::from_secs(5))
            .with_name("test_schedule")
            .build()
            .expect("Failed to build schedule");

        assert!(scheduler.schedules.contains_key(&schedule_id));
    }

    #[test]
    fn test_serialization() {
        let mut scheduler = Scheduler::new();

        // Add a schedule
        scheduler
            .every(Duration::from_secs(10))
            .with_name("serializable_schedule")
            .build()
            .expect("Failed to build schedule");

        // Freeze the state
        let state = scheduler
            .freeze()
            .expect("Failed to freeze scheduler state");

        // Should have one schedule
        assert_eq!(state.schedules.len(), 1);
        assert_eq!(state.schedules[0].name, "serializable_schedule");

        // Restore and check
        let restored = Scheduler::restore(state).expect("Failed to restore scheduler");
        assert_eq!(restored.schedules.len(), 1);
    }

    #[test]
    fn test_thread_pool_execution() {
        let mut scheduler = Scheduler::new();

        // Create a counter to track executions
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

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
            interval: Duration::from_secs(10),
            next_execution: None,
            callback_id: None,
            event_buses: Vec::new(),
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
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

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
