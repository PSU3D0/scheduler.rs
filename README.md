# ⏰ schedules.rs 🚀

<p align="center">
  <img src="assets/banner.jpg" alt="Schedules.rs Banner" width="800"/>
</p>

## ✨ A lightweight Rust library for scheduling operations across multiple time scales ✨

## 👥 Who This Is For

This library is perfect for developers who need:
- 🎯 Simple, predictable scheduling with absolute time intervals
- 🗓️ Calendar-based scheduling (specific days, times, dates)
- ⚙️ Precise control over execution timing in systems programming
- 🔧 Efficient scheduling in resource-constrained environments
- 🔄 Automatic or manual schedule execution control

The library supports both absolute time durations (every 5 seconds, every 10 minutes) as well as calendar-based scheduling (every Thursday, first day of month, at 3:30 PM). The API is clean, predictable, and easy to reason about for all scheduling tasks.

## 🎯 Features

- ⚡ Multiple tick frequencies (milliseconds to minutes)
- 🕰️ Configurable time sources for ultimate flexibility
- 🔄 Compound durations (e.g., 10 minutes plus 30 seconds)
- 📝 Named schedules with intuitive fluent builder API
- 🔗 Direct callback references and event emission
- 💾 Easy serialization and state persistence
- 🔄 Both synchronous AND asynchronous execution support
- 🗓️ Calendar-based scheduling:
  - ⏰ Time-of-day scheduling ("3:30 PM", "15:30")
  - 📆 Day-of-week scheduling (Monday, Tuesday, etc.)
  - 📅 Day-of-month scheduling (1st, 15th day of month)
  - 🌙 Month-specific scheduling (January, February, etc.)
  - 🔀 Multiple time/day combinations
- 🔀 Advanced scheduling patterns:
  - ⏱️ Jittered intervals (add randomness to prevent thundering herds)
  - 📈 Exponential backoff with configurable rate and maximum
  - 📉 Decay intervals that transition from quick to slow
  - 🔢 Limited execution counts (run exactly N times)
- 🎛️ Full configuration options for:
  - 🧵 Worker thread pool size
  - 🧮 Schedule store pre-allocation
  - 🎲 Deterministic operation with fixed RNG seeds
  - 🔄 Automatic or manual execution modes

## 🚀 Usage

```rust
// Create a scheduler with default settings
let mut scheduler = Scheduler::new();

// Or create with custom configuration
let config = SchedulerConfig {
    thread_count: 8,                           // Use 8 worker threads
    store_capacity: 100,                       // Pre-allocate for 100 schedules
    time_source: None,                         // Use default time source
    rng_seed: Some(12345),                     // Deterministic jitter with fixed seed
    timezone: Some(FixedOffset::east(3600)),   // For calendar scheduling (UTC+1)
};
let mut scheduler = Scheduler::with_config(config);

// Register a callback handler (traditional way)
struct LogHandler;
impl CallbackHandler for LogHandler {
    fn handle(&self, event: TickEvent) {
        println!("Event fired: {:?}", event);
    }
    
    fn handle_async<'a>(&'a self, event: TickEvent) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { self.handle(event) })
    }
}

scheduler.register_callback("log_handler", Box::new(LogHandler))?;

// Or register a callback using the helper function (much simpler!)
scheduler.register_callback(
    "simple_logger", 
    callback_fn(|event| println!("Event: {}", event.schedule_name))
)?;

// Create a schedule with the registered handler
scheduler.every(Duration::from_secs(5))
    .with_name("status_check")
    .with_callback_id("log_handler")
    .build()?;

// For simple cases, use the convenience method
scheduler.every(Duration::from_millis(100))
    .with_name("fast_tick")
    .execute(|event| {
        println!("Fast tick: {:?}", event);
    })?;

// Create advanced schedule patterns
// Jittered schedule (adds randomness to prevent thundering herds)
scheduler.every(Duration::from_secs(2))
    .with_name("jittered_interval")
    .with_jitter(Duration::from_millis(500))
    .with_callback_id("log_handler")
    .build()?;

// Exponential backoff (good for retries)
scheduler.every(Duration::from_secs(1))
    .with_name("exponential_backoff")
    .exponential(2.0, Some(Duration::from_secs(8)))
    .with_callback_id("log_handler")
    .build()?;

// Decaying schedule (transitions from quick to slow intervals)
scheduler.every(Duration::from_millis(500))
    .with_name("decaying_interval")
    .decay_to(Duration::from_secs(3), Duration::from_secs(5))
    .with_callback_id("log_handler")
    .build()?;

// Limited schedule (only executes a set number of times)
scheduler.every(Duration::from_secs(1))
    .with_name("limited_executions")
    .max_executions(5)
    .with_callback_id("log_handler")
    .build()?;

// Start the scheduler
scheduler.start();

// You can manage schedules directly:
let all_schedules = scheduler.get_all_schedules();
scheduler.remove_schedule("some_id")?;
scheduler.clear_schedules();

// Later, freeze the state
let state = scheduler.freeze()?;
let serialized = serde_json::to_string(&state)?;

// Restore (requires re-registering handlers)
let state: SchedulerState = serde_json::from_str(&serialized)?;
let mut restored = Scheduler::restore(state)?;
restored.register_callback("log_handler", Box::new(LogHandler))?;
restored.start();
```

## 🗓️ Calendar-Based Scheduling

```rust
// Create a scheduler
let mut scheduler = Scheduler::new();

// Register our handler
scheduler.register_callback("log_handler", Box::new(LogHandler))?;

// Schedule at a specific time each day (24-hour format)
scheduler
    .at("15:30")?
    .with_name("daily_afternoon_check")
    .with_callback_id("log_handler")
    .build()?;

// Schedule at a specific time each day (12-hour format)
scheduler
    .at("3:30 PM")?
    .with_name("daily_afternoon_check_12h")
    .with_callback_id("log_handler")
    .build()?;

// Schedule for specific days of the week
scheduler
    .on(MONDAY)
    .on_days(&[WEDNESDAY, FRIDAY]) // Add more days
    .at("10:00 AM")?
    .with_name("mwf_morning_check")
    .with_callback_id("log_handler")
    .build()?;

// Schedule for all weekdays
scheduler
    .at("9:00 AM")?
    .on_weekdays()
    .with_name("daily_standup")
    .with_callback_id("log_handler")
    .build()?;

// Schedule for a specific day of the month
scheduler
    .at("12:00")?
    .on_day_of_month(15)?
    .with_name("mid_month_report")
    .with_callback_id("log_handler")
    .build()?;

// Start the scheduler (automatic execution)
scheduler.start();
```

## 🔄 Manual Execution Mode

```rust
// Create a scheduler with manual execution mode
let mut scheduler = Scheduler::new_manual();

// Register handlers and define schedules as usual
scheduler.register_callback("log_handler", Box::new(LogHandler))?;

scheduler
    .at("3:30 PM")?
    .with_name("afternoon_check")
    .with_callback_id("log_handler")
    .build()?;

scheduler
    .every(Duration::from_secs(300))
    .with_name("five_minute_check")
    .with_callback_id("log_handler")
    .build()?;

// Instead of starting the scheduler, periodically call run_pending()
loop {
    // Check for and execute due schedules
    let executed_count = scheduler.run_pending()?;
    println!("Executed {} schedules", executed_count);
    
    // Wait for some time before checking again
    std::thread::sleep(Duration::from_secs(1));
}
```

Manual execution also works with async schedulers:

```rust
// Create an async scheduler with manual execution mode
let mut scheduler = AsyncScheduler::new_manual();

// Register async handlers and define schedules
scheduler.register_async_handler("async_handler", |event| async move {
    println!("Async handling of event: {:?}", event);
    tokio::time::sleep(Duration::from_millis(100)).await;
})?;

scheduler
    .at("3:30 PM")?
    .with_name("async_afternoon_task")
    .with_callback_id("async_handler")
    .build()?;

// Use within an async runtime
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut scheduler = AsyncScheduler::new_manual();
    // ... configure scheduler ...
    
    // Periodically call run_pending in your async context
    loop {
        let executed_count = scheduler.run_pending().await?;
        println!("Executed {} async schedules", executed_count);
        
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
```

## ⚡ Async Support

For asynchronous operation with Tokio (requires the `async` feature):

```rust
// Enable the feature in Cargo.toml:
// [dependencies]
// schedules = { version = "0.3.0", features = ["async"] }

// Create an async scheduler with default settings
let mut scheduler = AsyncScheduler::new();

// Or create with custom configuration
let config = SchedulerConfig {
    thread_count: 8,
    store_capacity: 100,
    time_source: None,
    rng_seed: Some(12345),                     // For deterministic behavior
    timezone: Some(FixedOffset::east(3600)),   // For calendar scheduling (UTC+1)
};
let mut scheduler = AsyncScheduler::with_config(config);

// Register an async handler
scheduler.register_async_handler("async_handler", |event| async move {
    println!("Async handling of event: {:?}", event);
    // Perform async operations here
    tokio::time::sleep(Duration::from_millis(100)).await;
})?;

// Create and start schedules as with the sync scheduler
scheduler.every(Duration::from_secs(1))
    .with_name("async_task")
    .with_callback_id("async_handler")
    .build()?;

// Create a schedule with an inline async closure
scheduler
    .every(Duration::from_secs(5))
    .with_name("inline_async")
    .execute(|event| async move {
        println!("Starting inline async task: {}", event.schedule_name);
        tokio::time::sleep(Duration::from_millis(1000)).await;
        println!("Completed inline async task: {}", event.schedule_name);
    })?;

// Calendar-based async scheduling
scheduler
    .at("9:00 AM")?
    .on_weekdays()
    .with_name("morning_async_task")
    .execute(|event| async move {
        println!("Starting morning task: {}", event.schedule_name);
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("Completed morning task: {}", event.schedule_name);
    })?;

scheduler.start();
```

## 📦 Installation

Add to your Cargo.toml:

```toml
[dependencies]
schedules = "0.4.0"

# For async support:
schedules = { version = "0.4.0", features = ["async"] }

# For all features including calendar scheduling and manual execution:
schedules = { version = "0.4.0", features = ["async", "calendar"] }
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🎉 Contribute

Contributions are welcome! Feel free to open issues and submit PRs to make schedule.rs even better!