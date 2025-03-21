# ⏰ schedules.rs 🚀

<p align="center">
  <img src="assets/banner.jpg" alt="Schedules.rs Banner" width="800"/>
</p>

## ✨ A lightweight Rust library for scheduling operations across multiple time scales ✨

## 👥 Who This Is For

This library is perfect for developers who need:
- 🎯 Simple, predictable scheduling with absolute time intervals
- 🧩 Lightweight scheduling without complex calendar-based rules
- ⚙️ Precise control over execution timing in systems programming
- 🔧 Efficient scheduling in resource-constrained environments

By design, we focus on absolute time durations (every 5 seconds, every 10 minutes) and deliberately avoid calendar-based scheduling (every Thursday, first day of month). This keeps the API clean, predictable, and easy to reason about for systems programming tasks.

## 🎯 Features

- ⚡ Multiple tick frequencies (milliseconds to minutes)
- 🕰️ Configurable time sources for ultimate flexibility
- 🔄 Compound durations (e.g., 10 minutes plus 30 seconds)
- 📝 Named schedules with intuitive fluent builder API
- 🔗 Direct callback references and event emission
- 💾 Easy serialization and state persistence
- 🔄 Both synchronous AND asynchronous execution support
- 🔀 Advanced scheduling patterns:
  - ⏱️ Jittered intervals (add randomness to prevent thundering herds)
  - 📈 Exponential backoff with configurable rate and maximum
  - 📉 Decay intervals that transition from quick to slow
  - 🔢 Limited execution counts (run exactly N times)
- 🎛️ Full configuration options for:
  - 🧵 Worker thread pool size
  - 🧮 Schedule store pre-allocation
  - 🎲 Deterministic operation with fixed RNG seeds

## 🚀 Usage

```rust
// Create a scheduler with default settings
let mut scheduler = Scheduler::new();

// Or create with custom configuration
let config = SchedulerConfig {
    thread_count: 8,              // Use 8 worker threads
    store_capacity: 100,          // Pre-allocate for 100 schedules
    time_source: None,            // Use default time source
    rng_seed: Some(12345),        // Deterministic jitter with fixed seed
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
    rng_seed: Some(12345),  // For deterministic behavior
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

scheduler.start();
```

## 📦 Installation

Add to your Cargo.toml:

```toml
[dependencies]
schedules = "0.3.0"

# For async support:
schedules = { version = "0.3.0", features = ["async"] }
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🎉 Contribute

Contributions are welcome! Feel free to open issues and submit PRs to make schedule.rs even better!