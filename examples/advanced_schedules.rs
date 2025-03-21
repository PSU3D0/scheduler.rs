use schedule_rs::{CallbackHandler, Scheduler, SchedulerConfig, TickEvent};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

// Handler that counts executions
struct CounterHandler {
    counter: Arc<AtomicUsize>,
    id: String,
}

impl CallbackHandler for CounterHandler {
    fn handle(&self, event: TickEvent) {
        let count = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        println!(
            "Handler '{}': Schedule '{}' executed at {} ms (execution #{})",
            self.id, event.schedule_name, event.timestamp, count
        );
    }

    fn handle_async<'a>(
        &'a self,
        event: TickEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { self.handle(event) })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a scheduler with deterministic jitter using a fixed seed
    let config = SchedulerConfig {
        thread_count: 4,
        store_capacity: 10,
        time_source: None,
        rng_seed: Some(12345), // Fixed seed for deterministic jitter
    };

    let mut scheduler = Scheduler::with_config(config);

    // Create counters to track executions
    let fixed_counter = Arc::new(AtomicUsize::new(0));
    let jitter_counter = Arc::new(AtomicUsize::new(0));
    let exp_counter = Arc::new(AtomicUsize::new(0));
    let decay_counter = Arc::new(AtomicUsize::new(0));
    let limited_counter = Arc::new(AtomicUsize::new(0));

    // Register handlers
    scheduler.register_callback(
        "fixed_handler",
        Box::new(CounterHandler {
            counter: fixed_counter.clone(),
            id: "fixed_handler".to_string(),
        }),
    )?;

    scheduler.register_callback(
        "jitter_handler",
        Box::new(CounterHandler {
            counter: jitter_counter.clone(),
            id: "jitter_handler".to_string(),
        }),
    )?;

    scheduler.register_callback(
        "exp_handler",
        Box::new(CounterHandler {
            counter: exp_counter.clone(),
            id: "exp_handler".to_string(),
        }),
    )?;

    scheduler.register_callback(
        "decay_handler",
        Box::new(CounterHandler {
            counter: decay_counter.clone(),
            id: "decay_handler".to_string(),
        }),
    )?;

    scheduler.register_callback(
        "limited_handler",
        Box::new(CounterHandler {
            counter: limited_counter.clone(),
            id: "limited_handler".to_string(),
        }),
    )?;

    // Create different schedule types
    println!("Creating schedules...");

    // 1. Fixed interval schedule - every 1 second
    scheduler
        .every(Duration::from_secs(1))
        .with_name("fixed_interval")
        .with_callback_id("fixed_handler")
        .build()?;

    // 2. Jittered schedule - around 2 seconds with up to 500ms jitter
    scheduler
        .every(Duration::from_secs(2))
        .with_name("jittered_interval")
        .with_jitter(Duration::from_millis(500))
        .with_callback_id("jitter_handler")
        .build()?;

    // 3. Exponential backoff - starts at 1 second and doubles each time, max 8 seconds
    scheduler
        .every(Duration::from_secs(1))
        .with_name("exponential_backoff")
        .exponential(2.0, Some(Duration::from_secs(8)))
        .with_callback_id("exp_handler")
        .build()?;

    // 4. Decaying schedule - changes from 500ms to 3 seconds with 5 second half-life
    scheduler
        .every(Duration::from_millis(500))
        .with_name("decaying_interval")
        .decay_to(Duration::from_secs(3), Duration::from_secs(5))
        .with_callback_id("decay_handler")
        .build()?;

    // 5. Limited schedule - only executes 5 times
    scheduler
        .every(Duration::from_secs(1))
        .with_name("limited_executions")
        .max_executions(5)
        .with_callback_id("limited_handler")
        .build()?;

    // Start the scheduler
    println!("Starting scheduler...");
    scheduler.start();

    // Run for 20 seconds
    println!("Running for 20 seconds...");
    thread::sleep(Duration::from_secs(20));

    // Stop the scheduler
    scheduler.stop();
    println!("Scheduler stopped.");

    // Report final counts
    println!("\nExecution Summary:");
    println!(
        "Fixed interval: {} executions",
        fixed_counter.load(Ordering::SeqCst)
    );
    println!(
        "Jittered interval: {} executions",
        jitter_counter.load(Ordering::SeqCst)
    );
    println!(
        "Exponential backoff: {} executions",
        exp_counter.load(Ordering::SeqCst)
    );
    println!(
        "Decaying interval: {} executions",
        decay_counter.load(Ordering::SeqCst)
    );
    println!(
        "Limited executions: {} executions (max 5)",
        limited_counter.load(Ordering::SeqCst)
    );

    Ok(())
}
