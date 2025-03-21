use schedules::{CallbackHandler, Scheduler, SchedulerState, TickEvent};
use std::fs;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::thread;
use std::time::Duration;

// A simple handler
struct CountHandler {
    name: String,
    count: u32,
}

impl CountHandler {
    fn new(name: impl Into<String>) -> Self {
        CountHandler {
            name: name.into(),
            count: 0,
        }
    }
}

impl CallbackHandler for CountHandler {
    fn handle(&self, event: TickEvent) {
        println!(
            "Handler '{}' executed for schedule: {} (count: {})",
            self.name, event.schedule_name, self.count
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
    let state_file = "scheduler_state.json";

    // Check if we have a saved state
    let mut scheduler = if Path::new(state_file).exists() {
        println!("Restoring scheduler from saved state...");

        // Read the state file
        let contents = fs::read_to_string(state_file)?;

        // Deserialize
        let state: SchedulerState = serde_json::from_str(&contents)?;

        // Restore the scheduler
        Scheduler::restore(state)?
    } else {
        println!("Creating a new scheduler...");
        Scheduler::new()
    };

    // Always register handlers (they can't be serialized)
    scheduler.register_callback("counter_1", Box::new(CountHandler::new("Counter 1")))?;
    scheduler.register_callback("counter_2", Box::new(CountHandler::new("Counter 2")))?;

    // If it's a new scheduler, create the schedules
    if !Path::new(state_file).exists() {
        println!("Creating schedules...");

        scheduler
            .every(Duration::from_secs(5))
            .with_name("five_second_tick")
            .with_callback_id("counter_1")
            .build()?;

        scheduler
            .every(Duration::from_secs(10))
            .with_name("ten_second_tick")
            .with_callback_id("counter_2")
            .build()?;
    }

    // Start the scheduler
    println!("Starting scheduler...");
    scheduler.start();

    // Run for a while
    println!("Running for 30 seconds...");
    thread::sleep(Duration::from_secs(30));

    // Stop the scheduler
    println!("Stopping scheduler...");
    scheduler.stop();

    // Freeze the state
    println!("Saving scheduler state...");
    let state = scheduler.freeze()?;

    // Serialize to JSON
    let serialized = serde_json::to_string_pretty(&state)?;

    // Save to file
    fs::write(state_file, serialized)?;

    println!("Scheduler state saved to {}", state_file);
    println!("Run this example again to restore the state!");

    Ok(())
}
