use schedule_rs::{CallbackHandler, Scheduler, TickEvent};
use std::future::Future;
use std::pin::Pin;
use std::thread;
use std::time::Duration;

// A simple handler
struct LogHandler {
    start: std::time::Instant,
}

impl CallbackHandler for LogHandler {
    fn handle(&self, event: TickEvent) {
        println!(
            "Event fired from schedule: {} at {:?}",
            event.schedule_name,
            self.start.elapsed()
        );
    }

    fn handle_async<'a>(
        &'a self,
        event: TickEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { self.handle(event) })
    }
}

impl LogHandler {
    fn new() -> Self {
        LogHandler {
            start: std::time::Instant::now(),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a scheduler
    let mut scheduler = Scheduler::new();
    let start = std::time::Instant::now();

    // Register our handler
    scheduler.register_callback("log_handler", Box::new(LogHandler::new()))?;

    // Create a schedule every 5 seconds
    println!("Adding schedule every 5 seconds...");
    scheduler
        .every(Duration::from_secs(5))
        .with_name("five_second_tick")
        .with_callback_id("log_handler")
        .build()?;

    // Create a schedule with a compound duration
    println!("Adding schedule every 10 seconds plus 500ms...");
    scheduler
        .every(Duration::from_secs(10))
        .plus(Duration::from_millis(500))
        .with_name("compound_duration")
        .with_callback_id("log_handler")
        .build()?;

    // Create a schedule with an inline closure
    println!("Adding schedule with inline closure...");
    #[cfg(not(feature = "async"))]
    scheduler
        .every(Duration::from_secs(3))
        .with_name("inline_closure")
        .execute(move |event| {
            println!(
                "Inline closure executed for {} at {:?}",
                event.schedule_name,
                start.clone().elapsed()
            );
        })?;

    #[cfg(feature = "async")]
    scheduler
        .every(Duration::from_secs(3))
        .with_name("inline_closure")
        .execute(move |event| async move {
            println!(
                "Inline closure executed for {} at {:?}",
                event.schedule_name,
                start.clone().elapsed()
            );
        })?;

    // Start the scheduler
    println!("Starting scheduler...");
    scheduler.start();

    // Run for 15 seconds then exit
    println!("Scheduler is running for 15 seconds...");
    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(15);

    while start_time.elapsed() < timeout {
        thread::sleep(Duration::from_secs(1));
    }

    // Stop the scheduler before exiting
    scheduler.stop();
    println!("Scheduler stopped.");

    Ok(())
}
