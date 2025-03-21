// This example requires the `async` feature:
// cargo run --example async --features async

#[cfg(feature = "async")]
use schedule_rs::async_support::AsyncScheduler;

#[cfg(feature = "async")]
use std::time::Duration;

#[cfg(feature = "async")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create an async scheduler
    let mut scheduler = AsyncScheduler::new();

    // Register an async handler
    scheduler.register_async_handler("async_task", |event| async move {
        println!("Starting async task for schedule: {}", event.schedule_name);

        // Simulate some async work
        tokio::time::sleep(Duration::from_millis(500)).await;

        println!("Completed async task for schedule: {}", event.schedule_name);
    })?;

    // Create a schedule using the async handler
    scheduler
        .every(Duration::from_secs(2))
        .with_name("async_schedule")
        .with_callback_id("async_task")
        .build()?;

    // Create another schedule with an inline async closure
    scheduler
        .every(Duration::from_secs(5))
        .with_name("inline_async")
        .execute(|event| async move {
            println!("Starting inline async task: {}", event.schedule_name);
            tokio::time::sleep(Duration::from_millis(1000)).await;
            println!("Completed inline async task: {}", event.schedule_name);
        })?;

    // Start the scheduler
    println!("Starting async scheduler...");
    scheduler.start();

    // Keep the program running
    println!("Scheduler is running. Press Ctrl+C to exit...");

    // Wait indefinitely
    tokio::signal::ctrl_c().await?;
    println!("Stopping scheduler...");

    Ok(())
}

#[cfg(not(feature = "async"))]
fn main() {
    println!("This example requires the 'async' feature!");
    println!("Run with: cargo run --example async --features async");
}
