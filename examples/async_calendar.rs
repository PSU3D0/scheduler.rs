#[cfg(feature = "async")]
use schedules::{FRIDAY, MONDAY, WEDNESDAY, async_support::AsyncScheduler};
use std::time::Duration;

#[cfg(feature = "async")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a manual scheduler
    let mut scheduler = AsyncScheduler::new_manual();

    // Create a schedule for a specific time of day
    println!("Adding schedule for 3:30 PM");
    // Create builder, then execute so we don't consume mutable reference
    let time_builder = scheduler.at("3:30 PM")?;
    time_builder.execute(|event| async move {
        println!(
            "[{}] Time-based schedule '{}' executed!",
            chrono::Local::now().format("%H:%M:%S"),
            event.schedule_name
        );
    })?;

    // Create a schedule for Monday, Wednesday, Friday at 10am
    println!("Adding schedule for Monday, Wednesday, Friday at 10:00 AM");
    // First create all the builders, then construct schedule
    let mut monday_builder = scheduler.on(MONDAY);
    let days_builder = monday_builder.on_days(&[WEDNESDAY, FRIDAY]);
    let time_builder = days_builder.at("10:00 AM")?;
    time_builder.execute(|event| async move {
        println!(
            "[{}] MWF schedule '{}' executed!",
            chrono::Local::now().format("%H:%M:%S"),
            event.schedule_name
        );

        // Simulate async work
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!(
            "Async work completed for schedule '{}'",
            event.schedule_name
        );
    })?;

    // Simulate manual execution
    println!("\nRunning async scheduler in manual mode...");
    println!("(This example will loop for 5 iterations, checking for tasks to run)");

    for i in 1..=5 {
        let now = chrono::Local::now();
        println!(
            "\nIteration {}: Checking for due schedules at {}",
            i,
            now.format("%Y-%m-%d %H:%M:%S")
        );

        let executed = scheduler.run_pending()?;
        println!("Executed {} schedules", executed);

        // Pause to allow async tasks to complete
        tokio::time::sleep(Duration::from_millis(200)).await;

        // In a real app, you'd wait until the next task is due
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    println!("\nExample completed.");
    Ok(())
}

#[cfg(not(feature = "async"))]
fn main() {
    println!("This example requires the 'async' feature.");
    println!("Run with: cargo run --example async_calendar --features async");
}
