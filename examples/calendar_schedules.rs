use chrono::{Datelike, Timelike};
use schedules::{CallbackHandler, FRIDAY, MONDAY, Scheduler, TickEvent, WEDNESDAY};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

// A simple handler
struct LogHandler {
    counter: Arc<AtomicUsize>,
}

impl CallbackHandler for LogHandler {
    fn handle(&self, event: TickEvent) {
        let count = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        println!(
            "[{}] Schedule '{}' executed ({})",
            chrono::Local::now().format("%H:%M:%S"),
            event.schedule_name,
            count
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
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a scheduler
    let mut scheduler = Scheduler::new_manual(); // Use manual execution mode

    // Register our handler
    scheduler.register_callback("log_handler", Box::new(LogHandler::new()))?;

    // Create a schedule for 24-hour time format (15:30)
    println!("Adding schedule for 24-hour time (example: 15:30)");
    scheduler
        .at("15:30")?
        .with_callback_id("log_handler")
        .build()?;

    // Create a schedule for 12-hour time format (3:30 PM)
    println!("Adding schedule for 12-hour time (example: 3:30 PM)");
    scheduler
        .at("3:30 PM")?
        .with_callback_id("log_handler")
        .build()?;

    // Create a schedule for specific days of the week
    println!("Adding schedule for Monday, Wednesday, and Friday at 10:00 AM");
    scheduler
        .on(MONDAY)
        .on_days(&[WEDNESDAY, FRIDAY]) // Add Wednesday and Friday
        .at("10:00 AM")?
        .with_callback_id("log_handler")
        .build()?;

    // Create a schedule for weekdays
    println!("Adding schedule for weekdays at 9:00 AM");
    scheduler
        .at("9:00 AM")?
        .on_weekdays()
        .with_callback_id("log_handler")
        .build()?;

    // Create a schedule for the 15th of each month
    println!("Adding schedule for 15th of each month at noon");
    scheduler
        .at("12:00")?
        .on_day_of_month(15)?
        .with_callback_id("log_handler")
        .build()?;

    // Simulate manual execution
    println!("\nRunning scheduler in manual mode...");
    println!("(This example will loop for 10 iterations, checking for tasks to run)");

    for i in 1..=10 {
        let now = chrono::Local::now();
        println!(
            "\nIteration {}: Checking for due schedules at {}",
            i,
            now.format("%Y-%m-%d %H:%M:%S")
        );

        let executed = scheduler.run_pending()?;
        println!("Executed {} schedules", executed);

        // Show what should happen:
        println!("Calendar schedules that would run at this time:");

        // Check time
        let hour = now.hour();
        let minute = now.minute();
        if hour == 15 && minute == 30 {
            println!("- 24-hour time schedule (15:30)");
        }
        if hour == 15 && minute == 30 {
            println!("- 12-hour time schedule (3:30 PM)");
        }

        // Check weekday schedules
        let weekday = now.weekday();
        let day_num = weekday.num_days_from_sunday();

        if day_num == 1 || day_num == 3 || day_num == 5 {
            // Monday, Wednesday, Friday
            if hour == 10 && minute == 0 {
                println!("- MWF schedule (Monday, Wednesday, Friday at 10:00 AM)");
            }
        }

        if day_num >= 1 && day_num <= 5 {
            // Weekdays (Monday-Friday)
            if hour == 9 && minute == 0 {
                println!("- Weekday schedule (9:00 AM)");
            }
        }

        // Check month day schedule
        let day = now.day();
        if day == 15 && hour == 12 && minute == 0 {
            println!("- Monthly schedule (15th at noon)");
        }

        // In a real app, you'd wait until the next task is due
        thread::sleep(Duration::from_secs(1));
    }

    println!("\nExample completed.");
    Ok(())
}
