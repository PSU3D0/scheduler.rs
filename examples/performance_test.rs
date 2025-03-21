use rand::Rng;
use schedules::{Scheduler, SchedulerConfig, TickEvent, callback_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Performance Test: Setting up thousands of callbacks");

    // Configuration for higher performance
    let config = SchedulerConfig {
        thread_count: num_cpus::get(),
        store_capacity: 5000, // Increased capacity for many schedules
        time_source: None,
        rng_seed: None,
    };

    let mut scheduler = Scheduler::with_config(config);

    // We'll use this to track the total number of callbacks executed
    let total_executions = Arc::new(AtomicUsize::new(0));

    // Create a single callback handler that will be shared by many schedules
    let execution_counter = total_executions.clone();
    let handler = callback_fn(move |_event: TickEvent| {
        execution_counter.fetch_add(1, Ordering::Relaxed);
    });

    // Register our handler
    scheduler.register_callback("counter", handler)?;

    // Setup timing
    let setup_start = Instant::now();
    // Number of schedules to create
    const NUM_SCHEDULES: usize = 2000;

    // Create thousands of schedules with well-distributed intervals
    for i in 0..NUM_SCHEDULES {
        // Use a more distributed timing pattern:
        // - Base interval spread from 20ms to 200ms
        // - Add staggered offset based on schedule index
        let mut rng = rand::thread_rng();
        let interval_ms = rng.gen_range(20..2000);
        let interval = Duration::from_millis(interval_ms);

        scheduler
            .every(interval)
            .with_name(format!("schedule_{}", i))
            .with_callback_id("counter")
            .build()?;
    }

    let setup_time = setup_start.elapsed();
    println!(
        "Setup complete: Created {} schedules in {:?} ({:.2} schedules/sec)",
        NUM_SCHEDULES,
        setup_time,
        NUM_SCHEDULES as f64 / setup_time.as_secs_f64()
    );

    // Start the scheduler and time the performance
    println!("Starting scheduler...");
    let start_time = Instant::now();
    scheduler.start();

    // Setup monitoring to track execution rate
    let monitor_executions = total_executions.clone();
    let monitoring_thread = thread::spawn(move || {
        let mut last_count = 0;
        let mut last_time = Instant::now();

        for i in 1..=10 {
            thread::sleep(Duration::from_secs(1));

            let now = Instant::now();
            let current_count = monitor_executions.load(Ordering::Relaxed);
            let elapsed = now.duration_since(last_time);

            let rate = (current_count - last_count) as f64 / elapsed.as_secs_f64();
            println!(
                "[{} sec] Executions: {} total, {:.2} executions/sec",
                i, current_count, rate
            );

            last_count = current_count;
            last_time = now;
        }
    });

    // Let the test run for about 10 seconds
    monitoring_thread.join().unwrap();

    // Stop the scheduler
    scheduler.stop();
    let total_time = start_time.elapsed();
    let final_count = total_executions.load(Ordering::Relaxed);

    // Final report
    println!("\nPerformance Test Results:");
    println!("Total runtime: {:?}", total_time);
    println!("Total executions: {}", final_count);
    println!(
        "Average rate: {:.2} executions/sec",
        final_count as f64 / total_time.as_secs_f64()
    );
    println!(
        "Average execution time: {:.3} ms",
        (total_time.as_millis() as f64) / (final_count as f64)
    );

    Ok(())
}
