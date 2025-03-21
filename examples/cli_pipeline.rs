use schedules::{Scheduler, TickEvent, callback_fn};
use std::env;
use std::io::{self, BufRead};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse arguments
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <interval_seconds> <execution_count>", args[0]);
        eprintln!("Example: echo 'date' | {} 5 10", args[0]);
        return Ok(());
    }

    let interval_secs: u64 = args[1].parse()?;
    let max_executions: u64 = args[2].parse()?;

    // Read command from stdin
    let stdin = io::stdin();
    let mut command_str = String::new();
    stdin.lock().read_line(&mut command_str)?;
    command_str = command_str.trim().to_string();

    if command_str.is_empty() {
        eprintln!("No command provided via pipe. Please pipe a command to run.");
        eprintln!("Example: echo 'date' | {} 5 10", args[0]);
        return Ok(());
    }

    println!(
        "Scheduling command: '{}' every {} seconds for {} executions",
        command_str, interval_secs, max_executions
    );

    // Store command in thread-safe container
    let command = Arc::new(Mutex::new(command_str));

    // Create execution counter
    let execution_count = Arc::new(Mutex::new(0u64));

    // Create scheduler
    let mut scheduler = Scheduler::new();

    // Clone Arc references for the closure
    let cmd_clone = Arc::clone(&command);
    let count_clone = Arc::clone(&execution_count);

    // Register a callback that executes the command
    scheduler.register_callback(
        "command_executor",
        callback_fn(move |_event: TickEvent| {
            let cmd_str = cmd_clone.lock().unwrap().clone();
            let mut count = count_clone.lock().unwrap();
            *count += 1;

            println!("\n[Execution {}/{}]", *count, max_executions);

            // Execute the command
            match Command::new("sh")
                .arg("-c")
                .arg(&cmd_str)
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .output()
            {
                Ok(_) => {}
                Err(e) => eprintln!("Error executing command: {}", e),
            };
        }),
    )?;

    // Create schedule with the specified interval
    scheduler
        .every(Duration::from_secs(interval_secs))
        .with_name("command_schedule")
        .with_callback_id("command_executor")
        .max_executions(max_executions)
        .build()?;

    // Start the scheduler
    println!("Starting scheduler. Press Ctrl+C to stop.");
    scheduler.start();

    // Wait until all executions are done
    loop {
        let current_count = *execution_count.lock().unwrap();
        if current_count >= max_executions {
            break;
        }
        thread::sleep(Duration::from_millis(500));
    }

    // Stop the scheduler
    scheduler.stop();
    println!("All executions completed. Exiting.");

    Ok(())
}
