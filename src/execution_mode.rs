use serde::{Deserialize, Serialize};

/// Execution mode for the scheduler
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ExecutionMode {
    /// Automatic execution in scheduler's background thread (default)
    #[default]
    Automatic,

    /// Manual execution requiring explicit run_pending() calls
    Manual,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_mode_default() {
        assert_eq!(ExecutionMode::default(), ExecutionMode::Automatic);
    }
}
