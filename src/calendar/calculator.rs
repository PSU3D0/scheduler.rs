use crate::calendar::{Predicate, PredicateCombination};
use chrono::{DateTime, Duration as ChronoDuration, FixedOffset, TimeZone, Utc};
use std::time::Duration;

/// Calculate the next time a calendar-based schedule should run
pub fn calculate_next_calendar_time(
    predicates: &[Predicate],
    combine: &PredicateCombination,
    now: u64,
    timezone: &Option<FixedOffset>,
) -> u64 {
    // Convert the timestamp to a DateTime
    let now_dt = match timestamp_to_datetime(now) {
        Some(dt) => dt,
        None => return now + Duration::from_secs(60).as_millis() as u64, // Fallback to 60s if conversion fails
    };

    // Start looking for the next valid time from 1 minute in the future
    let mut candidate = now_dt + ChronoDuration::minutes(1);
    let max_iterations = 10000; // Safety limit to prevent infinite loops
    let mut iterations = 0;

    // Check time points until we find one that satisfies our predicates
    while iterations < max_iterations {
        if matches_predicates(predicates, combine, &candidate, timezone) {
            return datetime_to_timestamp(&candidate)
                .unwrap_or_else(|| now + Duration::from_secs(60).as_millis() as u64);
        }

        // Move to the next minute
        candidate += ChronoDuration::minutes(1);
        iterations += 1;
    }

    // Fallback if we couldn't find a suitable time within reasonable iterations
    now + Duration::from_secs(3600).as_millis() as u64 // Default to 1 hour
}

/// Check if a DateTime matches the set of predicates
fn matches_predicates(
    predicates: &[Predicate],
    combine: &PredicateCombination,
    dt: &DateTime<Utc>,
    timezone: &Option<FixedOffset>,
) -> bool {
    match combine {
        PredicateCombination::All => predicates.iter().all(|pred| pred.matches(dt, timezone)),
        PredicateCombination::Any => predicates.iter().any(|pred| pred.matches(dt, timezone)),
    }
}

/// Convert a timestamp (milliseconds since epoch) to a DateTime
fn timestamp_to_datetime(timestamp_ms: u64) -> Option<DateTime<Utc>> {
    let seconds = (timestamp_ms / 1000) as i64;
    let nanoseconds = ((timestamp_ms % 1000) * 1_000_000) as u32;

    match Utc.timestamp_opt(seconds, nanoseconds) {
        chrono::offset::LocalResult::Single(dt) => Some(dt),
        _ => None,
    }
}

/// Convert a DateTime to a timestamp (milliseconds since epoch)
fn datetime_to_timestamp(dt: &DateTime<Utc>) -> Option<u64> {
    let timestamp_s = dt.timestamp();
    if timestamp_s < 0 {
        return None; // Can't represent pre-epoch time in this system
    }

    let timestamp_ns = dt.timestamp_subsec_nanos();
    Some((timestamp_s as u64 * 1000) + (timestamp_ns / 1_000_000) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calendar::{FRIDAY, MONDAY, TimeUnit};

    #[test]
    fn test_matches_predicates_all() {
        // Test with Monday at 10:00
        let monday_10am = Utc.with_ymd_and_hms(2023, 1, 2, 10, 0, 0).unwrap(); // Jan 2, 2023 was a Monday

        let predicates = vec![
            Predicate::Exact(MONDAY),
            Predicate::Exact(TimeUnit::TimeOfDay(
                chrono::NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            )),
        ];

        // Test with All combination (AND logic)
        assert!(matches_predicates(
            &predicates,
            &PredicateCombination::All,
            &monday_10am,
            &None
        ));

        // Test with a non-matching time (same day, wrong time)
        let monday_11am = Utc.with_ymd_and_hms(2023, 1, 2, 11, 0, 0).unwrap();
        assert!(!matches_predicates(
            &predicates,
            &PredicateCombination::All,
            &monday_11am,
            &None
        ));

        // Test with a non-matching day (wrong day, right time)
        let tuesday_10am = Utc.with_ymd_and_hms(2023, 1, 3, 10, 0, 0).unwrap(); // Jan 3, 2023 was a Tuesday
        assert!(!matches_predicates(
            &predicates,
            &PredicateCombination::All,
            &tuesday_10am,
            &None
        ));
    }

    #[test]
    fn test_matches_predicates_any() {
        // Test with Monday at 10:00
        let monday_10am = Utc.with_ymd_and_hms(2023, 1, 2, 10, 0, 0).unwrap(); // Jan 2, 2023 was a Monday

        let predicates = vec![Predicate::Exact(MONDAY), Predicate::Exact(FRIDAY)];

        // Test with Any combination (OR logic) - should match Monday
        assert!(matches_predicates(
            &predicates,
            &PredicateCombination::Any,
            &monday_10am,
            &None
        ));

        // Test with Friday - should also match
        let friday = Utc.with_ymd_and_hms(2023, 1, 6, 9, 0, 0).unwrap(); // Jan 6, 2023 was a Friday
        assert!(matches_predicates(
            &predicates,
            &PredicateCombination::Any,
            &friday,
            &None
        ));

        // Test with a non-matching day (neither Monday nor Friday)
        let tuesday = Utc.with_ymd_and_hms(2023, 1, 3, 10, 0, 0).unwrap(); // Jan 3, 2023 was a Tuesday
        assert!(!matches_predicates(
            &predicates,
            &PredicateCombination::Any,
            &tuesday,
            &None
        ));
    }

    #[test]
    fn test_calculate_next_calendar_time() {
        // Test calculating the next Monday 10:00 AM
        // Starting from a Sunday
        let sunday_noon = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();

        let predicates = vec![
            Predicate::Exact(MONDAY),
            Predicate::Exact(TimeUnit::TimeOfDay(
                chrono::NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            )),
        ];

        let next_time = calculate_next_calendar_time(
            &predicates,
            &PredicateCombination::All,
            datetime_to_timestamp(&sunday_noon).unwrap(),
            &None,
        );

        // The next Monday 10:00 should be 2023-01-02 10:00:00 UTC
        let expected = Utc.with_ymd_and_hms(2023, 1, 2, 10, 0, 0).unwrap();
        assert_eq!(next_time, datetime_to_timestamp(&expected).unwrap());
    }
}
