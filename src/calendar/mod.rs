use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

mod builder;
mod calculator;

pub use builder::CalendarScheduleBuilder;
pub use calculator::calculate_next_calendar_time;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Copy, PartialOrd, Ord)]
pub enum Month {
    January = 1,
    February = 2,
    March = 3,
    April = 4,
    May = 5,
    June = 6,
    July = 7,
    August = 8,
    September = 9,
    October = 10,
    November = 11,
    December = 12,
}

macro_rules! impl_from_for_month {
    ($($t:ty),*) => {
        $(
            impl From<$t> for Month {
                fn from(value: $t) -> Self {
                    match value {
                        1 => Month::January,
                        2 => Month::February,
                        3 => Month::March,
                        4 => Month::April,
                        5 => Month::May,
                        6 => Month::June,
                        7 => Month::July,
                        8 => Month::August,
                        9 => Month::September,
                        10 => Month::October,
                        11 => Month::November,
                        12 => Month::December,
                        _ => panic!("Invalid month value: {}", value),
                    }
                }
            }
        )*
    };
}

// Implement From for all numeric types
impl_from_for_month!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl fmt::Display for Month {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Month::January => write!(f, "January"),
            Month::February => write!(f, "February"),
            Month::March => write!(f, "March"),
            Month::April => write!(f, "April"),
            Month::May => write!(f, "May"),
            Month::June => write!(f, "June"),
            Month::July => write!(f, "July"),
            Month::August => write!(f, "August"),
            Month::September => write!(f, "September"),
            Month::October => write!(f, "October"),
            Month::November => write!(f, "November"),
            Month::December => write!(f, "December"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Copy, PartialOrd, Ord)]
pub enum DayOfWeek {
    Sunday = 0,
    Monday = 1,
    Tuesday = 2,
    Wednesday = 3,
    Thursday = 4,
    Friday = 5,
    Saturday = 6,
}

impl fmt::Display for DayOfWeek {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DayOfWeek::Sunday => write!(f, "Sunday"),
            DayOfWeek::Monday => write!(f, "Monday"),
            DayOfWeek::Tuesday => write!(f, "Tuesday"),
            DayOfWeek::Wednesday => write!(f, "Wednesday"),
            DayOfWeek::Thursday => write!(f, "Thursday"),
            DayOfWeek::Friday => write!(f, "Friday"),
            DayOfWeek::Saturday => write!(f, "Saturday"),
        }
    }
}

macro_rules! impl_from_for_day_of_week {
    ($($t:ty),*) => {
        $(
            impl From<$t> for DayOfWeek {
                fn from(value: $t) -> Self {
                    match value {
                        0 => DayOfWeek::Sunday,
                        1 => DayOfWeek::Monday,
                        2 => DayOfWeek::Tuesday,
                        3 => DayOfWeek::Wednesday,
                        4 => DayOfWeek::Thursday,
                        5 => DayOfWeek::Friday,
                        6 => DayOfWeek::Saturday,
                        _ => panic!("Invalid day of week value: {}", value),
                    }
                }
            }
        )*
    };
}

impl_from_for_day_of_week!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

/// A unit of time used for calendar-based schedules
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeUnit {
    /// Specific time of day in "HH:MM:SS" or "HH:MM" format (e.g., "15:30")
    TimeOfDay(NaiveTime),

    /// Day of week (0-6, where 0 is Sunday)
    DayOfWeek(DayOfWeek),

    /// Day of month (1-31)
    DayOfMonth(u8),

    /// Month of year (1-12)
    Month(Month),

    /// Specific date (YYYY-MM-DD)
    Date(NaiveDate),
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeUnit::TimeOfDay(time) => write!(f, "at {}", time),
            TimeUnit::DayOfWeek(day) => write!(f, "on {}", day),
            TimeUnit::DayOfMonth(day) => write!(f, "on day {} of month", day),
            TimeUnit::Month(month) => write!(f, "in {}", month),
            TimeUnit::Date(date) => write!(f, "on {}", date),
        }
    }
}

/// A predicate for time matching in calendar-based schedules
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Predicate {
    /// Single time unit to match
    Exact(TimeUnit),

    /// Match any of the time units (OR logic)
    Any(Vec<TimeUnit>),

    /// Match all of the time units (AND logic)
    All(Vec<TimeUnit>),

    /// Create a time-based schedule that runs at fixed duration intervals
    Interval(Duration),
}

impl Predicate {
    /// Check if the predicate matches the given DateTime
    pub fn matches(&self, dt: &DateTime<Utc>, timezone: &Option<FixedOffset>) -> bool {
        match self {
            Predicate::Exact(unit) => {
                if let Some(tz) = timezone {
                    // Convert to the specific timezone if provided
                    let dt_with_tz = dt.with_timezone(tz);
                    matches_time_unit(unit, &dt_with_tz)
                } else {
                    // Use UTC if no timezone provided
                    matches_time_unit(unit, dt)
                }
            }
            Predicate::Any(units) => {
                if let Some(tz) = timezone {
                    // Convert to the specific timezone if provided
                    let dt_with_tz = dt.with_timezone(tz);
                    units
                        .iter()
                        .any(|unit| matches_time_unit(unit, &dt_with_tz))
                } else {
                    // Use UTC if no timezone provided
                    units.iter().any(|unit| matches_time_unit(unit, dt))
                }
            }
            Predicate::All(units) => {
                if let Some(tz) = timezone {
                    // Convert to the specific timezone if provided
                    let dt_with_tz = dt.with_timezone(tz);
                    units
                        .iter()
                        .all(|unit| matches_time_unit(unit, &dt_with_tz))
                } else {
                    // Use UTC if no timezone provided
                    units.iter().all(|unit| matches_time_unit(unit, dt))
                }
            }
            Predicate::Interval(_) => true, // Interval predicates are handled separately
        }
    }
}

/// How to combine multiple time predicates
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PredicateCombination {
    /// All predicates must match (AND)
    All,

    /// Any predicate can match (OR)
    Any,
}

/// Check if a specific time unit matches a DateTime
fn matches_time_unit<Tz: TimeZone>(unit: &TimeUnit, dt: &DateTime<Tz>) -> bool {
    match unit {
        TimeUnit::TimeOfDay(time) => {
            let dt_time = dt.time();
            dt_time.hour() == time.hour()
                && dt_time.minute() == time.minute()
                && (time.second() == 0 || dt_time.second() == time.second())
        }
        TimeUnit::DayOfWeek(day) => dt.weekday().num_days_from_sunday() == *day as u32,
        TimeUnit::DayOfMonth(day) => dt.day() == *day as u32,
        TimeUnit::Month(month) => dt.month() == *month as u32,
        TimeUnit::Date(date) => {
            dt.year() == date.year() && dt.month() == date.month() && dt.day() == date.day()
        }
    }
}

/// Parse a time string in various formats
pub fn parse_time(time_str: &str) -> Result<NaiveTime, String> {
    // Try multiple formats in succession
    let formats = [
        "%H:%M",       // 15:30
        "%H:%M:%S",    // 15:30:45
        "%I:%M %p",    // 3:30 PM
        "%I:%M:%S %p", // 3:30:45 PM
        "%I:%M%p",     // 3:30PM
        "%I:%M:%S%p",  // 3:30:45PM
    ];

    // Try each format until we find one that works
    for format in &formats {
        if let Ok(time) = NaiveTime::parse_from_str(time_str, format) {
            return Ok(time);
        }
    }

    // If none of the formats matched, return an error
    Err(format!("Could not parse time: {}", time_str))
}

/// Constants for days of the week
pub const SUNDAY: TimeUnit = TimeUnit::DayOfWeek(DayOfWeek::Sunday);
pub const MONDAY: TimeUnit = TimeUnit::DayOfWeek(DayOfWeek::Monday);
pub const TUESDAY: TimeUnit = TimeUnit::DayOfWeek(DayOfWeek::Tuesday);
pub const WEDNESDAY: TimeUnit = TimeUnit::DayOfWeek(DayOfWeek::Wednesday);
pub const THURSDAY: TimeUnit = TimeUnit::DayOfWeek(DayOfWeek::Thursday);
pub const FRIDAY: TimeUnit = TimeUnit::DayOfWeek(DayOfWeek::Friday);
pub const SATURDAY: TimeUnit = TimeUnit::DayOfWeek(DayOfWeek::Saturday);

/// Extension trait for numeric values to create durations
pub trait DurationExt {
    fn seconds(self) -> Duration;
    fn minutes(self) -> Duration;
    fn hours(self) -> Duration;
    fn days(self) -> Duration;
    fn weeks(self) -> Duration;
}

impl DurationExt for u64 {
    fn seconds(self) -> Duration {
        Duration::from_secs(self)
    }
    fn minutes(self) -> Duration {
        Duration::from_secs(self * 60)
    }
    fn hours(self) -> Duration {
        Duration::from_secs(self * 3600)
    }
    fn days(self) -> Duration {
        Duration::from_secs(self * 86400)
    }
    fn weeks(self) -> Duration {
        Duration::from_secs(self * 604800)
    }
}

impl DurationExt for u32 {
    fn seconds(self) -> Duration {
        Duration::from_secs(self as u64)
    }
    fn minutes(self) -> Duration {
        Duration::from_secs((self * 60) as u64)
    }
    fn hours(self) -> Duration {
        Duration::from_secs((self * 3600) as u64)
    }
    fn days(self) -> Duration {
        Duration::from_secs((self * 86400) as u64)
    }
    fn weeks(self) -> Duration {
        Duration::from_secs((self * 604800) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_matches_time_unit_time_of_day() {
        let time = NaiveTime::from_hms_opt(15, 30, 0).unwrap();
        let unit = TimeUnit::TimeOfDay(time);

        // Matching time
        let dt1 = Utc.with_ymd_and_hms(2023, 1, 1, 15, 30, 0).unwrap();
        assert!(matches_time_unit(&unit, &dt1));

        // Non-matching time
        let dt2 = Utc.with_ymd_and_hms(2023, 1, 1, 16, 30, 0).unwrap();
        assert!(!matches_time_unit(&unit, &dt2));
    }

    #[test]
    fn test_matches_time_unit_day_of_week() {
        // Monday
        let unit = TimeUnit::DayOfWeek(1.into());

        // Monday 2023-01-02
        let dt1 = Utc.with_ymd_and_hms(2023, 1, 2, 12, 0, 0).unwrap();
        assert!(matches_time_unit(&unit, &dt1));

        // Tuesday 2023-01-03
        let dt2 = Utc.with_ymd_and_hms(2023, 1, 3, 12, 0, 0).unwrap();
        assert!(!matches_time_unit(&unit, &dt2));
    }

    #[test]
    fn test_matches_time_unit_day_of_month() {
        let unit = TimeUnit::DayOfMonth(15);

        // 15th of January
        let dt1 = Utc.with_ymd_and_hms(2023, 1, 15, 12, 0, 0).unwrap();
        assert!(matches_time_unit(&unit, &dt1));

        // 16th of January
        let dt2 = Utc.with_ymd_and_hms(2023, 1, 16, 12, 0, 0).unwrap();
        assert!(!matches_time_unit(&unit, &dt2));
    }

    #[test]
    fn test_matches_time_unit_month() {
        let unit = TimeUnit::Month(Month::March); // March

        // March 2023
        let dt1 = Utc.with_ymd_and_hms(2023, 3, 15, 12, 0, 0).unwrap();
        assert!(matches_time_unit(&unit, &dt1));

        // April 2023
        let dt2 = Utc.with_ymd_and_hms(2023, 4, 15, 12, 0, 0).unwrap();
        assert!(!matches_time_unit(&unit, &dt2));
    }

    #[test]
    fn test_matches_time_unit_date() {
        let date = NaiveDate::from_ymd_opt(2023, 5, 25).unwrap();
        let unit = TimeUnit::Date(date);

        // May 25, 2023
        let dt1 = Utc.with_ymd_and_hms(2023, 5, 25, 12, 0, 0).unwrap();
        assert!(matches_time_unit(&unit, &dt1));

        // May 26, 2023
        let dt2 = Utc.with_ymd_and_hms(2023, 5, 26, 12, 0, 0).unwrap();
        assert!(!matches_time_unit(&unit, &dt2));
    }

    #[test]
    fn test_predicate_exact() {
        let time = NaiveTime::from_hms_opt(15, 30, 0).unwrap();
        let pred = Predicate::Exact(TimeUnit::TimeOfDay(time));

        // Matching time
        let dt1 = Utc::now()
            .with_hour(15)
            .unwrap()
            .with_minute(30)
            .unwrap()
            .with_second(0)
            .unwrap();
        assert!(pred.matches(&dt1, &None));

        // Non-matching time
        let dt2 = Utc::now()
            .with_hour(16)
            .unwrap()
            .with_minute(30)
            .unwrap()
            .with_second(0)
            .unwrap();
        assert!(!pred.matches(&dt2, &None));
    }

    #[test]
    fn test_predicate_any() {
        let pred = Predicate::Any(vec![
            TimeUnit::DayOfWeek(DayOfWeek::Monday),    // Monday
            TimeUnit::DayOfWeek(DayOfWeek::Wednesday), // Wednesday
        ]);

        // Monday
        let dt1 = Utc.with_ymd_and_hms(2023, 1, 2, 12, 0, 0).unwrap();
        assert!(pred.matches(&dt1, &None));

        // Wednesday
        let dt2 = Utc.with_ymd_and_hms(2023, 1, 4, 12, 0, 0).unwrap();
        assert!(pred.matches(&dt2, &None));

        // Tuesday (should not match)
        let dt3 = Utc.with_ymd_and_hms(2023, 1, 3, 12, 0, 0).unwrap();
        assert!(!pred.matches(&dt3, &None));
    }

    #[test]
    fn test_predicate_all() {
        let pred = Predicate::All(vec![
            TimeUnit::DayOfWeek(DayOfWeek::Monday), // Monday
            TimeUnit::DayOfMonth(2),                // 2nd of month
        ]);

        // Monday, January 2nd, 2023
        let dt1 = Utc.with_ymd_and_hms(2023, 1, 2, 12, 0, 0).unwrap();
        assert!(pred.matches(&dt1, &None));

        // Monday, January 9th, 2023 (wrong day of month)
        let dt2 = Utc.with_ymd_and_hms(2023, 1, 9, 12, 0, 0).unwrap();
        assert!(!pred.matches(&dt2, &None));

        // Tuesday, January 2nd, 2024 (wrong day of week)
        let dt3 = Utc.with_ymd_and_hms(2024, 1, 2, 12, 0, 0).unwrap();
        assert!(!pred.matches(&dt3, &None));
    }

    #[test]
    fn test_duration_ext() {
        assert_eq!(5u64.seconds(), Duration::from_secs(5));
        assert_eq!(2u64.minutes(), Duration::from_secs(120));
        assert_eq!(3u64.hours(), Duration::from_secs(10800));
        assert_eq!(1u64.days(), Duration::from_secs(86400));
        assert_eq!(2u64.weeks(), Duration::from_secs(1209600));
    }

    #[test]
    fn test_parse_time() {
        // Test various time formats
        assert_eq!(
            parse_time("15:30").unwrap(),
            NaiveTime::from_hms_opt(15, 30, 0).unwrap()
        );

        assert_eq!(
            parse_time("15:30:45").unwrap(),
            NaiveTime::from_hms_opt(15, 30, 45).unwrap()
        );

        assert_eq!(
            parse_time("3:30 PM").unwrap(),
            NaiveTime::from_hms_opt(15, 30, 0).unwrap()
        );

        assert_eq!(
            parse_time("3:30PM").unwrap(),
            NaiveTime::from_hms_opt(15, 30, 0).unwrap()
        );
    }
}
