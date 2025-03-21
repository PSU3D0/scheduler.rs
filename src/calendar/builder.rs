use crate::calendar::{
    FRIDAY, MONDAY, Predicate, PredicateCombination, SATURDAY, SUNDAY, THURSDAY, TUESDAY, TimeUnit,
    WEDNESDAY, parse_time,
};
use crate::{ScheduleType, Scheduler, SchedulerError};

/// Builder for calendar-based schedules
#[derive(Clone)]
pub struct CalendarScheduleBuilder<'a> {
    scheduler: &'a Scheduler,
    name: String,
    predicates: Vec<Predicate>,
    combine: PredicateCombination,
    callback_id: Option<String>,
    event_buses: Vec<String>,
}

impl<'a> CalendarScheduleBuilder<'a> {
    /// Create a new calendar schedule builder
    pub fn new(scheduler: &'a Scheduler, name: String) -> Self {
        Self {
            scheduler,
            name,
            predicates: Vec::new(),
            combine: PredicateCombination::All,
            callback_id: None,
            event_buses: Vec::new(),
        }
    }

    /// Specify a specific time of day (24-hour or 12-hour format)
    pub fn at(&mut self, time_str: &str) -> Result<&mut Self, SchedulerError> {
        let time = parse_time(time_str)
            .map_err(|_| SchedulerError::Other(format!("Invalid time format: {}", time_str)))?;

        self.predicates
            .push(Predicate::Exact(TimeUnit::TimeOfDay(time)));
        Ok(self)
    }

    /// Specify multiple times of day
    pub fn at_times(&mut self, times: &[&str]) -> Result<&mut Self, SchedulerError> {
        let mut time_units = Vec::with_capacity(times.len());

        for time_str in times {
            let time = parse_time(time_str)
                .map_err(|_| SchedulerError::Other(format!("Invalid time format: {}", time_str)))?;

            time_units.push(TimeUnit::TimeOfDay(time));
        }

        self.predicates.push(Predicate::Any(time_units));
        Ok(self)
    }

    /// Schedule on specific day(s) of the week
    pub fn on_days(&mut self, days: &[TimeUnit]) -> &mut Self {
        if days.len() == 1 {
            self.predicates.push(Predicate::Exact(days[0].clone()));
        } else {
            self.predicates.push(Predicate::Any(days.to_vec()));
        }
        self
    }

    /// Schedule on Mondays
    pub fn on_monday(&mut self) -> &mut Self {
        self.predicates.push(Predicate::Exact(MONDAY));
        self
    }

    /// Schedule on Tuesdays
    pub fn on_tuesday(&mut self) -> &mut Self {
        self.predicates.push(Predicate::Exact(TUESDAY));
        self
    }

    /// Schedule on Wednesdays
    pub fn on_wednesday(&mut self) -> &mut Self {
        self.predicates.push(Predicate::Exact(WEDNESDAY));
        self
    }

    /// Schedule on Thursdays
    pub fn on_thursday(&mut self) -> &mut Self {
        self.predicates.push(Predicate::Exact(THURSDAY));
        self
    }

    /// Schedule on Fridays
    pub fn on_friday(&mut self) -> &mut Self {
        self.predicates.push(Predicate::Exact(FRIDAY));
        self
    }

    /// Schedule on Saturdays
    pub fn on_saturday(&mut self) -> &mut Self {
        self.predicates.push(Predicate::Exact(SATURDAY));
        self
    }

    /// Schedule on Sundays
    pub fn on_sunday(&mut self) -> &mut Self {
        self.predicates.push(Predicate::Exact(SUNDAY));
        self
    }

    /// Schedule on weekdays (Monday through Friday)
    pub fn on_weekdays(&mut self) -> &mut Self {
        self.predicates.push(Predicate::Any(vec![
            MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY,
        ]));
        self
    }

    /// Schedule on weekends (Saturday and Sunday)
    pub fn on_weekends(&mut self) -> &mut Self {
        self.predicates.push(Predicate::Any(vec![SATURDAY, SUNDAY]));
        self
    }

    /// Schedule on a specific day of the month (1-31)
    pub fn on_day_of_month(&mut self, day: u8) -> Result<&mut Self, SchedulerError> {
        if !(1..=31).contains(&day) {
            return Err(SchedulerError::Other(format!(
                "Invalid day of month: {}",
                day
            )));
        }

        self.predicates
            .push(Predicate::Exact(TimeUnit::DayOfMonth(day)));
        Ok(self)
    }

    /// Schedule on a specific month (1-12)
    pub fn in_month(&mut self, month: u8) -> Result<&mut Self, SchedulerError> {
        if !(1..=12).contains(&month) {
            return Err(SchedulerError::Other(format!("Invalid month: {}", month)));
        }

        self.predicates
            .push(Predicate::Exact(TimeUnit::Month(month.into())));
        Ok(self)
    }

    /// Set the callback handler ID for this schedule
    pub fn with_callback_id(&mut self, id: &str) -> &mut Self {
        self.callback_id = Some(id.to_string());
        self
    }

    /// Add an event bus for this schedule
    pub fn with_event_bus(&mut self, bus_id: &str) -> &mut Self {
        self.event_buses.push(bus_id.to_string());
        self
    }

    /// Combine predicates using OR logic (default is AND)
    pub fn with_any_predicate(&mut self) -> &mut Self {
        self.combine = PredicateCombination::Any;
        self
    }

    /// Combine predicates using AND logic (this is the default)
    pub fn with_all_predicates(&mut self) -> &mut Self {
        self.combine = PredicateCombination::All;
        self
    }

    /// Build and register this schedule with the scheduler
    pub fn build(&self) -> Result<String, SchedulerError> {
        if self.predicates.is_empty() {
            return Err(SchedulerError::Other(
                "No schedule predicates defined".to_string(),
            ));
        }

        // Create the calendar-based schedule
        let schedule_type = ScheduleType::Calendar {
            predicates: self.predicates.clone(),
            combine: self.combine.clone(),
            next_time: None, // Will be calculated when the scheduler processes the schedule
        };

        // Create and register the schedule
        let id = self.scheduler.add_schedule(crate::ScheduleDefinition {
            name: Some(self.name.clone()),
            schedule_type,
            callback_id: self.callback_id.clone(),
            event_buses: self.event_buses.clone(),
            limits: Default::default(),
        })?;

        Ok(id)
    }

    // Helper method to register a closure and get a callback ID
    fn register_closure_internal(
        &self,
        handler_box: Box<dyn crate::CallbackHandler>,
    ) -> Result<String, SchedulerError> {
        use uuid::Uuid;
        let id = format!("closure_{}", Uuid::new_v4());

        // Register the handler
        let mut registry = self.scheduler.callback_registry.lock()?;
        registry.insert(id.clone(), handler_box);

        Ok(id)
    }

    #[cfg(not(feature = "async"))]
    /// Execute a closure when this schedule fires
    pub fn execute<F: Fn(crate::TickEvent) + Send + Sync + Clone + 'static>(
        &self,
        handler: F,
    ) -> Result<String, SchedulerError> {
        // Create a handler from the closure
        let handler_box = Box::new(crate::ClosureHandler { handler });

        // Register the handler and get the ID
        let id = self.register_closure_internal(handler_box)?;

        // Create a clone of self with the callback ID
        let mut builder = self.clone();
        builder.callback_id = Some(id);

        // Build the schedule
        builder.build()
    }

    #[cfg(feature = "async")]
    /// Execute a closure when this schedule fires, with async support
    pub fn execute<F, Fut>(&self, handler: F) -> Result<String, SchedulerError>
    where
        F: Fn(crate::TickEvent) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        use crate::async_support::AsyncClosureHandler;

        // Create a handler from the closure
        let handler_box = Box::new(AsyncClosureHandler { handler });

        // Register the handler and get the ID
        let id = self.register_closure_internal(handler_box)?;

        // Create a clone of self with the callback ID
        let mut builder = self.clone();
        builder.callback_id = Some(id);

        // Build the schedule
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_at_method() {
        let scheduler = Scheduler::new();
        let mut builder = CalendarScheduleBuilder::new(&scheduler, "test".to_string());

        // Test with valid time
        assert!(builder.at("15:30").is_ok());

        // Test with invalid time
        assert!(builder.at("invalid").is_err());
    }

    #[test]
    fn test_on_days_method() {
        let scheduler = Scheduler::new();
        let mut builder = CalendarScheduleBuilder::new(&scheduler, "test".to_string());

        builder.on_days(&[MONDAY, WEDNESDAY, FRIDAY]);
        assert_eq!(builder.predicates.len(), 1);

        // Check that we have a Predicate::Any with 3 day values
        if let Predicate::Any(days) = &builder.predicates[0] {
            assert_eq!(days.len(), 3);
        } else {
            panic!("Expected Predicate::Any");
        }
    }

    #[test]
    fn test_convenience_methods() {
        let scheduler = Scheduler::new();
        let mut builder = CalendarScheduleBuilder::new(&scheduler, "test".to_string());

        // Test weekdays
        builder.on_weekdays();
        assert_eq!(builder.predicates.len(), 1);

        // Clear and test weekends
        builder.predicates.clear();
        builder.on_weekends();
        assert_eq!(builder.predicates.len(), 1);

        // Clear and test individual days
        builder.predicates.clear();
        builder.on_monday().on_wednesday();
        assert_eq!(builder.predicates.len(), 2);
    }
}
