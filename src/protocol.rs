use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MAX_EVENTS: usize = 10_000;
pub const MAX_BODY: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub event_id: Uuid,
    pub observed_at_ms: i64,
    pub latitude_e6: i32,
    pub longitude_e6: i32,
    pub speed_cm_s: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub batch_id: Uuid,
    pub events: Vec<Event>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Committed,
    Invalid,
    Conflict,
    Busy,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub batch_id: Uuid,
    pub outcome: Outcome,
    pub inserted: usize,
    pub duplicates: usize,
}

impl Receipt {
    pub fn rejected(batch_id: Uuid, outcome: Outcome) -> Self {
        Self {
            batch_id,
            outcome,
            inserted: 0,
            duplicates: 0,
        }
    }
}

impl Batch {
    pub fn validate(&mut self) -> Result<(), Outcome> {
        if self.batch_id.is_nil() || !(1..=MAX_EVENTS).contains(&self.events.len()) {
            return Err(Outcome::Invalid);
        }

        let mut unique = BTreeMap::new();
        for event in self.events.drain(..) {
            if event.event_id.is_nil()
                || !(0..=253_402_300_799_999).contains(&event.observed_at_ms)
                || !(-90_000_000..=90_000_000).contains(&event.latitude_e6)
                || !(-180_000_000..=180_000_000).contains(&event.longitude_e6)
                || !(0..=20_000).contains(&event.speed_cm_s)
            {
                return Err(Outcome::Invalid);
            }
            match unique.entry(event.event_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(event);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &event => {
                    return Err(Outcome::Conflict);
                }
                _ => {}
            }
        }
        self.events = unique.into_values().collect();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_ids_must_carry_identical_values() {
        let event = Event {
            event_id: Uuid::new_v4(),
            observed_at_ms: 1,
            latitude_e6: 0,
            longitude_e6: 0,
            speed_cm_s: 0,
        };
        let mut batch = Batch {
            batch_id: Uuid::new_v4(),
            events: vec![event.clone(), event.clone()],
        };
        assert!(batch.validate().is_ok());
        assert_eq!(batch.events, vec![event.clone()]);
        batch.events.push(Event {
            speed_cm_s: 1,
            ..event
        });
        assert_eq!(batch.validate(), Err(Outcome::Conflict));
    }
}
