use chrono::{NaiveDate};
use chrono::Datelike;
use once_cell;
use std::collections::HashSet;
use crate::consts::*;

pub fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, DATE_FORMAT).ok()
}

pub fn is_weekend(date: NaiveDate) -> bool {
    matches!(date.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun)
}

pub fn is_holiday(date: NaiveDate) -> bool {
    static HOLIDAYS: once_cell::sync::Lazy<HashSet<NaiveDate>> = once_cell::sync::Lazy::new(|| {
        [
            "20230101", "20230122", "20230123", "20230124",
            "20230405", "20230501", "20230622", "20230929",
        ]
        .iter()
        .filter_map(|s| parse_date(s))
        .collect()
    });
    HOLIDAYS.contains(&date)
}