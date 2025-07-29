use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Utc, Datelike};
use chrono_tz::Asia::Shanghai;
//use crate::task::model::Task;
use crate::task::holiday::{is_holiday, is_weekend, parse_date};
use crate::consts::*;

impl crate::task::model::Task {
    pub fn next_n_schedules(&self, maxdays: u32) -> Vec<NaiveDateTime> {
        let current_time = Utc::now().with_timezone(&Shanghai).naive_local();
        let time_points: Vec<NaiveTime> = self.time_point
            .split(',')
            .filter_map(|s| NaiveTime::parse_from_str(s.trim(), TIME_FORMAT).ok())
            .collect();
        if time_points.is_empty() {
            return vec![];
        }
        let candidates = self.generate_candidate_dates(current_time.date(), maxdays)
            .unwrap_or_default();
        let mut result = Vec::with_capacity(candidates.len() * time_points.len());
        for date in candidates {
            for &tp in &time_points {
                let datetime = date.and_time(tp);
                if datetime > current_time {
                    result.push(datetime);
                }
            }
        }
        result
    }
    fn generate_candidate_dates(&self, start: NaiveDate, max_days: u32) -> Option<Vec<NaiveDate>> {
        let mut dates = Vec::with_capacity(max_days as usize);
        let mut current = start;
        let mut remaining = max_days;
        while remaining > 0 {
            if self.is_date_valid(current) {
                dates.push(current);
            } 
            current = match current.succ_opt() {
                Some(next) => next,
                None => break,
            };
            remaining -= 1;
        }
        Some(dates)
    }
    fn is_date_valid(&self, date: NaiveDate) -> bool {
        let in_range = self.check_date(date).unwrap_or(false);
        in_range && match self.cycle_type.as_str() {
            CYCLE_TYPE_DAILY => self.is_daily_valid(date),
            CYCLE_TYPE_WORKDAY => self.is_workday(date),
            CYCLE_TYPE_HOLIDAY => self.is_holiday(date),
            CYCLE_TYPE_CUSTOM => self.is_custom_valid(date),
            _ => false
        }
    }
    fn check_date(&self, date: NaiveDate) -> Result<bool, String> {
        let start = parse_date(&self.start_date).ok_or("无效开始日期")?;
        let end = parse_date(&self.end_date).ok_or("无效结束日期")?;
        Ok(date >= start && date <= end)
    }
    fn is_daily_valid(&self, _: NaiveDate) -> bool {
        true
    }
    fn is_workday(&self, date: NaiveDate) -> bool {
        !is_holiday(date) && !is_weekend(date)
    }
    fn is_holiday(&self, date: NaiveDate) -> bool {
        is_holiday(date)
    }
    fn is_custom_valid(&self, date: NaiveDate) -> bool {
        match self.period.chars().next() {
            Some('D') => true,
            Some('W') => self.check_weekly(date),
            Some('M') => self.check_monthly(date),
            Some('Y') => self.check_yearly(date),
            _ => false
        }
    }
    fn check_weekly(&self, date: NaiveDate) -> bool {
        let weekday = date.weekday() as u8 + 1;
        self.period[1..]
            .chars()
            .filter_map(|c| c.to_digit(10))
            .any(|d| d == weekday as u32)
    }
    fn check_monthly(&self, date: NaiveDate) -> bool {
        let day = date.day();
        self.period[1..]
            .chars()
            .filter_map(|c| c.to_digit(10))
            .any(|d| d == day)
    }
    fn check_yearly(&self, date: NaiveDate) -> bool {
        let md = format!("{:02}{:02}", date.month(), date.day());
        self.period[1..] == md
    }
} 