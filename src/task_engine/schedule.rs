use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Utc, Datelike};
use chrono_tz::Asia::Shanghai;
//use crate::task::model::Task;
use crate::task_engine::holiday::{is_holiday, is_weekend, parse_date};
use crate::common::consts::*;
use crate::common::error::CronTaskError;

impl crate::task_engine::model::Task {
    /// 根据任务的调度规则计算指定时间范围内的所有调度时间点列表
    /// 
    /// # 参数
    /// * `max_future_time` - 最大未来时间
    /// 
    /// # 返回值
    /// 返回指定时间范围内的所有调度时间点列表
    pub fn get_schedules_in_range(&self, max_future_time: NaiveDateTime) -> Vec<NaiveDateTime> {
        let current_time = Utc::now().with_timezone(&Shanghai).naive_local();
        
        let max_days = (max_future_time - current_time).num_days() as u32 + 1;
        
        let time_points: Vec<NaiveTime> = self.time_point
            .split(',')
            .filter_map(|s| NaiveTime::parse_from_str(s.trim(), TIME_FORMAT).ok())
            .collect();
        if time_points.is_empty() {
            return vec![];
        }
        let candidates = self.generate_candidate_dates(current_time.date(), max_days)
            .unwrap_or_default();
        let mut result = Vec::with_capacity(candidates.len() * time_points.len());
        for date in candidates {
            for &tp in &time_points {
                let datetime = date.and_time(tp);
                if datetime > current_time && datetime <= max_future_time {
                    result.push(datetime);
                }
            }
        }
        result
    }
    
    /// 根据任务调度规则生成未来指定天数内的候选日期列表
    /// 
    /// # 参数
    /// * `start` - 开始日期
    /// * `max_days` - 最大天数
    /// 
    /// # 返回值
    /// 返回候选日期列表
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
    
    /// 根据任务的周期类型和调度规则检查指定日期是否有效
    /// 
    /// # 参数
    /// * `date` - 要检查的日期
    /// 
    /// # 返回值
    /// 如果日期有效返回true，否则返回false
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
    
    /// 检查指定日期是否在任务的开始日期和结束日期之间
    /// 
    /// # 参数
    /// * `date` - 要检查的日期
    /// 
    /// # 返回值
    /// 成功时返回检查结果，失败时返回错误信息
    fn check_date(&self, date: NaiveDate) -> Result<bool, CronTaskError> {
        use crate::common::error::CronTaskError;
        let start = parse_date(&self.start_date).ok_or(CronTaskError::TaskFormatError("无效开始日期".to_string()))?;
        let end = parse_date(&self.end_date).ok_or(CronTaskError::TaskFormatError("无效结束日期".to_string()))?;
        Ok(date >= start && date <= end)
    }
    
    /// 日常任务每天都有效
    /// 
    /// # 参数
    /// * `_` - 日期参数（未使用）
    /// 
    /// # 返回值
    /// 总是返回true
    fn is_daily_valid(&self, _: NaiveDate) -> bool {
        true
    }
    
    /// 工作日任务在非周末且非节假日时有效
    /// 
    /// # 参数
    /// * `date` - 要检查的日期
    /// 
    /// # 返回值
    /// 如果是工作日返回true，否则返回false
    fn is_workday(&self, date: NaiveDate) -> bool {
        !is_holiday(date) && !is_weekend(date)
    }
    
    /// 节假日任务仅在节假日时有效
    /// 
    /// # 参数
    /// * `date` - 要检查的日期
    /// 
    /// # 返回值
    /// 如果是节假日返回true，否则返回false
    fn is_holiday(&self, date: NaiveDate) -> bool {
        is_holiday(date)
    }
    
    /// 根据任务的周期参数检查指定日期是否有效
    /// 
    /// # 参数
    /// * `date` - 要检查的日期
    /// 
    /// # 返回值
    /// 如果日期符合自定义规则返回true，否则返回false
    fn is_custom_valid(&self, date: NaiveDate) -> bool {
        match self.period.chars().next() {
            Some('D') => true,
            Some('W') => self.check_weekly(date),
            Some('M') => self.check_monthly(date),
            Some('Y') => self.check_yearly(date),
            _ => false
        }
    }
    
    /// 根据任务的周期参数检查指定日期是否为有效的星期几
    /// 
    /// # 参数
    /// * `date` - 要检查的日期
    /// 
    /// # 返回值
    /// 如果日期符合每周规则返回true，否则返回false
    fn check_weekly(&self, date: NaiveDate) -> bool {
        let weekday = date.weekday().number_from_monday(); // 从周一为1开始计算
        self.period[1..]
            .chars()
            .filter_map(|c| c.to_digit(10))
            .any(|d| d == weekday)
    }
    
    /// 根据任务的周期参数检查指定日期是否为有效的月份中的某一天
    /// 
    /// # 参数
    /// * `date` - 要检查的日期
    /// 
    /// # 返回值
    /// 如果日期符合每月规则返回true，否则返回false
    fn check_monthly(&self, date: NaiveDate) -> bool {
        let day = date.day();
        self.period[1..]
            .chars()
            .filter_map(|c| c.to_digit(10))
            .any(|d| d == day)
    }
    
    /// 根据任务的周期参数检查指定日期是否为有效的年份中的某一天
    /// 
    /// # 参数
    /// * `date` - 要检查的日期
    /// 
    /// # 返回值
    /// 如果日期符合每年规则返回true，否则返回false
    fn check_yearly(&self, date: NaiveDate) -> bool {
        let md = format!("{:02}{:02}", date.month(), date.day());
        self.period[1..] == md
    }
}