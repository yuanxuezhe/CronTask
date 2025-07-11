use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Utc};
use chrono_tz::Asia::Shanghai;
use dbcore::{Database,ResultSet};
use macros::SqlCRUD;
use chrono::Datelike;
use chrono::Weekday;
use std::collections::HashSet;

// 常量定义
const DATE_FORMAT: &str = "%Y%m%d";
const TIME_FORMAT: &str = "%H%M%S";
const DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

// 任务状态常量
pub const TASK_STATUS_ACTIVE: &str = "A";
pub const TASK_STATUS_INACTIVE: &str = "I";

// 周期类型常量
pub const CYCLE_TYPE_DAILY: &str = "0";
pub const CYCLE_TYPE_WORKDAY: &str = "1";
pub const CYCLE_TYPE_HOLIDAY: &str = "2";
pub const CYCLE_TYPE_CUSTOM: &str = "3";

// 重试类型常量
pub const RETRY_TYPE_NONE: &str = "0";
pub const RETRY_TYPE_FORWARD: &str = "1";
pub const RETRY_TYPE_BACKWARD: &str = "2";

#[derive(Debug, Clone)]
pub struct TaskDetail {
    pub taskid: i32,
    pub timepoint: String,
    pub current_trigger_count: i32,
    pub status: i32,
    pub tag: i32,
}

#[derive(Debug, Default, Clone, SqlCRUD)]
#[table_name = "task"]
pub struct Task {
    #[primary_key]
    pub taskid: i32,
    pub taskname: String,
    pub start_date: String,
    pub end_date: String,
    // 周期类型: 0-每天, 1-工作日, 2-节假日, 3-自定义
    pub cycle_type: String,
    // 所属周期: D-每天, W{1,3}-每周, M{1,3}-每月, Y{1,3}-每年
    pub period: String, 
    pub time_point: String,
    // 重试类型: 0-不重试, 1-向后, 2-向前
    pub retry_type: String,
    pub retry_interval: i32,
    pub retry_count: i32,
    pub status: String,
    pub discribe: String,
}

/// 解析日期（格式：YYYYMMDD）
fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, DATE_FORMAT).ok()
}

/// 判断是否为周末
fn is_weekend(date: NaiveDate) -> bool {
    matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
}

/// 判断是否为节假日（示例实现，需替换实际数据）
fn is_holiday(date: NaiveDate) -> bool {
    let holidays: HashSet<NaiveDate> = [
        "20230101", "20230122", "20230123", "20230124",
        "20230405", "20230501", "20230622", "20230929",
    ]
    .iter()
    .filter_map(|s| parse_date(s))
    .collect();
    
    holidays.contains(&date)
}

impl Task {
    /// 获取未来指定天数内的有效触发时间点
    pub fn next_n_schedules(&self, maxdays: u32) -> Vec<NaiveDateTime> {
        let current_time = Utc::now().with_timezone(&Shanghai).naive_local();

        // 解析多个时间点
        let time_points: Vec<NaiveTime> = self.time_point
            .split(',')
            .filter_map(|s| NaiveTime::parse_from_str(s.trim(), TIME_FORMAT).ok())
            .collect();

        if time_points.is_empty() {
            return vec![];
        }

        // 生成候选日期
        let candidates = self.generate_candidate_dates(current_time.date(), maxdays)
            .unwrap_or_default();

        // 生成有效时间点
        candidates
            .into_iter()
            .flat_map(|date| {
                time_points.iter().filter_map(move |&tp| {
                    let datetime = date.and_time(tp);
                    if datetime > current_time {
                        Some(datetime)
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    /// 生成候选日期（优化版：动态生成避免全量计算）
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

    /// 验证日期有效性
    fn is_date_valid(&self, date: NaiveDate) -> bool {
        // 检查起止日期
        let in_range = self.check_date(date).unwrap_or(false);
        
        // 检查周期类型
        in_range && match self.cycle_type.as_str() {
            CYCLE_TYPE_DAILY => self.is_daily_valid(date),
            CYCLE_TYPE_WORKDAY => self.is_workday(date),
            CYCLE_TYPE_HOLIDAY => self.is_holiday(date),
            CYCLE_TYPE_CUSTOM => self.is_custom_valid(date),
            _ => false
        }
    }

    /// 检查日期范围
    fn check_date(&self, date: NaiveDate) -> Result<bool, String> {
        let start = parse_date(&self.start_date).ok_or("无效开始日期")?;
        let end = parse_date(&self.end_date).ok_or("无效结束日期")?;
        Ok(date >= start && date <= end)
    }

    /// 每天的有效性检查
    fn is_daily_valid(&self, _: NaiveDate) -> bool {
        true
    }

    /// 工作日检查
    fn is_workday(&self, date: NaiveDate) -> bool {
        !is_holiday(date) && !is_weekend(date)
    }

    /// 节假日检查
    fn is_holiday(&self, date: NaiveDate) -> bool {
        is_holiday(date)
    }

    /// 自定义周期检查
    fn is_custom_valid(&self, date: NaiveDate) -> bool {
        match self.period.chars().next() {
            Some('D') => true,
            Some('W') => self.check_weekly(date),
            Some('M') => self.check_monthly(date),
            Some('Y') => self.check_yearly(date),
            _ => false
        }
    }

    /// 周规则检查（示例：W13 表示每周一、三）
    fn check_weekly(&self, date: NaiveDate) -> bool {
        let weekday = date.weekday() as u8 + 1; // 周一=1,...,周日=7
        self.period[1..]
            .chars()
            .filter_map(|c| c.to_digit(10))
            .any(|d| d == weekday as u32)
    }

    /// 月规则检查（示例：M15 表示每月15号）
    fn check_monthly(&self, date: NaiveDate) -> bool {
        let day = date.day();
        self.period[1..]
            .chars()
            .filter_map(|c| c.to_digit(10))
            .any(|d| d == day)
    }

    /// 年规则检查（示例：Y0101 表示每年1月1日）
    fn check_yearly(&self, date: NaiveDate) -> bool {
        let md = format!("{:02}{:02}", date.month(), date.day());
        self.period[1..] == md
    }
}