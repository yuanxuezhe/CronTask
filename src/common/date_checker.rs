use crate::common::consts::*;
use chrono::Datelike;
use chrono::NaiveDate;
use once_cell;
use std::collections::HashSet;

/// 将指定格式的日期字符串解析为NaiveDate对象
///
/// # 参数
/// * `s` - 日期字符串，格式为"%Y%m%d"
///
/// # 返回值
/// 解析成功返回Some(NaiveDate)，失败返回None
pub fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, DATE_FORMAT).ok()
}

/// 判断指定日期是否为周六或周日
///
/// # 参数
/// * `date` - 要判断的日期
///
/// # 返回值
/// 是周末返回true，否则返回false
pub fn is_weekend(date: NaiveDate) -> bool {
    matches!(date.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun)
}

/// 判断指定日期是否为预定义的节假日
///
/// # 参数
/// * `date` - 要判断的日期
///
/// # 返回值
/// 是节假日返回true，否则返回false
pub fn is_holiday(date: NaiveDate) -> bool {
    static HOLIDAYS: once_cell::sync::Lazy<HashSet<NaiveDate>> = once_cell::sync::Lazy::new(|| {
        [
            "20230101", "20230122", "20230123", "20230124", "20230405", "20230501", "20230622",
            "20230929", "20231001",
        ]
        .iter()
        .filter_map(|s| parse_date(s))
        .collect()
    });
    HOLIDAYS.contains(&date)
}

/// 工作日任务在非周末且非节假日时有效
///
/// # 参数
/// * `date` - 要检查的日期
///
/// # 返回值
/// 如果是工作日返回true，否则返回false
pub fn is_workday(date: NaiveDate) -> bool {
    !is_holiday(date) && !is_weekend(date)
}

/// 日常任务每天都有效
///
/// # 参数
/// * `_` - 日期参数（未使用）
///
/// # 返回值
/// 总是返回true
pub fn is_daily_valid(_: NaiveDate) -> bool {
    true
}

/// 根据任务的周期参数检查指定日期是否为有效的星期几
///
/// # 参数
/// * `date` - 要检查的日期
/// * `period` - 任务的周期参数
///
/// # 返回值
/// 如果日期符合每周规则返回true，否则返回false
pub fn check_weekly(date: NaiveDate, period: &str) -> bool {
    let weekday = date.weekday().number_from_monday(); // 从周一为1开始计算
    period[1..]
        .split(',')
        .filter_map(|s| s.parse::<u32>().ok())
        .any(|d| d == weekday)
}

/// 根据任务的周期参数检查指定日期是否为有效的月份中的某一天
///
/// # 参数
/// * `date` - 要检查的日期
/// * `period` - 任务的周期参数
///
/// # 返回值
/// 如果日期符合每月规则返回true，否则返回false
pub fn check_monthly(date: NaiveDate, period: &str) -> bool {
    let day = date.day();
    period[1..]
        .split(',')
        .filter_map(|s| s.parse::<u32>().ok())
        .any(|d| d == day)
}

/// 根据任务的周期参数检查指定日期是否为有效的年份中的某一天
///
/// # 参数
/// * `date` - 要检查的日期
/// * `period` - 任务的周期参数
///
/// # 返回值
/// 如果日期符合每年规则返回true，否则返回false
pub fn check_yearly(date: NaiveDate, period: &str) -> bool {
    let md = format!("{:02}{:02}", date.month(), date.day());
    period[1..] == md
}

/// 根据任务的周期参数检查指定日期是否有效
///
/// # 参数
/// * `date` - 要检查的日期
/// * `period` - 任务的周期参数
///
/// # 返回值
/// 如果日期符合自定义规则返回true，否则返回false
pub fn is_custom_valid(date: NaiveDate, period: &str) -> bool {
    match period.chars().next() {
        Some('D') => true,
        Some('W') => check_weekly(date, period),
        Some('M') => check_monthly(date, period),
        Some('Y') => check_yearly(date, period),
        _ => false,
    }
}

/// 根据任务的周期类型和调度规则检查指定日期是否有效
///
/// # 参数
/// * `date` - 要检查的日期
/// * `cycle_type` - 任务的周期类型
/// * `period` - 任务的周期参数
///
/// # 返回值
/// 如果日期有效返回true，否则返回false
pub fn is_date_valid(date: NaiveDate, cycle_type: &str, period: &str) -> bool {
    match cycle_type {
        CYCLE_TYPE_DAILY => is_daily_valid(date),
        CYCLE_TYPE_WORKDAY => is_workday(date),
        CYCLE_TYPE_HOLIDAY => is_holiday(date),
        CYCLE_TYPE_CUSTOM => is_custom_valid(date, period),
        _ => false,
    }
}