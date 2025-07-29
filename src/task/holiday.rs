use chrono::{NaiveDate};
use chrono::Datelike;
use once_cell;
use std::collections::HashSet;
use crate::consts::*;

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
            "20230101", "20230122", "20230123", "20230124",
            "20230405", "20230501", "20230622", "20230929",
        ]
        .iter()
        .filter_map(|s| parse_date(s))
        .collect()
    });
    HOLIDAYS.contains(&date)
}