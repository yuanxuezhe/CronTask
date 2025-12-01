use chrono::{NaiveDate, Datelike};
use crate::common::date_checker::{is_weekend, is_holiday, is_workday, parse_date, is_daily_valid, check_weekly, check_monthly, check_yearly, is_custom_valid, is_date_valid};

#[test]
fn test_parse_date() {
    // 测试有效日期
    let date_str = "20230101";
    let result = parse_date(date_str);
    assert!(result.is_some());
    let date = result.unwrap();
    assert_eq!(date.year(), 2023);
    assert_eq!(date.month(), 1);
    assert_eq!(date.day(), 1);
    
    // 测试无效日期
    let invalid_date_str = "20230230";
    let result = parse_date(invalid_date_str);
    assert!(result.is_none());
    
    // 测试空字符串
    let result = parse_date("");
    assert!(result.is_none());
    
    // 测试格式错误的日期
    let result = parse_date("2023-01-01");
    assert!(result.is_none());
}

#[test]
fn test_is_weekend() {
    // 2023-01-01 是星期日
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(is_weekend(date));
    
    // 2023-01-02 是星期一
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
    assert!(!is_weekend(date));
    
    // 2023-01-07 是星期六
    let date = NaiveDate::from_ymd_opt(2023, 1, 7).unwrap();
    assert!(is_weekend(date));
    
    // 测试月末的周末
    let date = NaiveDate::from_ymd_opt(2023, 12, 30).unwrap(); // 星期六
    assert!(is_weekend(date));
    let date = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(); // 星期日
    assert!(is_weekend(date));
}

#[test]
fn test_is_holiday() {
    // 测试所有预定义的节假日
    let holidays = [
        "20230101", "20230122", "20230123", "20230124", 
        "20230405", "20230501", "20230622", "20230929", "20231001"
    ];
    
    for &holiday_str in holidays.iter() {
        let date = NaiveDate::parse_from_str(holiday_str, "%Y%m%d").unwrap();
        assert!(is_holiday(date), "{} 应该是节假日", holiday_str);
    }
    
    // 测试非节假日
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
    assert!(!is_holiday(date));
    
    // 测试2024年的日期（不在预定义列表中）
    let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    assert!(!is_holiday(date));
}

#[test]
fn test_is_workday() {
    // 2023-01-01 是星期日，也是节假日
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(!is_workday(date));
    
    // 2023-01-02 是星期一，工作日
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
    assert!(is_workday(date));
    
    // 2023-01-07 是星期六，非工作日
    let date = NaiveDate::from_ymd_opt(2023, 1, 7).unwrap();
    assert!(!is_workday(date));
    
    // 2023-10-01 是国庆节，非工作日
    let date = NaiveDate::from_ymd_opt(2023, 10, 1).unwrap();
    assert!(!is_workday(date));
    
    // 测试工作日的节假日
    let date = NaiveDate::from_ymd_opt(2023, 5, 1).unwrap(); // 劳动节，星期一
    assert!(!is_workday(date));
}

#[test]
fn test_is_daily_valid() {
    // 日常任务每天都有效
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(is_daily_valid(date));
    
    let date = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();
    assert!(is_daily_valid(date));
    
    // 测试2024年
    let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    assert!(is_daily_valid(date));
}

#[test]
fn test_check_weekly() {
    // 测试每周一和周三有效
    let period = "W1,3";
    
    // 2023-01-02 是星期一
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
    assert!(check_weekly(date, period));
    
    // 2023-01-04 是星期三
    let date = NaiveDate::from_ymd_opt(2023, 1, 4).unwrap();
    assert!(check_weekly(date, period));
    
    // 2023-01-03 是星期二
    let date = NaiveDate::from_ymd_opt(2023, 1, 3).unwrap();
    assert!(!check_weekly(date, period));
    
    // 测试每周多天
    let period = "W1,3,5";
    let date = NaiveDate::from_ymd_opt(2023, 1, 6).unwrap(); // 星期五
    assert!(check_weekly(date, period));
    
    // 测试每周日
    let period = "W7";
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(); // 星期日
    assert!(check_weekly(date, period));
    
    // 测试无效的星期几
    let period = "W8";
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
    assert!(!check_weekly(date, period));
}

#[test]
fn test_check_monthly() {
    // 测试每月1号和15号有效
    let period = "M1,15";
    
    // 2023-01-01 是1号
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(check_monthly(date, period));
    
    // 2023-01-15 是15号
    let date = NaiveDate::from_ymd_opt(2023, 1, 15).unwrap();
    assert!(check_monthly(date, period));
    
    // 2023-01-02 是2号
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
    assert!(!check_monthly(date, period));
    
    // 测试月末
    let period = "M31";
    let date = NaiveDate::from_ymd_opt(2023, 1, 31).unwrap(); // 1月31日
    assert!(check_monthly(date, period));
    let date = NaiveDate::from_ymd_opt(2023, 2, 28).unwrap(); // 2月28日
    assert!(!check_monthly(date, period));
    
    // 测试每月多天
    let period = "M1,10,20";
    let date = NaiveDate::from_ymd_opt(2023, 1, 10).unwrap();
    assert!(check_monthly(date, period));
    let date = NaiveDate::from_ymd_opt(2023, 1, 20).unwrap();
    assert!(check_monthly(date, period));
}

#[test]
fn test_check_yearly() {
    // 测试每年1月1日有效
    let period = "Y0101";
    
    // 2023-01-01 是1月1日
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(check_yearly(date, period));
    
    // 2023-01-02 是1月2日
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
    assert!(!check_yearly(date, period));
    
    // 测试其他日期
    let period = "Y1231";
    let date = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(); // 12月31日
    assert!(check_yearly(date, period));
    
    let period = "Y0618";
    let date = NaiveDate::from_ymd_opt(2023, 6, 18).unwrap(); // 6月18日
    assert!(check_yearly(date, period));
    
    // 测试无效格式
    let period = "Y0230";
    let date = NaiveDate::from_ymd_opt(2023, 2, 28).unwrap();
    assert!(!check_yearly(date, period));
}

#[test]
fn test_is_custom_valid() {
    // 测试日常任务
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(is_custom_valid(date, "D"));
    
    // 测试每周任务
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(); // 星期一
    assert!(is_custom_valid(date, "W1"));
    assert!(!is_custom_valid(date, "W2"));
    
    // 测试每月任务
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(); // 1号
    assert!(is_custom_valid(date, "M1"));
    assert!(!is_custom_valid(date, "M2"));
    
    // 测试每年任务
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(); // 1月1日
    assert!(is_custom_valid(date, "Y0101"));
    assert!(!is_custom_valid(date, "Y0102"));
    
    // 测试无效类型
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(!is_custom_valid(date, "X"));
    
    // 测试空字符串
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(!is_custom_valid(date, ""));
    
    // 测试只有前缀的周期
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
    assert!(!is_custom_valid(date, "W"));
    assert!(!is_custom_valid(date, "M"));
    assert!(!is_custom_valid(date, "Y"));
}

#[test]
fn test_is_date_valid() {
    // 测试日常任务
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(is_date_valid(date, "0", "D"));
    
    // 测试工作日任务
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(); // 星期一，工作日
    assert!(is_date_valid(date, "1", "D"));
    
    let date = NaiveDate::from_ymd_opt(2023, 1, 7).unwrap(); // 星期六，非工作日
    assert!(!is_date_valid(date, "1", "D"));
    
    // 测试节假日任务
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(); // 元旦，节假日
    assert!(is_date_valid(date, "2", "D"));
    
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(); // 工作日，非节假日
    assert!(!is_date_valid(date, "2", "D"));
    
    // 测试自定义任务
    let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(); // 星期一
    assert!(is_date_valid(date, "3", "W1"));
    assert!(!is_date_valid(date, "3", "W2"));
    
    // 测试无效的周期类型
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(!is_date_valid(date, "4", "D"));
    
    // 测试空周期类型
    let date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    assert!(!is_date_valid(date, "", "D"));
    
    // 测试不同年份的自定义任务
    let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(); // 2024年1月1日
    assert!(is_date_valid(date, "3", "Y0101"));
}
