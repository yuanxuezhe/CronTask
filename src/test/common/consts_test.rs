use crate::common::consts::*;

#[test]
fn test_date_formats() {
    // 验证日期格式常量
    assert_eq!(DATE_FORMAT, "%Y%m%d");
    assert_eq!(TIME_FORMAT, "%H%M%S");
    assert_eq!(DATETIME_FORMAT, "%Y-%m-%d %H:%M:%S");
}

#[test]
fn test_cycle_types() {
    // 验证周期类型常量
    assert_eq!(CYCLE_TYPE_DAILY, "0");
    assert_eq!(CYCLE_TYPE_WORKDAY, "1");
    assert_eq!(CYCLE_TYPE_HOLIDAY, "2");
    assert_eq!(CYCLE_TYPE_CUSTOM, "3");
}

#[test]
fn test_task_status() {
    // 验证任务状态常量
    assert_eq!(TASK_STATUS_UNMONITORED, 0);
    assert_eq!(TASK_STATUS_MONITORING, 1);
    assert_eq!(TASK_STATUS_RETRY, 2);
}

#[test]
fn test_task_tags() {
    // 验证任务标签常量
    assert_eq!(TASK_TAG_DELETE, 0);
    assert_eq!(TASK_TAG_KEEP, 1);
    assert_eq!(TASK_TAG_NEW, 2);
}

#[test]
fn test_other_consts() {
    // 验证其他常量
    assert_eq!(TASK_KEY_SEPARATOR, "|");
    assert_eq!(RELOAD_TASK_NAME, "__reload_tasks__");
}
