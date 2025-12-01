use crate::common::error::CronTaskError;

#[test]
fn test_config_error() {
    let error = CronTaskError::ConfigError("测试配置错误".to_string());
    assert_eq!(format!("{}", error), "配置错误: 测试配置错误");
}

#[test]
fn test_task_format_error() {
    let error = CronTaskError::TaskFormatError("测试任务格式错误".to_string());
    assert_eq!(format!("{}", error), "任务格式错误: 测试任务格式错误");
}

#[test]
fn test_task_execution_error() {
    let error = CronTaskError::TaskExecutionError("测试任务执行错误".to_string());
    assert_eq!(format!("{}", error), "任务执行错误: 测试任务执行错误");
}

#[test]
fn test_schedule_error() {
    let error = CronTaskError::ScheduleError("测试调度错误".to_string());
    assert_eq!(format!("{}", error), "调度错误: 测试调度错误");
}

#[test]
fn test_task_not_found() {
    let error = CronTaskError::TaskNotFound("测试任务不存在".to_string());
    assert_eq!(format!("{}", error), "任务不存在: 测试任务不存在");
}

#[test]
fn test_task_past_due() {
    let error = CronTaskError::TaskPastDue;
    assert_eq!(format!("{}", error), "任务已过期");
}

#[test]
fn test_task_too_far_in_future() {
    let error = CronTaskError::TaskTooFarInFuture;
    assert_eq!(format!("{}", error), "任务时间超出范围");
}

#[test]
fn test_time_calculation_error() {
    let error = CronTaskError::TimeCalculationError;
    assert_eq!(format!("{}", error), "时间计算错误");
}

#[test]
fn test_time_conversion_failed() {
    let error = CronTaskError::TimeConversionFailed("测试时间转换失败".to_string());
    assert_eq!(format!("{}", error), "时间转换失败: 测试时间转换失败");
}

#[test]
fn test_time_overflow() {
    let error = CronTaskError::TimeOverflow;
    assert_eq!(format!("{}", error), "时间溢出");
}

#[test]
fn test_task_already_exists() {
    let error = CronTaskError::TaskAlreadyExists;
    assert_eq!(format!("{}", error), "任务已存在");
}
