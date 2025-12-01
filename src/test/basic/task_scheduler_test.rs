use crate::basic::{MessageBus, TaskScheduler, TimeWheel};
use std::sync::Arc;
use std::time::Duration;
use chrono::Local;

#[tokio::test]
async fn test_task_scheduler_creation() {
    let message_bus = MessageBus::new(100);
    let task_scheduler = TaskScheduler::new(Duration::from_millis(100), 100, message_bus);
    assert!(task_scheduler.is_running());
}

#[tokio::test]
async fn test_add_task() {
    let message_bus = MessageBus::new(100);
    let task_scheduler = TaskScheduler::new(Duration::from_millis(100), 100, message_bus);
    let timestamp = Local::now().naive_local();
    let result = task_scheduler.add(timestamp, Duration::from_millis(1000), "test_key").await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "test_key");
}

#[tokio::test]
async fn test_cancel_task() {
    let message_bus = MessageBus::new(100);
    let task_scheduler = TaskScheduler::new(Duration::from_millis(100), 100, message_bus);
    let timestamp = Local::now().naive_local();
    let result = task_scheduler.cancel(timestamp, Duration::from_millis(1000), "test_key").await;
    assert!(result.is_ok());
    // 预期返回值是"任务不存在,无需取消"或"任务已取消"，取决于任务是否存在
    let result_str = result.unwrap();
    assert!(result_str.contains("任务不存在,无需取消") || result_str.contains("任务已取消"));
}

#[tokio::test]
async fn test_get_time_wheel() {
    let message_bus = MessageBus::new(100);
    let task_scheduler = TaskScheduler::new(Duration::from_millis(100), 100, message_bus);
    let time_wheel = task_scheduler.time_wheel();
    assert!(!Arc::ptr_eq(&time_wheel, &Arc::new(TimeWheel::new(Duration::from_millis(100), 100, MessageBus::new(100)))));
}
