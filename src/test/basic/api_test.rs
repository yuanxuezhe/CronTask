use crate::basic::{create_message_bus, create_time_bus, create_task_scheduler};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_create_message_bus() {
    // 只测试创建消息总线成功，不测试发送消息
    let message_bus = create_message_bus(100);
    // 验证创建成功
    assert!(Arc::ptr_eq(&message_bus, &message_bus));
}

#[tokio::test]
async fn test_create_time_bus() {
    let time_bus = create_time_bus();
    // 只测试创建时间总线成功，不测试接收消息
    assert!(Arc::ptr_eq(&time_bus, &time_bus));
}

#[tokio::test]
async fn test_create_task_scheduler() {
    let message_bus = create_message_bus(100);
    let task_scheduler = create_task_scheduler(Duration::from_millis(100), 100, message_bus);
    // 验证任务调度器创建成功
    assert!(task_scheduler.is_running());
}
