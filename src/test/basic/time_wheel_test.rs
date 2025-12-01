use std::sync::Arc;
use std::time::Duration;
use chrono::{Local, TimeDelta};
use crate::basic::{create_message_bus, TimeWheel};
use crate::common::error::CronTaskError;

#[tokio::test]
async fn test_time_wheel_creation() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 86400;
    
    let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus);
    
    assert_eq!(time_wheel.tick_duration, tick_duration);
    assert_eq!(time_wheel.total_slots, total_slots);
    assert_eq!(time_wheel.current_slot.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(time_wheel.slots.len(), total_slots);
}

#[tokio::test]
async fn test_add_and_remove_task() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 86400;
    
    let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus);
    
    let timestamp = Local::now().naive_local();
    let delay = Duration::from_millis(5000);
    let key = "test_task_123".to_string();
    
    // 添加任务
    let result = time_wheel.add_task(timestamp, delay, key.clone()).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), key);
    
    // 删除任务
    let result = time_wheel.del_task(timestamp, delay, key.clone()).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "任务已取消");
}

#[tokio::test]
async fn test_calculate_target_slot() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 86400;
    
    let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus);
    
    let timestamp = Local::now().naive_local();
    let delay = Duration::from_millis(5000);
    
    let result = time_wheel.calculate_target_slot(timestamp, delay);
    assert!(result.is_ok());
    
    let (target_time, target_slot) = result.unwrap();
    assert!(target_time > timestamp);
    assert!(target_slot < total_slots);
}

#[tokio::test]
async fn test_process_current_slot() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 86400;
    
    let time_wheel = Arc::new(TimeWheel::new(tick_duration, total_slots, message_bus));
    
    // 运行时间轮
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        time_wheel.clone().run(|_timestamp| async move {
            // 空回调
        })
    ).await;
    
    // 应该在超时前完成，因为回调是空的
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_add_duplicate_task() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 86400;
    
    let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus);
    
    let timestamp = Local::now().naive_local();
    let delay = Duration::from_millis(5000);
    let key = "duplicate_task".to_string();
    
    // 第一次添加任务，应该成功
    let result = time_wheel.add_task(timestamp, delay, key.clone()).await;
    assert!(result.is_ok());
    
    // 第二次添加相同任务，应该失败
    let result = time_wheel.add_task(timestamp, delay, key.clone()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        CronTaskError::TaskAlreadyExists => assert!(true),
        _ => assert!(false, "预期 TaskAlreadyExists 错误")
    }
}

#[tokio::test]
async fn test_remove_nonexistent_task() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 86400;
    
    let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus);
    
    let timestamp = Local::now().naive_local();
    let delay = Duration::from_millis(5000);
    let key = "nonexistent_task".to_string();
    
    // 删除不存在的任务，应该成功但返回特定消息
    let result = time_wheel.del_task(timestamp, delay, key.clone()).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "任务不存在,无需取消");
}

#[tokio::test]
async fn test_add_past_due_task() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 86400;
    
    let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus);
    
    // 使用过去的时间
    let past_time = Local::now().naive_local() - TimeDelta::hours(1);
    let delay = Duration::from_millis(5000);
    let key = "past_due_task".to_string();
    
    // 添加过期任务，应该失败
    let result = time_wheel.add_task(past_time, delay, key.clone()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        CronTaskError::TaskPastDue => assert!(true),
        _ => assert!(false, "预期 TaskPastDue 错误")
    }
}

#[tokio::test]
async fn test_add_task_too_far_in_future() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 100; // 使用较小的槽数，便于测试
    
    let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus);
    
    let timestamp = Local::now().naive_local();
    // 使用超过总槽数的延迟
    let delay = Duration::from_millis((total_slots * 1000 + 1000) as u64);
    let key = "future_task".to_string();
    
    // 添加未来太远的任务，应该失败
    let result = time_wheel.add_task(timestamp, delay, key.clone()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        CronTaskError::TaskTooFarInFuture => assert!(true),
        _ => assert!(false, "预期 TaskTooFarInFuture 错误")
    }
}

#[tokio::test]
async fn test_get_real_slot() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 86400;
    
    let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus);
    
    let current_time = Local::now().naive_local();
    
    // 测试当前时间的槽位
    let result = time_wheel.get_real_slot(current_time);
    assert!(result.is_ok());
    
    // 测试过去时间的槽位
    let past_time = current_time - TimeDelta::hours(1);
    let result = time_wheel.get_real_slot(past_time);
    assert!(result.is_err());
    match result.unwrap_err() {
        CronTaskError::TaskPastDue => assert!(true),
        _ => assert!(false, "预期 TaskPastDue 错误")
    }
}

#[tokio::test]
async fn test_get_slot() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 100; // 使用较小的槽数，便于测试
    
    let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus);
    
    let current_time = Local::now().naive_local();
    
    // 测试当前时间的槽位
    let result = time_wheel.get_slot(current_time);
    assert!(result.is_ok());
    let current_slot = result.unwrap();
    assert!(current_slot < total_slots);
    
    // 测试未来时间的槽位
    let future_time = current_time + TimeDelta::seconds(50);
    let result = time_wheel.get_slot(future_time);
    assert!(result.is_ok());
    let future_slot = result.unwrap();
    assert!(future_slot < total_slots);
}

#[tokio::test]
async fn test_del_task_past_due() {
    let message_bus = create_message_bus(100);
    let tick_duration = Duration::from_millis(1000);
    let total_slots = 86400;
    
    let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus);
    
    // 使用过去的时间
    let past_time = Local::now().naive_local() - TimeDelta::hours(1);
    let delay = Duration::from_millis(5000);
    let key = "past_due_delete".to_string();
    
    // 删除过期任务，应该成功但返回特定消息
    let result = time_wheel.del_task(past_time, delay, key.clone()).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "计算目标槽位失败: 任务已过期");
}

#[tokio::test]
async fn test_time_wheel_with_different_tick_durations() {
    let message_bus = create_message_bus(100);
    
    // 测试不同的滴答间隔
    let tick_durations = [
        Duration::from_millis(100),
        Duration::from_millis(500),
        Duration::from_millis(1000),
        Duration::from_millis(5000)
    ];
    
    for &tick_duration in tick_durations.iter() {
        let total_slots = 100;
        let time_wheel = TimeWheel::new(tick_duration, total_slots, message_bus.clone());
        
        let timestamp = Local::now().naive_local();
        let delay = Duration::from_millis(2000);
        let key = format!("task_{}", tick_duration.as_millis());
        
        // 添加任务，应该成功
        let result = time_wheel.add_task(timestamp, delay, key.clone()).await;
        assert!(result.is_ok(), "滴答间隔 {}ms 添加任务失败", tick_duration.as_millis());
    }
}
