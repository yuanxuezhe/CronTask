use crate::core::core::CronTask;
use dbcore::Database;
use std::sync::Arc;

#[tokio::test]
async fn test_on_call_back_inner_reload() {
    // 创建一个CronTask实例
    let db = Database::new("evdata.db").await.unwrap();
    let cron_task = Arc::new(CronTask::new(
        10000,
        1000,
        86400,
        1000,
        db,
    ));
    
    // 测试重新加载任务的特殊逻辑
    let result = cron_task.on_call_back_inner("__reload_tasks__".to_string()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_on_call_back_inner_invalid_key() {
    // 创建一个CronTask实例
    let db = Database::new("evdata.db").await.unwrap();
    let cron_task = Arc::new(CronTask::new(
        10000,
        1000,
        86400,
        1000,
        db,
    ));
    
    // 测试无效的任务键
    let result = cron_task.on_call_back_inner("invalid_key".to_string()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_on_call_back_inner_task_not_found() {
    // 创建一个CronTask实例
    let db = Database::new("evdata.db").await.unwrap();
    let cron_task = Arc::new(CronTask::new(
        10000,
        1000,
        86400,
        1000,
        db,
    ));
    
    // 测试不存在的任务
    let result = cron_task.on_call_back_inner("123|2023-01-01 00:00:00".to_string()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_on_call_back_inner_invalid_task_id() {
    // 创建一个CronTask实例
    let db = Database::new("evdata.db").await.unwrap();
    let cron_task = Arc::new(CronTask::new(
        10000,
        1000,
        86400,
        1000,
        db,
    ));
    
    // 测试无效的任务ID格式
    let result = cron_task.on_call_back_inner("invalid_id|2023-01-01 00:00:00".to_string()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_on_call_back_inner_invalid_time_format() {
    // 创建一个CronTask实例
    let db = Database::new("evdata.db").await.unwrap();
    let cron_task = Arc::new(CronTask::new(
        10000,
        1000,
        86400,
        1000,
        db,
    ));
    
    // 测试无效的时间点格式
    let result = cron_task.on_call_back_inner("123|invalid_time".to_string()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_on_call_back_inner_invalid_key_format() {
    // 创建一个CronTask实例
    let db = Database::new("evdata.db").await.unwrap();
    let cron_task = Arc::new(CronTask::new(
        10000,
        1000,
        86400,
        1000,
        db,
    ));
    
    // 测试无效的键格式（缺少分隔符）
    let result = cron_task.on_call_back_inner("1232023-01-01 00:00:00".to_string()).await;
    assert!(result.is_err());
    
    // 测试无效的键格式（多个分隔符）
    let result = cron_task.on_call_back_inner("123|2023-01-01|00:00:00".to_string()).await;
    assert!(result.is_err());
}
