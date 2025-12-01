use crate::core::core::CronTask;
use dbcore::Database;
use std::sync::Arc;

#[tokio::test]
async fn test_cron_task_creation() {
    // 创建一个CronTask实例
    let db = Database::new("evdata.db").await.unwrap();
    let cron_task = Arc::new(CronTask::new(
        10000,
        1000,
        86400,
        1000,
        db,
    ));
    
    // 验证CronTask实例的基本属性
    assert_eq!(cron_task.reload_interval, 10000);
    assert!(!cron_task.shutdown_flag.load(std::sync::atomic::Ordering::Relaxed));
}

#[tokio::test]
async fn test_init_load_tasks() {
    // 创建一个CronTask实例
    let db = Database::new("evdata.db").await.unwrap();
    let cron_task = Arc::new(CronTask::new(
        10000,
        1000,
        86400,
        1000,
        db,
    ));
    
    // 测试初始化加载任务
    cron_task.init_load_tasks().await;
    
    // 这里我们无法直接验证init_load_tasks的效果，因为它是异步加载任务
    // 我们只能确保它不会崩溃
    assert!(true);
}
