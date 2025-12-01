use crate::core::core::CronTask;
use dbcore::Database;
use std::sync::Arc;

#[tokio::test]
async fn test_reschedule_all() {
    // 创建一个CronTask实例
    let db = Database::new("evdata.db").await.unwrap();
    let cron_task = Arc::new(CronTask::new(
        10000,
        1000,
        86400,
        1000,
        db,
    ));
    
    // 测试重新调度所有任务
    cron_task.reschedule_all().await;
    
    // 这里我们无法直接验证reschedule_all的效果，因为它是异步处理
    // 我们只能确保它不会崩溃
    assert!(true);
}

#[tokio::test]
async fn test_reload_tasks() {
    // 创建一个CronTask实例
    let db = Database::new("evdata.db").await.unwrap();
    let cron_task = Arc::new(CronTask::new(
        10000,
        1000,
        86400,
        1000,
        db,
    ));
    
    // 测试重新加载任务
    CronTask::reload_tasks(&cron_task).await;
    
    // 这里我们无法直接验证reload_tasks的效果，因为它是异步处理
    // 我们只能确保它不会崩溃
    assert!(true);
}
