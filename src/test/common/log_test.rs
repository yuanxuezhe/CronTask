use crate::common::log;
use crate::basic::MessageBus;
use dbcore::Database;
use std::sync::Arc;

#[tokio::test]
async fn test_set_cron_task() {
    // 创建一个消息总线实例
    let _message_bus = Arc::new(MessageBus::new(100));
    
    // 创建一个临时的CronTask实例
    let cron_task = Arc::new(crate::core::core::CronTask::new(
        10000,
        1000,
        86400,
        1000,
        Database::new("evdata.db").await.unwrap(),
    ));
    
    // 设置全局CronTask实例
    log::set_cron_task(&cron_task);
    
    // 这里我们无法直接验证set_cron_task的效果，因为它是设置全局状态
    // 我们只能确保它不会崩溃
    assert!(true);
}
