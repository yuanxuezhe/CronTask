// 标准库导入
use std::sync::Arc;

// 外部 crate 导入
use chrono::NaiveDateTime;

// 内部模块导入
use crate::core::core::CronTask;

/// 任务取消处理器
pub struct CancelHandler;

impl CancelHandler {
    /// 处理任务取消消息
    pub async fn handle_cancel_task(
        cron_task: &Arc<CronTask>, 
        timestamp: NaiveDateTime, 
        delay_ms: u64, 
        key: String
    ) {
        crate::info_log!("crontask::cancel 取消任务: {} at {} + {}ms", key, timestamp.format("%Y-%m-%d %H:%M:%S%.3f"), delay_ms);
        let result = cron_task.task_scheduler.cancel(
            timestamp,
            std::time::Duration::from_millis(delay_ms),
            key.clone(),
        ).await;
        
        if let Err(e) = result {
            crate::error_log!("任务取消失败: {} - {}", key, e);
        }
    }
}