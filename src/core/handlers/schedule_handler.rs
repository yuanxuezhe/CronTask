// 标准库导入
use std::sync::Arc;

// 外部 crate 导入
use chrono::NaiveDateTime;

// 内部模块导入
use crate::core::core::CronTask;
use crate::common::utils::gen_task_key;
use crate::message::message_bus::CronMessage;

/// 任务调度处理器
pub struct ScheduleHandler;

impl ScheduleHandler {
    /// 处理任务调度消息
    pub async fn handle_schedule_task(
        cron_task: &Arc<CronTask>, 
        timestamp: NaiveDateTime, 
        delay_ms: u64, 
        key: String, 
        arg: String
    ) {
        crate::info_log!("task[{}] add at {} + {}ms", key, timestamp.format("%Y-%m-%d %H:%M:%S%.3f"), delay_ms);
        let result = cron_task.taskscheduler.schedule(
            timestamp,
            std::time::Duration::from_millis(delay_ms),
            key.clone(),
            arg,
            {
                let message_bus = cron_task.message_bus.clone();
                move |key, eventdata| {
                    let _ = message_bus.send(CronMessage::ExecuteTask { key, eventdata });
                }
            },
        ).await;
        
        if let Err(e) = result {
            crate::error_log!("任务调度失败: {} - {}", key, e);
            // 当任务调度失败时，更新任务状态为未监控，以便重新加载时可以重新尝试调度
            // 需要修改状态，使用write()
            let mut guard = cron_task.inner.write().await;
            for (_, detail) in guard.taskdetails.iter_mut() {
                if gen_task_key(detail.taskid, &detail.timepoint) == key {
                    detail.status = crate::common::consts::TASK_STATUS_UNMONITORED;
                    break;
                }
            }
        }
    }
}