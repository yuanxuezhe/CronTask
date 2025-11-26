// 标准库导入
use std::sync::Arc;

// 外部 crate 导入
use chrono::NaiveDateTime;

// 内部模块导入
use crate::common::consts::TASK_STATUS_UNMONITORED;
use crate::common::utils::gen_task_key;
use crate::core::core::CronTask;
use crate::message::message_bus::CronMessage;

/// 消息总线处理器
pub struct MessageHandler;

impl MessageHandler {
    /// 处理消息总线中的消息 - 仅负责消息路由和处理，不包含业务逻辑
    pub async fn handle_messages(cron_task: &Arc<CronTask>) {
        let mut receiver = cron_task.message_bus.subscribe();

        while let Ok(message) = receiver.recv().await {
            match message {
                CronMessage::ScheduleTask {
                    timestamp,
                    delay_ms,
                    key,
                    arg,
                } => {
                    // 处理任务调度消息
                    Self::handle_schedule_task(cron_task, timestamp, delay_ms, key, arg).await;
                }
                CronMessage::CancelTask {
                    timestamp,
                    delay_ms,
                    key,
                } => {
                    // 处理任务取消消息
                    Self::handle_cancel_task(cron_task, timestamp, delay_ms, key).await;
                }
                CronMessage::ReloadTasks => {
                    // 处理重新加载任务逻辑
                    CronTask::reload_tasks(cron_task).await;
                }
                CronMessage::ExecuteTask { key, eventdata } => {
                    // 处理任务执行逻辑
                    let _ = cron_task.on_call_back_inner(key, eventdata).await;
                }
                CronMessage::Log { level, message } => {
                    // 异步处理日志消息
                    match level {
                        ::log::Level::Error => ::log::error!("{message}"),
                        ::log::Level::Warn => ::log::warn!("{message}"),
                        ::log::Level::Info => ::log::info!("{message}"),
                        ::log::Level::Debug => ::log::debug!("{message}"),
                        ::log::Level::Trace => ::log::trace!("{message}"),
                    }
                }
            }
        }
    }

    /// 处理任务调度消息 - 仅负责事件扭转，不包含业务逻辑
    pub async fn handle_schedule_task(
        cron_task: &Arc<CronTask>,
        timestamp: NaiveDateTime,
        delay_ms: u64,
        key: String,
        arg: String,
    ) {
        crate::info_log!(
            "task[{}] add at {} + {}ms",
            key,
            timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
            delay_ms
        );
        let result = cron_task
            .task_scheduler
            .schedule(
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
            )
            .await;

        if let Err(e) = result {
            crate::error_log!("任务调度失败: {} - {}", key, e);
            // 当任务调度失败时，更新任务状态为未监控，以便重新加载时可以重新尝试调度
            // 需要修改状态，使用write()
            let mut guard = cron_task.inner.write().await;
            for (_, detail) in guard.taskdetails.iter_mut() {
                if gen_task_key(detail.taskid, &detail.timepoint) == key {
                    detail.status = TASK_STATUS_UNMONITORED;
                    break;
                }
            }
        }
    }

    /// 处理任务取消消息
    pub async fn handle_cancel_task(
        cron_task: &Arc<CronTask>,
        timestamp: NaiveDateTime,
        delay_ms: u64,
        key: String,
    ) {
        crate::info_log!(
            "crontask::cancel 取消任务: {} at {} + {}ms",
            key,
            timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
            delay_ms
        );
        let result = cron_task
            .task_scheduler
            .cancel(
                timestamp,
                std::time::Duration::from_millis(delay_ms),
                key.clone(),
            )
            .await;

        if let Err(e) = result {
            crate::error_log!("任务取消失败: {} - {}", key, e);
        }
    }
}