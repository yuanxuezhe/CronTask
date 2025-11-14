// 标准库导入
use std::sync::Arc;

// 内部模块导入
use crate::message::message_bus::CronMessage;
use crate::core::core::CronTask;

/// 消息总线处理器
pub struct MessageHandler;

impl MessageHandler {
    /// 处理消息总线中的消息
    pub async fn handle_messages(cron_task: &Arc<CronTask>) {
        let mut receiver = cron_task.message_bus.subscribe();
        
        while let Ok(message) = receiver.recv().await {
            match message {
                CronMessage::ScheduleTask { timestamp, delay_ms, key, arg } => {
                    crate::core::handlers::schedule_handler::ScheduleHandler::handle_schedule_task(
                        cron_task, timestamp, delay_ms, key, arg
                    ).await;
                },
                CronMessage::CancelTask { timestamp, delay_ms, key } => {
                    crate::core::handlers::cancel_handler::CancelHandler::handle_cancel_task(
                        cron_task, timestamp, delay_ms, key
                    ).await;
                },
                CronMessage::ReloadTasks => {
                    CronTask::reload_tasks(cron_task).await;
                },
                CronMessage::ExecuteTask { key, eventdata } => {
                    let _ = cron_task.on_call_back_inner(key, eventdata).await;
                },
                CronMessage::Log { level, message } => {
                    // 异步处理日志消息
                    match level {
                        ::log::Level::Error => ::log::error!("{}", message),
                        ::log::Level::Warn => ::log::warn!("{}", message),
                        ::log::Level::Info => ::log::info!("{}", message),
                        ::log::Level::Debug => ::log::debug!("{}", message),
                        ::log::Level::Trace => ::log::trace!("{}", message),
                    }
                }
            }
        }
    }
}