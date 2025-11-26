// 标准库导入
use std::sync::Arc;

// 内部模块导入
use crate::message::message_bus::CronMessage;
use crate::core::core::CronTask;

/// 消息总线处理器
pub struct MessageHandler;

impl MessageHandler {
    /// 处理消息总线中的消息 - 仅负责消息路由，不包含业务逻辑
    pub async fn handle_messages(cron_task: &Arc<CronTask>) {
        let mut receiver = cron_task.message_bus.subscribe();
        
        while let Ok(message) = receiver.recv().await {
            match message {
                CronMessage::ScheduleTask { timestamp, delay_ms, key, arg } => {
                    // 路由到调度处理器
                    crate::core::handlers::schedule_handler::ScheduleHandler::handle_schedule_task(
                        cron_task, timestamp, delay_ms, key, arg
                    ).await; 
                 },
                CronMessage::CancelTask { timestamp, delay_ms, key } => {
                    // 路由到取消处理器
                    crate::core::handlers::cancel_handler::CancelHandler::handle_cancel_task(
                        cron_task, timestamp, delay_ms, key
                    ).await;
                },
                CronMessage::ReloadTasks => {
                    // 路由到重新加载任务逻辑
                    CronTask::reload_tasks(cron_task).await;
                },
                CronMessage::ExecuteTask { key, eventdata } => {
                    // 路由到任务执行逻辑
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