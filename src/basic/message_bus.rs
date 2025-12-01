// 外部 crate 导入
use std::sync::Arc;

use chrono::NaiveDateTime;
use tokio::sync::broadcast;

/// Cron任务消息类型
#[derive(Debug, Clone)]
pub enum CronMessage {
    /// 调度任务消息
    ScheduleTask {
        /// 任务触发时间
        timestamp: NaiveDateTime,
        /// 延迟毫秒数
        delay_ms: u64,
        /// 任务唯一标识符
        key: String,
    },
    /// 取消任务消息
    CancelTask {
        /// 任务原定触发时间
        timestamp: NaiveDateTime,
        /// 原定延迟毫秒数
        delay_ms: u64,
        /// 任务唯一标识符
        key: String,
    },
    /// 重新加载任务消息
    ReloadTasks,
    /// 执行任务消息
    ExecuteTask {
        /// 任务唯一标识符
        key: String,
    },
    /// 日志消息
    Log {
        /// 日志级别
        level: log::Level,
        /// 日志内容
        message: String,
    },
}

/// 消息总线
pub struct MessageBus {
    /// 消息发送者
    sender: broadcast::Sender<CronMessage>,
}

impl MessageBus {
    /// 创建新的消息总线
    pub fn new(channel_buffer_size: usize) -> Arc<Self> {
        let (sender, _) = broadcast::channel(channel_buffer_size);
        Arc::new(Self { sender })
    }

    /// 订阅消息
    pub fn subscribe(&self) -> broadcast::Receiver<CronMessage> {
        self.sender.subscribe()
    }

    /// 发送消息
    pub fn send(
        &self,
        message: CronMessage,
    ) -> Result<(), broadcast::error::SendError<CronMessage>> {
        self.sender.send(message).map(|_| ())
    }
    
    /// 发送重新加载任务消息
    pub fn send_reload_tasks(&self) -> Result<(), broadcast::error::SendError<CronMessage>> {
        self.send(CronMessage::ReloadTasks)
    }
    
    /// 发送调度任务消息
    pub fn send_schedule_task(
        &self,
        timestamp: NaiveDateTime,
        delay_ms: u64,
        key: String,
    ) -> Result<(), broadcast::error::SendError<CronMessage>> {
        self.send(CronMessage::ScheduleTask { timestamp, delay_ms, key })
    }
    
    /// 发送取消任务消息
    pub fn send_cancel_task(
        &self,
        timestamp: NaiveDateTime,
        delay_ms: u64,
        key: String,
    ) -> Result<(), broadcast::error::SendError<CronMessage>> {
        self.send(CronMessage::CancelTask { timestamp, delay_ms, key })
    }
    
    /// 发送执行任务消息
    pub fn send_execute_task(
        &self,
        key: String,
    ) -> Result<(), broadcast::error::SendError<CronMessage>> {
        self.send(CronMessage::ExecuteTask { key })
    }
    
    /// 发送日志消息
    pub fn send_log(
        &self,
        level: log::Level,
        message: String,
    ) -> Result<(), broadcast::error::SendError<CronMessage>> {
        self.send(CronMessage::Log { level, message })
    }
}

// 为MessageBus实现Clone特性
impl Clone for MessageBus {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}




