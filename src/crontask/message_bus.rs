use tokio::sync::broadcast;
use std::sync::Arc;
use chrono::{NaiveDateTime};

/// 消息类型枚举
#[derive(Debug, Clone)]
pub enum CronMessage {
    /// 任务调度消息
    ScheduleTask {
        timestamp: NaiveDateTime,
        delay_ms: u64,
        key: String,
        arg: String,
    },
    /// 任务取消消息
    CancelTask {
        timestamp: NaiveDateTime,
        delay_ms: u64,
        key: String,
    },
    /// 重新加载任务消息
    ReloadTasks,
    /// 任务执行消息
    ExecuteTask {
        key: String,
        eventdata: String,
    },
}

/// 消息总线
pub struct MessageBus {
    /// 消息发送者
    sender: broadcast::Sender<CronMessage>,
}

impl MessageBus {
    /// 创建新的消息总线
    pub fn new() -> Arc<Self> {
        let (sender, _) = broadcast::channel(1000);
        Arc::new(Self { sender })
    }

    /// 订阅消息
    pub fn subscribe(&self) -> broadcast::Receiver<CronMessage> {
        self.sender.subscribe()
    }

    /// 发送消息
    pub fn send(&self, message: CronMessage) -> Result<(), broadcast::error::SendError<CronMessage>> {
        self.sender.send(message).map(|_| ())
    }
    
    /// 获取发送者克隆（用于传递给其他模块）
    pub fn sender(&self) -> broadcast::Sender<CronMessage> {
        self.sender.clone()
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