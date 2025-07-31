use tokio::sync::broadcast;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use chrono_tz::Asia::Shanghai;

/// 时间事件类型
#[derive(Debug, Clone)]
pub enum TimeEvent {
    /// 每秒事件
    Second(DateTime<chrono_tz::Tz>),
    /// 每分钟事件
    Minute(DateTime<chrono_tz::Tz>),
    /// 每小时事件
    Hour(DateTime<chrono_tz::Tz>),
    /// 每天事件
    Day(DateTime<chrono_tz::Tz>),
    /// 自定义时间点事件
    Custom(String, DateTime<chrono_tz::Tz>),
}

/// 时间总线
pub struct TimeBus {
    /// 时间事件发送者
    sender: broadcast::Sender<TimeEvent>,
}

impl TimeBus {
    /// 创建新的时间总线
    pub fn new() -> Arc<Self> {
        let (sender, _) = broadcast::channel(1000);
        Arc::new(Self { sender })
    }

    /// 订阅时间事件
    pub fn subscribe(&self) -> broadcast::Receiver<TimeEvent> {
        self.sender.subscribe()
    }

    /// 发送时间事件
    pub fn send(&self, event: TimeEvent) -> Result<(), broadcast::error::SendError<TimeEvent>> {
        self.sender.send(event).map(|_| ())
    }
    
    /// 获取当前时间
    pub fn now() -> DateTime<chrono_tz::Tz> {
        Utc::now().with_timezone(&Shanghai)
    }
    
    /// 获取当前时间戳（毫秒）
    pub fn current_timestamp() -> u64 {
        Utc::now().timestamp_millis() as u64
    }
}

// 为TimeBus实现Clone特性
impl Clone for TimeBus {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}