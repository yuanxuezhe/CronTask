use tokio::sync::broadcast;
use chrono::NaiveDateTime;
use std::sync::Arc;

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
        /// 任务参数
        arg: String,
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
        /// 任务相关数据
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
    
    /// 获取发送者（用于测试）
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_message_bus_creation() {
        let message_bus = MessageBus::new();
        assert!(message_bus.subscribe().recv().await.is_ok());
    }

    #[tokio::test]
    async fn test_send_and_receive_message() {
        let message_bus = MessageBus::new();
        let mut receiver = message_bus.subscribe();
        
        // 发送一条测试消息
        let test_message = CronMessage::ReloadTasks;
        message_bus.send(test_message.clone()).unwrap();
        
        // 接收消息并验证
        let received_message = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("接收消息超时")
            .expect("接收消息失败");
            
        assert_eq!(std::mem::discriminant(&test_message), std::mem::discriminant(&received_message));
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let message_bus = MessageBus::new();
        let mut receiver1 = message_bus.subscribe();
        let mut receiver2 = message_bus.subscribe();
        
        // 发送一条测试消息
        let test_message = CronMessage::ReloadTasks;
        message_bus.send(test_message.clone()).unwrap();
        
        // 两个订阅者都应该能接收到消息
        let msg1 = timeout(Duration::from_millis(100), receiver1.recv())
            .await
            .expect("接收器1超时")
            .expect("接收器1接收失败");
            
        let msg2 = timeout(Duration::from_millis(100), receiver2.recv())
            .await
            .expect("接收器2超时")
            .expect("接收器2接收失败");
            
        // 验证两个接收者都收到了相同类型的消息
        assert_eq!(std::mem::discriminant(&test_message), std::mem::discriminant(&msg1));
        assert_eq!(std::mem::discriminant(&test_message), std::mem::discriminant(&msg2));
    }

    #[tokio::test]
    async fn test_schedule_task_message() {
        let message_bus = MessageBus::new();
        let mut receiver = message_bus.subscribe();
        
        // 发送调度任务消息
        let schedule_message = CronMessage::ScheduleTask {
            timestamp: chrono::Local::now().naive_local(),
            delay_ms: 1000,
            key: "test_key".to_string(),
            arg: "test_arg".to_string(),
        };
        
        message_bus.send(schedule_message.clone()).unwrap();
        
        // 接收并验证消息
        let received_message = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("接收调度任务消息超时")
            .expect("接收调度任务消息失败");
            
        match (schedule_message, received_message) {
            (CronMessage::ScheduleTask { timestamp: ts1, delay_ms: dm1, key: k1, arg: a1 }, 
             CronMessage::ScheduleTask { timestamp: ts2, delay_ms: dm2, key: k2, arg: a2 }) => {
                assert_eq!(ts1, ts2);
                assert_eq!(dm1, dm2);
                assert_eq!(k1, k2);
                assert_eq!(a1, a2);
            },
            _ => panic!("消息类型不匹配"),
        }
    }

    #[tokio::test]
    async fn test_cancel_task_message() {
        let message_bus = MessageBus::new();
        let mut receiver = message_bus.subscribe();
        
        // 发送取消任务消息
        let cancel_message = CronMessage::CancelTask {
            timestamp: chrono::Local::now().naive_local(),
            delay_ms: 1000,
            key: "test_key".to_string(),
        };
        
        message_bus.send(cancel_message.clone()).unwrap();
        
        // 接收并验证消息
        let received_message = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("接收取消任务消息超时")
            .expect("接收取消任务消息失败");
            
        match (cancel_message, received_message) {
            (CronMessage::CancelTask { timestamp: ts1, delay_ms: dm1, key: k1 }, 
             CronMessage::CancelTask { timestamp: ts2, delay_ms: dm2, key: k2 }) => {
                assert_eq!(ts1, ts2);
                assert_eq!(dm1, dm2);
                assert_eq!(k1, k2);
            },
            _ => panic!("消息类型不匹配"),
        }
    }

    #[tokio::test]
    async fn test_execute_task_message() {
        let message_bus = MessageBus::new();
        let mut receiver = message_bus.subscribe();
        
        // 发送执行任务消息
        let execute_message = CronMessage::ExecuteTask {
            key: "test_key".to_string(),
            eventdata: "test_data".to_string(),
        };
        
        message_bus.send(execute_message.clone()).unwrap();
        
        // 接收并验证消息
        let received_message = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("接收执行任务消息超时")
            .expect("接收执行任务消息失败");
            
        match (execute_message, received_message) {
            (CronMessage::ExecuteTask { key: k1, eventdata: e1 }, 
             CronMessage::ExecuteTask { key: k2, eventdata: e2 }) => {
                assert_eq!(k1, k2);
                assert_eq!(e1, e2);
            },
            _ => panic!("消息类型不匹配"),
        }
    }
}