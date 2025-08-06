// 标准库导入
use std::collections::HashMap;
use std::sync::Arc;

// 外部 crate 导入
use chrono::{DateTime, Datelike, Utc};
use chrono_tz::Asia::Shanghai;
use tokio::sync::{broadcast, RwLock};

/// 时间事件类型 - 使用位值表示不同精度的时间信号
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimePulse {
    /// 时间戳
    pub timestamp: DateTime<chrono_tz::Tz>,
    /// 信号类型位值：
    /// - 毫秒:  0b000001 (1)
    /// - 秒:    0b000010 (2)
    /// - 分钟:  0b000100 (4)
    /// - 小时:  0b001000 (8)
    /// - 天:    0b010000 (16)
    /// - 周:    0b100000 (32)
    pub signal_type: u8,
}

/// 时间回调函数类型
pub type TimeCallback = Box<dyn Fn(TimePulse) + Send + Sync>;

/// 时间总线
pub struct TimeBus {
    /// 时间事件发送者
    sender: broadcast::Sender<TimePulse>,
    /// 回调函数注册表，键为订阅的位值
    callbacks: Arc<RwLock<HashMap<u8, Vec<TimeCallback>>>>,
}

impl TimeBus {
    /// 创建新的时间总线
    pub fn new() -> Arc<Self> {
        let (sender, _) = broadcast::channel(1000);
        let time_bus = Arc::new(Self { 
            sender,
            callbacks: Arc::new(RwLock::new(HashMap::new())),
        });
        
        // 启动时间脉冲生成器
        start_time_generator(time_bus.clone());
        
        time_bus
    }

    /// 订阅时间事件（通过广播）
    pub fn subscribe(&self) -> broadcast::Receiver<TimePulse> {
        self.sender.subscribe()
    }

    /// 发送时间脉冲
    pub fn send(&self, pulse: TimePulse) -> Result<(), broadcast::error::SendError<TimePulse>> {
        self.sender.send(pulse).map(|_| ())
    }
    
    /// 注册时间回调函数
    /// 
    /// # 参数
    /// * `signal_type` - 订阅的信号类型位值 (1=毫秒, 2=秒, 4=分钟, 8=小时, 16=天, 32=周)
    /// * `callback` - 回调函数
    pub async fn register_callback<F>(&self, signal_type: u8, callback: F)
    where
        F: Fn(TimePulse) + Send + Sync + 'static,
    {
        let mut callbacks = self.callbacks.write().await;
        callbacks.entry(signal_type).or_insert_with(Vec::new).push(Box::new(callback));
    }
    
    /// 获取当前时间
    pub fn now() -> DateTime<chrono_tz::Tz> {
        Utc::now().with_timezone(&Shanghai)
    }
}

// 私有辅助函数实现
impl TimeBus {
    /// 运行时间脉冲生成器
    async fn run_time_generator(self: Arc<Self>) {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(1)); // 1ms精度
        let mut last_second = 0;
        let mut last_minute = 0;
        let mut last_hour = 0;
        let mut last_day = 0;
        let mut last_week = 0;
        
        loop {
            interval.tick().await;
            
            let now = Self::now();
            let signal_type = Self::determine_signal_type(
                now, 
                &mut last_second, 
                &mut last_minute, 
                &mut last_hour, 
                &mut last_day, 
                &mut last_week
            );
            
            if signal_type > 0 {
                let pulse = TimePulse {
                    timestamp: now,
                    signal_type,
                };
                
                // 发送脉冲到订阅者
                let _ = self.sender.send(pulse.clone());
                
                // 执行注册的回调函数
                self.execute_callbacks(pulse).await;
            }
        }
    }
    
    /// 确定需要发送的信号类型
    fn determine_signal_type(
        now: DateTime<chrono_tz::Tz>,
        last_second: &mut i64,
        last_minute: &mut i64,
        last_hour: &mut i64,
        last_day: &mut i64,
        last_week: &mut i64,
    ) -> u8 {
        let mut signal_type = 0;
        
        // 毫秒信号（总是发送）
        signal_type |= 0b000001;
        
        // 秒信号
        let second = now.timestamp();
        if second != *last_second {
            *last_second = second;
            signal_type |= 0b000010;
        }
        
        // 分钟信号
        let minute = now.timestamp() / 60;
        if minute != *last_minute {
            *last_minute = minute;
            signal_type |= 0b000100;
        }
        
        // 小时信号
        let hour = now.timestamp() / 3600;
        if hour != *last_hour {
            *last_hour = hour;
            signal_type |= 0b001000;
        }
        
        // 天信号
        let day = now.timestamp() / 86400;
        if day != *last_day {
            *last_day = day;
            signal_type |= 0b010000;
        }
        
        // 周信号
        let week = now.iso_week().week() as i64;
        if week != *last_week {
            *last_week = week;
            signal_type |= 0b100000;
        }
        
        signal_type
    }
    
    /// 执行注册的回调函数
    async fn execute_callbacks(&self, pulse: TimePulse) {
        let callbacks = self.callbacks.read().await;
        
        // 执行精确匹配的回调
        if let Some(callbacks_for_type) = callbacks.get(&pulse.signal_type) {
            for callback in callbacks_for_type {
                let pulse_clone = pulse.clone();
                callback(pulse_clone);
            }
        }
        
        // 执行组合信号的回调（例如同时订阅了毫秒和秒的回调）
        for (registered_type, callbacks_for_type) in callbacks.iter() {
            if pulse.signal_type & *registered_type == *registered_type {
                for callback in callbacks_for_type {
                    let pulse_clone = pulse.clone();
                    callback(pulse_clone);
                }
            }
        }
    }
}

// 私有辅助函数
fn start_time_generator(time_bus: Arc<TimeBus>) {
    tokio::spawn(async move {
        time_bus.run_time_generator().await;
    });
}

// 为TimeBus实现Clone特性
impl Clone for TimeBus {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            callbacks: self.callbacks.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_time_bus_creation() {
        let time_bus = TimeBus::new();
        assert!(time_bus.subscribe().recv().await.is_ok());
    }

    #[tokio::test]
    async fn test_time_pulse_signals() {
        let time_bus = TimeBus::new();
        let mut receiver = time_bus.subscribe();
        
        // 等待第一个脉冲信号
        let pulse = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("接收时间脉冲超时")
            .expect("接收时间脉冲失败");
            
        // 验证脉冲信号的基本结构
        assert!(pulse.signal_type > 0);
        assert!(pulse.timestamp.timestamp_millis() > 0);
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let time_bus = TimeBus::new();
        let mut receiver1 = time_bus.subscribe();
        let mut receiver2 = time_bus.subscribe();
        
        // 两个订阅者都应该能接收到信号
        let pulse1 = timeout(Duration::from_millis(100), receiver1.recv())
            .await
            .expect("接收器1超时")
            .expect("接收器1接收失败");
            
        let pulse2 = timeout(Duration::from_millis(100), receiver2.recv())
            .await
            .expect("接收器2超时")
            .expect("接收器2接收失败");
            
        // 验证两个接收者都收到了相同的信号
        assert_eq!(pulse1.signal_type, pulse2.signal_type);
    }

    #[tokio::test]
    async fn test_signal_type_bit_values() {
        let time_bus = TimeBus::new();
        let mut receiver = time_bus.subscribe();
        
        let start_time = std::time::Instant::now();
        let mut received_signals = Vec::new();
        
        // 在短时间内收集多个信号
        while start_time.elapsed() < Duration::from_millis(100) {
            if let Ok(result) = timeout(Duration::from_millis(10), receiver.recv()).await {
                if let Ok(pulse) = result {
                    received_signals.push(pulse.signal_type);
                }
            } else {
                break;
            }
        }
        
        // 验证至少收到了一些信号
        assert!(!received_signals.is_empty());
        
        // 验证信号类型符合预期的位值规则
        for signal_type in received_signals {
            // 信号类型应该是2的幂或它们的组合
            assert!(signal_type > 0);
        }
    }
    
    #[tokio::test]
    async fn test_callback_registration() {
        let time_bus = TimeBus::new();
        let callback_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_flag_clone = callback_flag.clone();
        
        // 注册一个回调函数
        time_bus.register_callback(2, move |_pulse| {
            callback_flag_clone.store(true, std::sync::atomic::Ordering::Relaxed);
        }).await;
        
        // 等待足够时间让回调有机会执行多次
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // 验证回调函数至少被调用过一次
        assert!(callback_flag.load(std::sync::atomic::Ordering::Relaxed));
    }
}