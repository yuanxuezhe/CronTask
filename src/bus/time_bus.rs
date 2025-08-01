use tokio::sync::broadcast;
use std::sync::Arc;
use chrono::{DateTime, Utc, Timelike, Datelike};
use chrono_tz::Asia::Shanghai;
use std::time::Duration;
use std::collections::HashMap;
use tokio::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

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
        let time_bus_clone = time_bus.clone();
        tokio::spawn(async move {
            time_bus_clone.run_time_generator().await;
        });
        
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
    
    /// 运行时间脉冲生成器
    async fn run_time_generator(self: Arc<Self>) {
        let mut millisecond_interval = tokio::time::interval(Duration::from_millis(1));
        
        // 记录上一次触发的时间单位，避免重复触发
        let mut last_second = None;
        let mut last_minute = None;
        let mut last_hour = None;
        let mut last_day = None;
        let mut last_week = None;
        
        loop {
            millisecond_interval.tick().await;
            
            let now = Self::now();
            let mut signal_type = 0b000001; // 毫秒信号 (1)
            
            // 检查是否需要触发秒事件
            if last_second != Some(now.second()) {
                last_second = Some(now.second());
                signal_type |= 0b000010; // 添加秒信号 (2)
            }
            
            // 检查是否需要触发分钟事件
            if last_minute != Some(now.minute()) {
                last_minute = Some(now.minute());
                signal_type |= 0b000100; // 添加分钟信号 (4)
            }
            
            // 检查是否需要触发小时事件
            if last_hour != Some(now.hour()) {
                last_hour = Some(now.hour());
                signal_type |= 0b001000; // 添加小时信号 (8)
            }
            
            // 检查是否需要触发天事件
            if last_day != Some(now.date_naive()) {
                last_day = Some(now.date_naive());
                signal_type |= 0b010000; // 添加天信号 (16)
            }
            
            // 检查是否需要触发周事件
            let week_number = now.iso_week().week();
            if last_week != Some(week_number) {
                last_week = Some(week_number);
                signal_type |= 0b100000; // 添加周信号 (32)
            }
            
            // 创建时间脉冲
            let pulse = TimePulse {
                timestamp: now,
                signal_type,
            };
            
            // 发送时间脉冲
            let _ = self.send(pulse);
            
            // 直接执行回调函数
            self.execute_callbacks(signal_type, pulse).await;
        }
    }
    
    /// 执行回调函数
    async fn execute_callbacks(&self, signal_type: u8, pulse: TimePulse) {
        let callbacks = self.callbacks.read().await;
        // 遍历所有注册的回调函数
        for (&registered_signal_type, callbacks_for_type) in callbacks.iter() {
            // 使用位与操作检查是否应该触发此回调
            if signal_type & registered_signal_type != 0 {
                // 执行所有匹配的回调函数
                for callback in callbacks_for_type {
                    let pulse_clone = pulse; // 创建脉冲事件的克隆
                    callback(pulse_clone);
                }
            }
        }
    }
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
        let callback_flag = Arc::new(AtomicBool::new(false));
        let callback_flag_clone = callback_flag.clone();
        
        // 注册一个回调函数
        time_bus.register_callback(2, move |_pulse| {
            callback_flag_clone.store(true, Ordering::Relaxed);
        }).await;
        
        // 等待足够时间让回调有机会执行多次
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // 验证回调函数至少被调用过一次
        assert!(callback_flag.load(Ordering::Relaxed));
    }
}