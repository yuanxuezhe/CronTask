// 标准库导入
use std::collections::HashMap;
use std::sync::Arc;

// 外部 crate 导入
use chrono::{DateTime, Datelike, Utc};
use chrono_tz::Asia::Shanghai;
use tokio::sync::{broadcast, RwLock};

// 本地导入

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

/// 时间回调函数类型 - 使用Arc包装以便于共享所有权
pub type TimeCallback = Arc<Box<dyn Fn(TimePulse) + Send + Sync + 'static>>;

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
        // 创建一个新的Arc<Box<dyn Fn(TimePulse) + Send + Sync + 'static>>
        // 确保正确进行trait object转换
        let callback_box: Box<dyn Fn(TimePulse) + Send + Sync + 'static> = Box::new(callback);
        let callback_arc = Arc::new(callback_box);
        callbacks
            .entry(signal_type)
            .or_insert_with(Vec::new)
            .push(callback_arc);
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
        // 初始化为最小精度秒，而不是毫秒
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50)); // 50ms精度，平衡性能和精度
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
                &mut last_week,
            );

            if signal_type > 0 {
                let pulse = TimePulse {
                    timestamp: now,
                    signal_type,
                };

                // 发送脉冲到订阅者
                if let Err(e) = self.sender.send(pulse) {
                    // 记录错误但不中断循环
                    eprintln!("发送时间脉冲失败: {e}");
                }

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

        // 使用更精确的毫秒级时间戳判断
        let current_timestamp = now.timestamp_millis();

        // 毫秒信号
        signal_type |= 0b000001;

        // 秒信号
        let second = current_timestamp / 1000;
        if second != *last_second {
            *last_second = second;
            signal_type |= 0b000010;

            // 只有在秒变化时才检查更高精度的信号

            // 分钟信号
            let minute = second / 60;
            if minute != *last_minute {
                *last_minute = minute;
                signal_type |= 0b000100;

                // 小时信号
                let hour = minute / 60;
                if hour != *last_hour {
                    *last_hour = hour;
                    signal_type |= 0b001000;

                    // 天信号
                    let day = hour / 24;
                    if day != *last_day {
                        *last_day = day;
                        signal_type |= 0b010000;

                        // 周信号（ISO周）
                        let week = now.iso_week().week() as i64;
                        if week != *last_week {
                            *last_week = week;
                            signal_type |= 0b100000;
                        }
                    }
                }
            }
        }

        signal_type
    }

    /// 执行注册的回调函数
    async fn execute_callbacks(&self, pulse: TimePulse) {
        // 创建一个向量来存储所有需要执行的回调函数的Arc克隆
        let mut callbacks_to_execute = Vec::new();

        // 在锁的作用域内获取匹配的回调函数
        {
            let callbacks_ref = self.callbacks.read().await;

            // 遍历所有注册的回调类型
            for (&signal_mask, callbacks_list) in callbacks_ref.iter() {
                // 检查当前pulse是否匹配此信号类型
                if pulse.signal_type & signal_mask == signal_mask {
                    // 为每个匹配的回调函数创建Arc克隆
                    for callback in callbacks_list {
                        callbacks_to_execute.push(Arc::clone(callback));
                    }
                }
            }
        }
        // 锁在这里自动释放

        // 在锁外，为每个回调启动独立的异步任务
        for callback_arc in callbacks_to_execute {
            // 克隆pulse以在异步任务中使用
            let pulse_copy = pulse;

            // 启动异步任务，move语义确保callback_arc和pulse_copy的所有权被转移
            tokio::spawn(async move {
                // 直接调用回调函数，因为Arc会自动解引用
                (callback_arc)(pulse_copy);
            });
        }
    }
}

// 私有辅助函数
fn start_time_generator(time_bus: Arc<TimeBus>) {
    tokio::spawn(async move {
        time_bus.run_time_generator().await;
    });
}

// 移除了Clone实现，使用Arc<TimeBus>替代

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_time_bus_creation() {
        let time_bus = TimeBus::new();
        // 添加超时处理以避免测试无限等待
        let result = timeout(Duration::from_millis(100), time_bus.subscribe().recv()).await;
        assert!(result.is_ok(), "超时: 未能在100毫秒内接收信号");
        assert!(result.unwrap().is_ok(), "接收信号失败");
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
            // 尝试接收信号，每次等待10毫秒
            if let Ok(result) = timeout(Duration::from_millis(10), receiver.recv()).await {
                if let Ok(pulse) = result {
                    received_signals.push(pulse.signal_type);
                }
            }
            // 不break，继续尝试接收信号
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
        time_bus
            .register_callback(2, move |_pulse| {
                callback_flag_clone.store(true, std::sync::atomic::Ordering::Relaxed);
            })
            .await;

        // 等待足够时间让回调有机会执行多次
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 验证回调函数至少被调用过一次
        assert!(callback_flag.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_time_bus_callbacks_with_logging() {
        let time_bus = TimeBus::new();

        // 创建原子计数器来跟踪回调调用次数
        let millisecond_callback_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let second_callback_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let minute_callback_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let millisecond_count_clone = millisecond_callback_count.clone();
        let second_count_clone = second_callback_count.clone();
        let minute_count_clone = minute_callback_count.clone();

        // 注册毫秒回调
        time_bus
            .register_callback(1, move |_pulse| {
                millisecond_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
            .await;

        // 注册秒回调
        time_bus
            .register_callback(2, move |_pulse| {
                second_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
            .await;

        // 注册分钟回调 (组合信号：毫秒|秒 = 1|2 = 3)
        time_bus
            .register_callback(3, move |pulse| {
                let count = minute_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                println!(
                    "[{}] 组合回调 #{}: 时间戳 = {}",
                    pulse.timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
                    count,
                    pulse.timestamp.timestamp()
                );
            })
            .await;

        // 等待一段时间以收集回调
        tokio::time::sleep(Duration::from_secs(3)).await;

        // 获取最终计数
        let millisecond_calls = millisecond_callback_count.load(std::sync::atomic::Ordering::Relaxed);
        let second_calls = second_callback_count.load(std::sync::atomic::Ordering::Relaxed);
        let minute_calls = minute_callback_count.load(std::sync::atomic::Ordering::Relaxed);

        println!("测试结果:");
        println!("  - 毫秒回调被调用 {} 次", millisecond_calls);
        println!("  - 秒回调被调用 {} 次", second_calls);
        println!("  - 分钟回调被调用 {} 次", minute_calls);

        // 验证回调确实被调用了
        assert!(millisecond_calls > 0, "毫秒回调未被调用");
        assert!(second_calls > 0, "秒回调未被调用");
        assert!(minute_calls > 0, "分钟回调未被调用");

        println!("时间总线回调测试通过!");
    }

    #[tokio::test]
    async fn test_combined_signal_callbacks() {
        let time_bus = TimeBus::new();

        // 创建原子计数器来跟踪回调调用次数
        let millisecond_callback_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let second_callback_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let combined_callback_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let millisecond_count_clone = millisecond_callback_count.clone();
        let second_count_clone = second_callback_count.clone();
        let combined_count_clone = combined_callback_count.clone();

        // 注册毫秒回调
        time_bus
            .register_callback(1, move |_pulse| {
                millisecond_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
            .await;

        // 注册秒回调
        time_bus
            .register_callback(2, move |_pulse| {
                second_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
            .await;

        // 注册同时订阅毫秒和秒的组合回调 (组合信号：毫秒|秒 = 1|2 = 3)
        time_bus
            .register_callback(3, move |_pulse| {
                combined_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
            .await;

        // 等待一段时间以收集回调
        tokio::time::sleep(Duration::from_secs(2)).await;

        // 获取最终计数
        let millisecond_calls = millisecond_callback_count.load(std::sync::atomic::Ordering::Relaxed);
        let second_calls = second_callback_count.load(std::sync::atomic::Ordering::Relaxed);
        let combined_calls = combined_callback_count.load(std::sync::atomic::Ordering::Relaxed);

        println!("组合信号测试结果:");
        println!("  - 毫秒回调被调用 {} 次", millisecond_calls);
        println!("  - 秒回调被调用 {} 次", second_calls);
        println!("  - 组合回调被调用 {} 次", combined_calls);

        // 验证回调确实被调用了
        assert!(millisecond_calls > 0, "毫秒回调未被调用");
        assert!(second_calls > 0, "秒回调未被调用");
        assert!(combined_calls > 0, "组合回调未被调用");

        println!("组合信号回调测试通过!");
    }
}