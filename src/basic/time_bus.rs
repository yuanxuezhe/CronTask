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




