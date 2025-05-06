// src/crontask.rs
use chrono::{NaiveDateTime, Utc};
use std::time::Duration;

pub struct CronTask {
    // pub name: String,
    // pub execute_at: NaiveDateTime,
    // pub interval: Option<Duration>, // 如果为 None，则表示只执行一次
    pub callback: Box<dyn Fn(String) + Send + Sync>, // 回调函数
}

impl CronTask {
    fn new(tick_duration: Duration, total_slots: usize) -> Self {
        let slots = (0..total_slots)
            .map(|_| {
                ArcSwap::new(Arc::new(TimeWheelSlot {
                    tasks: Mutex::new(HashMap::new()),
                }))
            })
            .collect();
        Self {
            slots,
            current_slot: Arc::new(AtomicUsize::new(0)),
            tick_duration,
            total_slots,
        }
    }

    // 创建一个只执行一次的任务
    pub fn new_once(name: String, execute_at: NaiveDateTime, callback: impl Fn(String) + Send + Sync + 'static) -> Self {
        Self {
            name,
            execute_at,
            interval: None,
            callback: Box::new(callback),
        }
    }

    // 创建一个周期性任务
    pub fn new_repeating(
        name: String,
        execute_at: NaiveDateTime,
        interval: Duration,
        callback: impl Fn(String) + Send + Sync + 'static,
    ) -> Self {
        Self {
            name,
            execute_at,
            interval: Some(interval),
            callback: Box::new(callback),
        }
    }

    // 执行任务
    pub fn execute(&self) {
        (self.callback)(self.name.clone());
    }

    // 判断任务是否是周期性任务
    pub fn is_repeating(&self) -> bool {
        self.interval.is_some()
    }
}