// 标准库导入
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::collections::HashSet;

// 外部 crate 导入
use arc_swap::ArcSwap;
use chrono::{NaiveDateTime, TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;
use std::time::Duration;
use tokio::sync::Mutex;

// 内部模块导入
use crate::common::error::CronTaskError;
use super::message_bus::{CronMessage, MessageBus};

/// 时间轮槽位
pub struct TimeWheelSlot {
    /// 存储任务的HashSet，值为任务key
    pub tasks: Mutex<HashSet<String>>,
}

/// 时间轮
pub struct TimeWheel {
    /// 时间轮槽位数组
    pub slots: Vec<ArcSwap<TimeWheelSlot>>,
    /// 当前槽位索引
    pub current_slot: Arc<AtomicUsize>,
    /// 滴答间隔
    pub tick_duration: Duration,
    /// 总槽数
    pub total_slots: usize,
    /// 基准时间
    pub base_time: NaiveDateTime,
    /// 消息总线，用于发送任务执行事件
    pub message_bus: Arc<MessageBus>,
}

impl TimeWheel {
    /// 创建新的时间轮
    ///
    /// # 参数
    /// * `tick_duration` - 滴答间隔
    /// * `total_slots` - 总槽数
    /// * `message_bus` - 消息总线，用于发送任务执行事件
    ///
    /// # 返回值
    /// 返回新的时间轮实例
    pub fn new(tick_duration: Duration, total_slots: usize, message_bus: Arc<MessageBus>) -> Self {
        let slots = (0..total_slots)
            .map(|_| {
                ArcSwap::new(Arc::new(TimeWheelSlot {
                    tasks: Mutex::new(HashSet::new()),
                }))
            })
            .collect();

        Self {
            slots,
            current_slot: Arc::new(AtomicUsize::new(0)),
            tick_duration,
            total_slots,
            base_time: Self::get_current_time(),
            message_bus,
        }
    }

    /// 获取时间对应的绝对槽位
    ///
    /// # 参数
    /// * `timestamp` - 时间戳
    ///
    /// # 返回值
    /// 返回时间对应的绝对槽位索引
    pub fn get_real_slot(&self, timestamp: NaiveDateTime) -> Result<usize, CronTaskError> {
        if timestamp < self.base_time {
            return Err(CronTaskError::TaskPastDue);
        }

        // 直接使用 Duration 计算，避免 NaiveDateTime 减法和纳秒转换
        let base_dt = self.base_time.and_local_timezone(Shanghai).unwrap();
        let target_dt = timestamp.and_local_timezone(Shanghai).unwrap();
        let duration = target_dt.signed_duration_since(base_dt);
        let nanos = match duration.num_nanoseconds() {
            Some(nanos) if nanos >= 0 => nanos as u64,
            _ => return Err(CronTaskError::TimeCalculationError),
        };

        let tick_index = nanos / self.tick_duration.as_nanos() as u64;
        Ok(tick_index as usize)
    }

    /// 获取时间对应的槽位（取模后的结果）
    ///
    /// # 参数
    /// * `timestamp` - 时间戳
    ///
    /// # 返回值
    /// 返回时间对应的槽位索引
    pub fn get_slot(&self, timestamp: NaiveDateTime) -> Result<usize, CronTaskError> {
        let real_slot = self.get_real_slot(timestamp)?;
        Ok(real_slot % self.total_slots)
    }

    /// 直接计算目标时间对应的槽位，避免重复计算
    ///
    /// # 参数
    /// * `timestamp` - 任务触发时间
    /// * `delay` - 延迟时间
    ///
    /// # 返回值
    /// 返回目标时间和对应的槽位
    pub fn calculate_target_slot(&self, timestamp: NaiveDateTime, delay: Duration) -> Result<(NaiveDateTime, usize), CronTaskError> {
        // 计算目标时间
        let delta = TimeDelta::from_std(delay)
            .map_err(|e| CronTaskError::TimeConversionFailed(format!("时间转换失败: {e}")))?;
        let target_time = timestamp
            .checked_add_signed(delta)
            .ok_or(CronTaskError::TimeOverflow)?;

        // 计算目标槽位
        let target_slot = self.get_slot(target_time)?;
        Ok((target_time, target_slot))
    }

    /// 添加任务到时间轮
    ///
    /// # 参数
    /// * `timestamp` - 任务触发时间
    /// * `delay` - 延迟时间
    /// * `key` - 任务唯一标识符
    ///
    /// # 返回值
    /// 返回操作结果
    pub async fn add_task(
        &self,
        timestamp: NaiveDateTime,
        delay: Duration,
        key: String,
    ) -> Result<String, CronTaskError> {
        // 使用新方法计算目标时间和槽位，避免重复计算
        let (target_time, target_slot) = self.calculate_target_slot(timestamp, delay)?;
        
        // 检查时间有效性
        self.validate_task_time(target_time, Self::get_current_time())?;

        // 添加任务
        self.insert_task(target_slot, key).await
    }

    /// 从时间轮中删除任务
    ///
    /// # 参数
    /// * `timestamp` - 任务原定触发时间
    /// * `delay` - 原定延迟时间
    /// * `key` - 任务唯一标识符
    ///
    /// # 返回值
    /// 返回操作结果
    pub async fn del_task(
        &self,
        timestamp: NaiveDateTime,
        delay: Duration,
        key: String,
    ) -> Result<String, CronTaskError> {
        // 使用新方法计算目标时间和槽位，避免重复计算
        let (target_time, target_slot) = match self.calculate_target_slot(timestamp, delay) {
            Ok(result) => result,
            Err(e) => {
                // 如果计算失败，返回错误信息
                return Ok(format!("计算目标槽位失败: {e}"));
            }
        };

        // 如果任务时间已过时
        if target_time < Self::get_current_time() {
            // 即使时间已过，也要尝试从时间轮中删除任务
            let slot_index = target_slot % self.total_slots;
            let slot = self.slots[slot_index].load();
            let mut tasks = slot.tasks.lock().await;
            if tasks.remove(&key) {
                return Ok("任务已取消".to_string());
            }
            return Ok("任务时间已过时,无需取消".to_string());
        }

        // 使用存储的当前槽位而不是实时计算
        let current_slot = self.current_slot.load(Ordering::Relaxed);

        if target_slot <= current_slot || target_slot - current_slot >= self.total_slots {
            // 即使超出范围，也要尝试删除任务
            let slot_index = target_slot % self.total_slots;
            let slot = self.slots[slot_index].load();
            let mut tasks = slot.tasks.lock().await;
            if tasks.remove(&key) {
                return Ok("任务已取消".to_string());
            }
            return Ok("任务时间已过时或超出范围,无需取消".to_string());
        }

        // 删除任务
        self.remove_task(target_slot, key).await
    }

    /// 使用时间总线的秒信号驱动时间轮运行
    pub async fn run<F, Fut>(self: Arc<Self>, on_tick: F)
    where
        F: Fn(NaiveDateTime) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        // 基于实际时间计算当前槽位，而不是简单地递增
        match self.get_slot(Self::get_current_time()) {
            Ok(current_slot) => {
                // 原子更新当前槽位
                self.current_slot.store(current_slot, Ordering::Release);

                // 执行任务处理
                self.process_current_slot_with_callback(&on_tick).await;
            }
            Err(e) => {
                // 记录错误但不中断执行
                eprintln!("时间轮计算槽位失败: {e}");
            }
        }
    }

    /// 获取当前时间的辅助函数
    fn get_current_time() -> NaiveDateTime {
        Utc::now().with_timezone(&Shanghai).naive_local()
    }
}

// 私有辅助方法实现
impl TimeWheel {
    /// 验证任务时间有效性
    fn validate_task_time(
        &self,
        target_time: NaiveDateTime,
        now: NaiveDateTime,
    ) -> Result<(), CronTaskError> {
        // 允许少量时间差，考虑到系统调度可能存在的微小延迟
        let time_diff = target_time - now;
        if time_diff < TimeDelta::milliseconds(-100) {
            return Err(CronTaskError::TaskPastDue);
        }

        // 获取真实的槽位索引，用于比较是否超出当前周期
        let current_real_slot = self.get_real_slot(now).unwrap_or_default(); // 如果获取当前槽位失败，使用0作为默认值

        let target_real_slot = self.get_real_slot(target_time)?;

        // 检查目标槽位是否超出了时间轮的当前周期
        // 只有当前一个总槽位周期内的时间点才能订阅
        if target_real_slot < current_real_slot {
            return Err(CronTaskError::TaskPastDue);
        }

        if target_real_slot - current_real_slot >= self.total_slots {
            return Err(CronTaskError::TaskTooFarInFuture);
        }

        Ok(())
    }

    /// 插入任务到指定槽位
    async fn insert_task(
        &self,
        target_slot: usize,
        key: String,
    ) -> Result<String, CronTaskError> {
        let slot_index = target_slot % self.total_slots;
        let slot = self.slots[slot_index].load();
        let mut tasks = slot.tasks.lock().await;

        if tasks.contains(&key) {
            return Err(CronTaskError::TaskAlreadyExists);
        }

        tasks.insert(key.clone());
        Ok(key)
    }

    /// 从指定槽位删除任务
    async fn remove_task(&self, target_slot: usize, key: String) -> Result<String, CronTaskError> {
        let slot_index = target_slot % self.total_slots;
        let slot = self.slots[slot_index].load();
        let mut tasks = slot.tasks.lock().await;

        if tasks.remove(&key) {
            Ok("任务已取消".to_string())
        } else {
            Ok("任务不存在,无需取消".to_string())
        }
    }

    /// 执行当前槽位中的所有任务并调用回调函数
    pub async fn process_current_slot_with_callback<F, Fut>(&self, on_tick: &F)
    where
        F: Fn(NaiveDateTime) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let current = self.current_slot.load(Ordering::Relaxed);

        // 验证槽位索引的有效性
        if current >= self.slots.len() {
            eprintln!(
                "警告: 无效的槽位索引 {}，最大有效索引为 {}",
                current,
                self.slots.len() - 1
            );
            return;
        }

        let slot = self.slots[current].load();

        // 1. 先获取当前时间，减少后续计算
        let now = Self::get_current_time();

        // 2. 同步执行回调，避免生命周期问题
        match tokio::time::timeout(
            std::time::Duration::from_millis(100),
            on_tick(now),
        )
        .await
        {
            Ok(_) => {}
            Err(_) => {
                eprintln!("警告: 时间轮回调执行超时");
            }
        }

        // 3. 获取并处理任务，最小化锁持有时间
        let task_clones = match tokio::time::timeout(
            std::time::Duration::from_millis(50),
            slot.tasks.lock()
        ).await {
            Ok(mut tasks) => {
                // 立即获取所有任务并释放锁，减少锁持有时间
                let task_clones: Vec<_> = tasks.drain().collect();
                // 显式释放锁
                drop(tasks);
                task_clones
            }
            Err(_) => {
                eprintln!("警告: 获取任务锁超时，跳过任务处理");
                return;
            }
        };

        // 4. 批量发送任务执行事件，减少上下文切换
        if !task_clones.is_empty() {
            // 克隆message_bus以避免生命周期问题
            let message_bus = self.message_bus.clone();
            // 使用spawn处理异步消息发送
            tokio::task::spawn(async move {
                for key in task_clones {
                    let message = CronMessage::ExecuteTask { key };
                    if let Err(e) = message_bus.send(message) {
                        eprintln!("警告: 发送任务执行消息失败: {}", e);
                    }
                }
            });
        }
    }
}
