// 标准库导入
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// 外部 crate 导入
use arc_swap::ArcSwap;
use hashbrown::HashMap;
use chrono::{NaiveDateTime, TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;
use std::time::Duration;
use tokio::sync::Mutex;

// 内部模块导入
use crate::common::error::CronTaskError;

/// 任务类型定义
pub type Task = Arc<dyn Fn(String, String) + Send + Sync + 'static>;

/// 时间轮槽位
pub struct TimeWheelSlot {
    /// 存储任务的HashMap，键为任务key，值为任务函数和参数
    pub tasks: Mutex<HashMap<String, (Task, String)>>,
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
}

impl TimeWheel {
    /// 创建新的时间轮
    /// 
    /// # 参数
    /// * `tick_duration` - 滴答间隔
    /// * `total_slots` - 总槽数
    /// 
    /// # 返回值
    /// 返回新的时间轮实例
    pub fn new(tick_duration: Duration, total_slots: usize) -> Self {
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
            base_time: Self::get_current_time(),
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
        
        let duration = timestamp - self.base_time;
        let nanos = match duration.num_nanoseconds() {
            Some(nanos) => nanos as u64,
            None => return Err(CronTaskError::TimeCalculationError),
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
        Ok(self.get_real_slot(timestamp)? % self.total_slots)
    }
    
    /// 添加任务到时间轮
    /// 
    /// # 参数
    /// * `timestamp` - 任务触发时间
    /// * `delay` - 延迟时间
    /// * `key` - 任务唯一标识符
    /// * `arg` - 任务参数
    /// * `task` - 任务执行函数
    /// 
    /// # 返回值
    /// 返回操作结果
    pub async fn add_task(&self, timestamp: NaiveDateTime, delay: Duration, key: String, arg: String, task: Task) -> Result<String, CronTaskError> {
        // 计算目标时间
        let delta = TimeDelta::from_std(delay)
            .map_err(|e| CronTaskError::TimeConversionFailed(format!("时间转换失败: {}", e)))?;
        let target_time = timestamp.checked_add_signed(delta)
            .ok_or(CronTaskError::TimeOverflow)?;
            
        // 使用存储的当前槽位而不是实时计算
        let target_slot = self.get_slot(target_time)?;
        // 打印当前槽位和目标槽位
        // println!("当前槽位: {}, 目标槽位: {}", current_slot, target_slot);
        // 检查时间有效性
        self.validate_task_time(target_time, Self::get_current_time())?;
        
        // 添加任务
        self.insert_task(target_slot, key, task, arg).await
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
    pub async fn del_task(&self, timestamp: NaiveDateTime, delay: Duration, key: String) -> Result<String, CronTaskError> {
        // 计算目标时间
        let delta = TimeDelta::from_std(delay)
            .map_err(|e| CronTaskError::TimeConversionFailed(e.to_string()))?;
        let target_time = timestamp.checked_add_signed(delta)
            .ok_or(CronTaskError::TimeOverflow)?;
        
        // 如果任务时间已过时
        if target_time < Self::get_current_time() {
            // 即使时间已过，也要尝试从时间轮中删除任务
            if let Ok(target_slot) = self.get_slot(target_time) {
                let slot_index = target_slot % self.total_slots;
                let slot = self.slots[slot_index].load();
                let mut tasks = slot.tasks.lock().await;
                if tasks.remove(&key).is_some() {
                    return Ok("任务已取消".to_string());
                }
            }
            return Ok("任务时间已过时,无需取消".to_string());
        }
        
        // 使用存储的当前槽位而不是实时计算
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let target_slot = self.get_slot(target_time)?;
        
        if target_slot <= current_slot || target_slot - current_slot >= self.total_slots {
            // 即使超出范围，也要尝试删除任务
            let slot_index = target_slot % self.total_slots;
            let slot = self.slots[slot_index].load();
            let mut tasks = slot.tasks.lock().await;
            if tasks.remove(&key).is_some() {
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
            },
            Err(e) => {
                // 记录错误但不中断执行
                eprintln!("时间轮计算槽位失败: {}", e);
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
        now: NaiveDateTime
    ) -> Result<(), CronTaskError> {
        // 允许少量时间差，考虑到系统调度可能存在的微小延迟
        let time_diff = target_time - now;
        if time_diff < TimeDelta::milliseconds(-100) {
            return Err(CronTaskError::TaskPastDue);
        }
        
        // 获取真实的槽位索引，用于比较是否超出当前周期
        let current_real_slot = match self.get_real_slot(now) {
            Ok(slot) => slot,
            Err(_) => 0, // 如果获取当前槽位失败，使用0作为默认值
        };
        
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
    async fn insert_task(&self, target_slot: usize, key: String, task: Task, arg: String) -> Result<String, CronTaskError> {
        let slot_index = target_slot % self.total_slots;
        let slot = self.slots[slot_index].load();
        let mut tasks = slot.tasks.lock().await;
        
        if tasks.contains_key(&key) {
            return Err(CronTaskError::TaskAlreadyExists);
        }
        
        tasks.insert(key.clone(), (task, arg));
        Ok(key)
    }
    
    /// 从指定槽位删除任务
    async fn remove_task(&self, target_slot: usize, key: String) -> Result<String, CronTaskError> {
        let slot_index = target_slot % self.total_slots;
        let slot = self.slots[slot_index].load();
        let mut tasks = slot.tasks.lock().await;
        
        match tasks.remove(&key) {
            Some(_) => Ok("任务已取消".to_string()),
            None => Ok("任务不存在,无需取消".to_string()),
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
            eprintln!("警告: 无效的槽位索引 {}，最大有效索引为 {}", current, self.slots.len() - 1);
            return;
        }
        
        let slot = self.slots[current].load();

        // 使用实时时间而不是基于tick计数的时间
        // 触发滴答事件回调
        match tokio::time::timeout(std::time::Duration::from_millis(100), on_tick(Self::get_current_time())).await {
            Ok(_) => {},
            Err(_) => {
                eprintln!("警告: 时间轮回调执行超时");
            }
        }
        
        // 获取并释放任务锁，添加超时处理
        let task_clones = match tokio::time::timeout(std::time::Duration::from_millis(50), slot.tasks.lock()).await {
            Ok(mut tasks) => {
                let task_clones: Vec<_> = tasks.drain().map(|(key, (task, arg))| (key, task, arg)).collect();
                drop(tasks);
                task_clones
            },
            Err(_) => {
                eprintln!("警告: 获取任务锁超时，跳过任务处理");
                return;
            }
        };
        
        // 并行执行任务
        for (key, task, arg) in task_clones {
            let task = Arc::clone(&task);
            // 使用spawn而不是spawn_blocking，除非任务确实需要阻塞
            tokio::task::spawn(async move {
                task(key, arg);
            });
        }
    }
}