use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use arc_swap::ArcSwap;
use hashbrown::HashMap;
use chrono::{NaiveDateTime, TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::hint::spin_loop;
use tokio::sync::Mutex;
use crate::comm::error::TaskSchedulerError;

pub type Task = Arc<dyn Fn(String, String) + Send + Sync + 'static>;

pub struct TimeWheelSlot {
    /// 存储任务的HashMap，键为任务key，值为任务函数和参数
    pub tasks: Mutex<HashMap<String, (Task, String)>>,
}

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
            base_time: Utc::now().with_timezone(&Shanghai).naive_local(),
        }
    }
    
    /// 获取时间对应的绝对槽位
    /// 
    /// # 参数
    /// * `timestamp` - 时间戳
    /// 
    /// # 返回值
    /// 返回时间对应的绝对槽位索引
    pub fn get_real_slot(&self, timestamp: NaiveDateTime) -> Result<usize, TaskSchedulerError> {
        let duration = timestamp - self.base_time;
        let nanos = match duration.num_nanoseconds() {
            Some(nanos) if nanos >= 0 => nanos as u64,
            _ => return Err(TaskSchedulerError::TaskPastDue),
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
    pub fn get_slot(&self, timestamp: NaiveDateTime) -> Result<usize, TaskSchedulerError> {
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
    pub async fn add_task(&self, timestamp: NaiveDateTime, delay: Duration, key: String, arg: String, task: Task) -> Result<String, TaskSchedulerError> {
        let now = Utc::now().with_timezone(&Shanghai).naive_local();
        
        // 计算目标时间
        let delta = TimeDelta::from_std(delay)
            .map_err(|e| TaskSchedulerError::TimeConversionFailed(format!("时间转换失败: {}", e)))?;
        let target_time = timestamp.checked_add_signed(delta)
            .ok_or(TaskSchedulerError::TimeOverflow)?;
        let current_slot = self.get_real_slot(now)?;
        let target_slot = self.get_real_slot(target_time)?;
        if target_time < now {
            return Err(TaskSchedulerError::TaskPastDue);
        }
        if target_slot <= current_slot {
            return Err(TaskSchedulerError::TaskPastDue);
        }
        if target_slot - current_slot >= self.total_slots {
            return Err(TaskSchedulerError::TaskTooFarInFuture);
        }
        
        // 添加任务
        let slot = self.slots[target_slot % self.total_slots].load();
        let mut tasks = slot.tasks.lock().await;
        
        if tasks.contains_key(&key) {
            return Err(TaskSchedulerError::TaskAlreadyExists);
        }
        
        tasks.insert(key.clone(), (task, arg));
        Ok(key)
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
    pub async fn del_task(&self, timestamp: NaiveDateTime, delay: Duration, key: String) -> Result<String, TaskSchedulerError> {
        let now = Utc::now().with_timezone(&Shanghai).naive_local();
        
        // 计算目标时间
        let delta = TimeDelta::from_std(delay)
            .map_err(|e| TaskSchedulerError::TimeConversionFailed(e.to_string()))?;
        let target_time = timestamp.checked_add_signed(delta)
            .ok_or(TaskSchedulerError::TimeOverflow)?;
        
        // 如果任务时间已过时
        if target_time < now {
            return Ok("任务时间已过时,无需取消".to_string());
        }
        
        // 获取槽位
        let current_slot = self.get_real_slot(now)?;
        let target_slot = self.get_real_slot(target_time)?;
        
        if target_slot <= current_slot || target_slot - current_slot >= self.total_slots {
            return Ok("任务时间已过时或超出范围,无需取消".to_string());
        }
        
        // 删除任务
        let slot = self.slots[target_slot % self.total_slots].load();
        let mut tasks = slot.tasks.lock().await;
        
        match tasks.remove(&key) {
            Some(_) => Ok("任务已取消".to_string()),
            None => Ok("任务不存在,无需取消".to_string()),
        }
    }
    
    /// 使用时间总线的秒信号驱动时间轮运行
    pub async fn run<F, Fut>(self: Arc<Self>, on_tick: F) 
    where 
        F: Fn(NaiveDateTime) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        // 执行一次任务处理
        self.process_current_slot_with_callback(&on_tick).await;
        
        // 更新当前槽位
        let current = self.current_slot.load(Ordering::Relaxed);
        self.current_slot.store(
            (current + 1) % self.total_slots,
            Ordering::Release
        );
    }

    /// 执行当前槽位中的所有任务并调用回调函数
    pub async fn process_current_slot_with_callback<F, Fut>(&self, on_tick: &F) 
    where 
        F: Fn(NaiveDateTime) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let current = self.current_slot.load(Ordering::Relaxed);
        let slot = self.slots[current].load();
        
        // 计算当前时间
        let elapsed_ticks = current;
        let elapsed_nanos = (elapsed_ticks as u128) * self.tick_duration.as_nanos();
        let current_time = self.base_time + 
            TimeDelta::nanoseconds(elapsed_nanos as i64);

        // 触发滴答事件回调
        on_tick(current_time).await;
        
        // 获取并释放任务锁
        let task_clones = {
            let mut tasks = slot.tasks.lock().await;
            let task_clones: Vec<_> = tasks.drain().map(|(key, (task, arg))| (key, task, arg)).collect();
            drop(tasks);
            task_clones
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