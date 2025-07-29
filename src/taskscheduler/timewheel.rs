use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use arc_swap::ArcSwap;
use hashbrown::HashMap;
use chrono::{NaiveDateTime, TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::hint::spin_loop;
use tokio::sync::Mutex;

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
    pub fn get_real_slot(&self, timestamp: NaiveDateTime) -> usize {
        let duration = timestamp - self.base_time;
        let nanos = duration
            .num_nanoseconds()
            .expect("时间超出范围，计算失败") as u64;
        let tick_index = nanos / self.tick_duration.as_nanos() as u64;
        tick_index as usize
    }
    
    /// 获取时间对应的槽位（取模后的结果）
    /// 
    /// # 参数
    /// * `timestamp` - 时间戳
    /// 
    /// # 返回值
    /// 返回时间对应的槽位索引
    pub fn get_slot(&self, timestamp: NaiveDateTime) -> usize {
        self.get_real_slot(timestamp) % self.total_slots
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
    /// 成功时返回任务key，失败时返回错误信息
    pub async fn add_task(&self, timestamp: NaiveDateTime, delay: Duration, key: String, arg: String, task: Task) -> Result<String, String> {
        let now = Utc::now().with_timezone(&Shanghai).naive_local();
        let current_slot = self.get_real_slot(now);
        let delta = TimeDelta::from_std(delay)
            .map_err(|e| format!("时间转换失败: {}", e))?;
        let target_time = timestamp.checked_add_signed(delta)
            .ok_or("时间计算溢出（超出范围）")?;
        let target_slot = self.get_real_slot(target_time);
        if target_time < now {
            return Err("任务时间已过时".to_string());
        }
        if target_slot <= current_slot {
            return Err("任务时间已过时".to_string());
        }
        if target_slot - current_slot >= self.total_slots {
            return Err("任务时间超出时间轮最大范围，延迟添加到监控".to_string());
        }
        let slot = self.slots[target_slot % self.total_slots].load();
        let mut tasks = slot.tasks.lock().await;
        if tasks.contains_key(&key) {
            return Err("任务已存在".to_string());
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
    /// 返回操作结果信息
    pub async fn del_task(&self, timestamp: NaiveDateTime, delay: Duration, key: String) -> Result<String, String> {
        let now = Utc::now().with_timezone(&Shanghai).naive_local();
        let current_slot = self.get_real_slot(now);
        let delta = TimeDelta::from_std(delay)
            .map_err(|e| format!("时间转换失败: {}", e))?;
        let target_time = timestamp.checked_add_signed(delta)
            .ok_or("时间计算溢出（超出范围）");
        let target_slot = match target_time {
            Ok(t) => self.get_real_slot(t),
            Err(_) => return Ok("任务时间已过时,无需取消".to_string()),
        };
        if let Ok(t) = target_time {
            if t < now {
                return Ok("任务时间已过时,无需取消".to_string());
            }
            if target_slot <= current_slot {
                return Ok("任务时间已过时,无需取消".to_string());
            }
            if target_slot - current_slot >= self.total_slots {
                return Ok("任务时间超出时间轮最大范围,无需取消".to_string());
            }
        }
        let slot = self.slots[target_slot % self.total_slots].load();
        let mut tasks = slot.tasks.lock().await;
        match tasks.remove(&key) {
            Some(_) => Ok("任务已取消".to_string()),
            None => Ok("任务不存在,无需取消".to_string()),
        }
    }
    
    /// 使用tokio::time::interval驱动时间轮运行
    pub async fn run(&self) {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
        let now_ns = since_epoch.as_nanos();
        let tick_ns = self.tick_duration.as_nanos();
        let next_tick_ns = ((now_ns / tick_ns) + 1) * tick_ns;
        let remaining_ns = next_tick_ns - now_ns;
        spin_sleep::sleep(Duration::from_nanos(remaining_ns.try_into().unwrap()));
        let mut interval = tokio::time::interval(self.tick_duration);
        let current_slot = self.get_slot(Utc::now().with_timezone(&Shanghai).naive_local());
        self.current_slot.store(current_slot, Ordering::Release);
        loop {
            interval.tick().await;
            self.process_current_slot().await;
            let current = self.current_slot.load(Ordering::Relaxed);
            self.current_slot.store(
                (current + 1) % self.total_slots,
                Ordering::Release
            );
        }
    }
    
    /// 使用自旋锁驱动时间轮运行，提供更高精度的时间控制
    pub async fn run_highprecision(&self) {
        let mut next_tick = {
            let now = SystemTime::now();
            let dur = now.duration_since(UNIX_EPOCH).unwrap();
            let now_ns = dur.as_nanos();
            let tick_ns = self.tick_duration.as_nanos();
            let next_tick_ns = ((now_ns / tick_ns) + 1) * tick_ns;
            let diff_ns = next_tick_ns - now_ns;
            let now_inst = Instant::now();
            let offset = now_inst - Instant::now();
            now_inst + Duration::from_nanos(diff_ns as u64) + offset
        };
        let current_slot = self.get_slot(Utc::now().with_timezone(&Shanghai).naive_local());
        self.current_slot.store(current_slot, Ordering::Release);
        loop {
            while Instant::now() < next_tick {
                spin_loop();
            }
            next_tick += self.tick_duration;
            self.process_current_slot().await;
            let current = self.current_slot.load(Ordering::Relaxed);
            self.current_slot.store(
                (current + 1) % self.total_slots,
                Ordering::Release
            );
        }
    }
    
    /// 执行当前槽位中的所有任务
    async fn process_current_slot(&self) {
        let current = self.current_slot.load(Ordering::Relaxed);
        let slot = self.slots[current].load();
        let mut tasks = slot.tasks.lock().await;
        let task_clones: Vec<_> = tasks.drain().map(|(key, (task, arg))| (key, task, arg)).collect();
        drop(tasks);
        for (key, task, arg) in task_clones {
            let task = Arc::clone(&task);
            tokio::task::spawn_blocking(move || {
                task(key, arg);
            });
        }
    }
}