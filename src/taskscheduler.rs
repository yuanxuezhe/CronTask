//use crate::taskerror::TaskError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender, Receiver, channel};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex; // 替换为异步锁
use arc_swap::ArcSwap;
use hashbrown::HashMap;
use chrono::{NaiveDateTime, TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::hint::spin_loop;

// 修改任务类型，闭包携带字符串参数
type Task = Arc<dyn Fn(String, String) + Send + Sync + 'static>;

struct TimeWheelSlot {
    tasks: AsyncMutex<HashMap<String, (Task, String)>>, // 使用 Tokio 的异步锁
}

struct TimeWheel {
    slots: Vec<ArcSwap<TimeWheelSlot>>,
    current_slot: Arc<AtomicUsize>,
    tick_duration: Duration,
    total_slots: usize,
    base_time: NaiveDateTime,
}

impl TimeWheel {
    fn new(tick_duration: Duration, total_slots: usize) -> Self {
        let slots = (0..total_slots)
            .map(|_| {
                ArcSwap::new(Arc::new(TimeWheelSlot {
                    tasks: AsyncMutex::new(HashMap::new()),
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

    fn get_real_slot(&self, timestamp: NaiveDateTime) -> usize {
        // 获取当天从午夜到当前时间的总纳秒数
        //let time = timestamp.time();
        let duration = timestamp - self.base_time;

        // 转换为总纳秒
        let nanos = duration.num_nanoseconds().unwrap() as u64;
            
        // 获取当前时间戳在时间轮上处于第几个 tick
        let tick_index = nanos / self.tick_duration.as_nanos() as u64;

        // 对总槽数取模，得到所在槽位
        tick_index as usize
    }

    fn get_slot(&self, timestamp: NaiveDateTime) -> usize {
        self.get_real_slot(timestamp) % self.total_slots
    }

    // 添加任务时传入字符串参数
    async fn add_task(&self, timestamp: NaiveDateTime, delay: Duration, key: String, arg: String, task: Task) -> Result<String, String> {
        // 获取当前时间（上海时区）
        let now = Utc::now().with_timezone(&Shanghai).naive_local();

        let current_slot = self.get_real_slot(now); // 获取当前槽位

        // 将 std::time::Duration 转换为 chrono::TimeDelta
        let delta = TimeDelta::from_std(delay)
            .map_err(|e| format!("时间转换失败: {}", e))?;

        // 计算目标时间
        let target_time = timestamp.checked_add_signed(delta)
            .ok_or("时间计算溢出（超出范围）")?;

        let target_slot = self.get_real_slot(target_time); // 获取目标槽位
        
        if target_time < now {
            return Err("任务时间已过时".to_string());
        }
        // 检查目标时间是否早于当前时间
        if target_slot <= current_slot {
            return Err("任务时间已过时".to_string());
        }

        if target_slot - current_slot >= self.total_slots {
            return Err("任务时间超出时间轮最大范围，延迟添加到监控".to_string());
        }

        let slot = self.slots[target_slot % self.total_slots].load();

        let mut tasks = slot.tasks.lock().await;
        // 检查是否已存在相同的任务
        if tasks.contains_key(&key) {
            return Err("任务已存在".to_string());
        }

        tasks.insert(key.clone(), (task, arg));

        Ok(key)
    }

    async fn run(&self) {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
        let now_ns = since_epoch.as_nanos();

        // 时间对齐到下一整 tick_duration
        let tick_ns = self.tick_duration.as_nanos();
        let next_tick_ns = ((now_ns / tick_ns) + 1) * tick_ns;
        let remaining_ns = next_tick_ns - now_ns;
        spin_sleep::sleep(Duration::from_nanos(remaining_ns.try_into().unwrap())); // 或 std::thread::sleep
        
        // 创建 Tokio 定时器
        let mut interval = tokio::time::interval(self.tick_duration);
        
        // 计算当日秒数
        let current_slot = self.get_slot(Utc::now().with_timezone(&Shanghai).naive_local());
        self.current_slot.store(current_slot, Ordering::Release);
        loop {
            // 等待下一次触发
            interval.tick().await;

            let current = self.current_slot.load(Ordering::Relaxed);
       
            let slot = self.slots[current].load();
            let mut tasks = slot.tasks.lock().await;
            // 执行所有任务（在阻塞线程中运行）
            let task_clones: Vec<_> = tasks.drain().map(|(key, (task, arg))| (key, task, arg)).collect();
            drop(tasks); // 提前释放锁
           
            for (key, task, arg) in task_clones {
                let task = Arc::clone(&task);
                let arg = arg.clone();
                tokio::task::spawn_blocking(move || {
                    task(key.clone(), arg); // 在专用线程池执行
                });
            }

            // 更新槽位
            self.current_slot.store(
                (current + 1) % self.total_slots,
                Ordering::Release
            );
        }
    }

    async fn run_highprecision(&self) {
        let mut next_tick = {
            // 当前系统时间（UNIX 时间戳）
            let now = SystemTime::now();
            let dur = now.duration_since(UNIX_EPOCH).unwrap();
            let now_ns = dur.as_nanos();

            // tick_duration 纳秒
            let tick_ns = self.tick_duration.as_nanos();

            // 下一个整 tick 时刻（纳秒）
            let next_tick_ns = ((now_ns / tick_ns) + 1) * tick_ns;

            // 差值纳秒
            let diff_ns = next_tick_ns - now_ns;

            // 当前 Instant 与 SystemTime 的差值偏移
            let now_inst = Instant::now();
            let offset = now_inst - Instant::now(); // 微小偏移修正

            // 返回下一整 tick 对齐时间点（Instant）
            now_inst + Duration::from_nanos(diff_ns as u64) + offset
        };

        let current_slot = self.get_slot(Utc::now().with_timezone(&Shanghai).naive_local());
        self.current_slot.store(current_slot, Ordering::Release);
        loop {
            // 忙等直到时间点
            while Instant::now() < next_tick {
                spin_loop(); // 精确等待
            }

            next_tick += self.tick_duration;

            let current = self.current_slot.load(Ordering::Relaxed);
       
            let slot = self.slots[current].load();
            let mut tasks = slot.tasks.lock().await;
            // 执行所有任务（在阻塞线程中运行）
            let task_clones: Vec<_> = tasks.drain().map(|(key, (task, arg))| (key, task, arg)).collect();
            drop(tasks); // 提前释放锁
           
            for (key, task, arg) in task_clones {
                let task = Arc::clone(&task);
                let arg = arg.clone();
                tokio::task::spawn_blocking(move || {
                    task(key.clone(), arg); // 在专用线程池执行
                });
            }

            // 更新槽位
            self.current_slot.store(
                (current + 1) % self.total_slots,
                Ordering::Release
            );
        }
    }
}

// 调度器接口支持字符串参数
pub struct TaskScheduler {
    sender: Sender<(
        NaiveDateTime, 
        Duration, 
        String, 
        String, 
        Task, 
        Sender<String> // 用于返回结果的通道
        )>,
}

impl TaskScheduler {
    pub fn new(tick_duration: Duration, total_slots: usize, high_precision: bool) -> Self {
        // 修改后（显式指定类型）
        let (sender, receiver) = mpsc::channel::<(
            NaiveDateTime,
            Duration,
            String,
            String,
            Task,                // 使用已定义的 Task 类型别名
            Sender<String>,      // 明确返回通道类型
        )>();

        let time_wheel = Arc::new(TimeWheel::new(tick_duration, total_slots));
        
        // 接收任务线程
        let tw_clone = Arc::clone(&time_wheel);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                for (timestamp, delay, key, arg, task, result_sender) in receiver {
                    let arg_for_msg = arg.clone();
                    let result = tw_clone.add_task(timestamp, delay, key, arg, task).await
                         .map(|key| format!("{}|{}", key, arg_for_msg))
                        .unwrap_or_else(|e| format!("错误: {}", e));
                    result_sender.send(result).unwrap();
                }
            });
        });

        // 驱动时间轮
        let tw_driver = Arc::clone(&time_wheel);
        thread::spawn(move || {
            //tw_driver.run();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                if high_precision {
                    tw_driver.run_highprecision().await;
                } else {
                    tw_driver.run().await;
                }
                
            });
        });

        Self { sender }
    }

    // 新接口：支持传入字符串参数
    pub fn schedule<F, K>(
        &self, 
        timestamp: NaiveDateTime, 
        delay: Duration, 
        key: K,               // ← 新增参数 key
        arg: String, 
        task: F
    ) -> Receiver<String>
    where
        F: Fn(String, String) + Send + Sync + 'static,
        K: ToString,          // ← 要求 key 可转为 String
    {
        let arc_task = Arc::new(task);
        // 将 key 转为 String
        let key_str = key.to_string();
        // 创建临时通道，用于接收返回值
        let (result_sender, result_receiver) = channel();
        
        // 发送任务请求，附带 result_sender
        self.sender.send((timestamp, delay, key.to_string(), arg, arc_task, result_sender))
            .expect("Failed to send task");
        
        result_receiver
    }
}