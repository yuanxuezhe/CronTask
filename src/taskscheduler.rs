//use crate::taskerror::TaskError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender, Receiver, channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use arc_swap::ArcSwap;
use hashbrown::HashMap;
use chrono::{NaiveDateTime, Timelike, TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

// 修改任务类型，闭包携带字符串参数
type Task = Box<dyn Fn(String) + Send + Sync + 'static>;

struct TimeWheelSlot {
    tasks: Mutex<HashMap<u64, (Task, String)>>, // 存储任务闭包和字符串参数
}

struct TimeWheel {
    slots: Vec<ArcSwap<TimeWheelSlot>>,
    current_slot: Arc<AtomicUsize>,
    tick_duration: Duration,
    total_slots: usize,
}

impl TimeWheel {
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

    // 添加任务时传入字符串参数
    fn add_task(&self, timestamp: NaiveDateTime, delay: Duration, task: Task, arg: String) -> Result<u64, String> {
        // 获取当前时间（上海时区）
        let now = Utc::now().with_timezone(&Shanghai).naive_local();

        // 将 std::time::Duration 转换为 chrono::TimeDelta
        let delta = TimeDelta::from_std(delay)
            .map_err(|e| format!("时间转换失败: {}", e))?;

        // 计算目标时间
        let target_time = timestamp.checked_add_signed(delta)
            .ok_or("时间计算溢出（超出范围）")?;

        // 检查目标时间是否早于当前时间
        if target_time < now {
            return Err("任务时间已过时".to_string());
        }

        // 计算目标时间与当前时间的差值
        let duration_since_now = target_time - now;

        // 检查是否超过24小时（24h = 24 * 3600秒）
        if duration_since_now > TimeDelta::hours(24) {
            return Err("任务时间超出时间轮最大范围，延迟添加到监控".to_string());
        }

        // 计算NaiveDateTime属于第几格  根据当天的秒数确定，比如当前时间9点45分50秒，属于  9*3600 + 45*60 + 50
        let current_seconds = timestamp.time().num_seconds_from_midnight() % (24 * 3600);
        let current_seconds =  current_seconds as usize;
        // 计算延迟的秒数
        let ticks = (delay.as_nanos() / self.tick_duration.as_nanos()) as usize;
        let target_slot = (current_seconds + ticks) % self.total_slots;
        //println!("当前时间: {}, 目标槽: {}  msg:{}", current_seconds, target_slot, arg);
        
        let slot = self.slots[target_slot].load();
        //let mut tasks = slot.tasks.lock().unwrap();
        let mut tasks = slot.tasks
            .lock()
            .map_err(|e| format!("Failed to lock tasks: {:?}", e))?;

        let task_id = rand::random::<u64>();
        tasks.insert(task_id, (task, arg));

        Ok(task_id)
    }

    fn run(&self) {
        // 对齐到下一个整秒
        let mut start_time = {
            let now = SystemTime::now();
            let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
            let nanos = since_epoch.subsec_nanos();
            let remaining_ns = 1_000_000_000 - nanos as u64;
            spin_sleep::sleep(Duration::from_nanos(remaining_ns)); // 或 std::thread::sleep
            Instant::now()
        };
        
        // 计算当日秒数
        let now1 = Utc::now().with_timezone(&Shanghai);
        let current_slot = now1.num_seconds_from_midnight() as usize;
        self.current_slot.store(current_slot, Ordering::Release);
        loop {
            let current = self.current_slot.load(Ordering::Relaxed);
       
            let slot = self.slots[current].load();
            let mut tasks = slot.tasks.lock().unwrap();
            for (task, arg) in tasks.values() {
                task(arg.clone()); // 执行时传入参数
            }
            tasks.clear();

            start_time += self.tick_duration;

            self.current_slot.store((current + 1) % self.total_slots, Ordering::Release);
            // 高精度等待
            spin_sleep::sleep(start_time - Instant::now());
        }
    }
}

// 调度器接口支持字符串参数
pub struct TaskScheduler {
    sender: Sender<(
        NaiveDateTime, 
        Duration, 
        Task, 
        String, 
        Sender<String> // 用于返回结果的通道
        )>,
}

impl TaskScheduler {
    pub fn new(tick_duration: Duration, total_slots: usize) -> Self {
        //let (sender, receiver) = mpsc::channel();
        // 修改后（显式指定类型）
        let (sender, receiver) = mpsc::channel::<(
            NaiveDateTime,
            Duration,
            Task,                // 使用已定义的 Task 类型别名
            String,
            Sender<String>,      // 明确返回通道类型
        )>();

        let time_wheel = Arc::new(TimeWheel::new(tick_duration, total_slots));
        
        // 接收任务线程
        let tw_clone = time_wheel.clone();
        thread::spawn(move || {
            for (timestamp, delay, task, arg, result_sender) in receiver {
                //let result = tw_clone.add_task(timestamp, delay, task, arg.clone());
                //result_sender.send(format!("{}   taskid:{}", arg, result)).expect("Failed to send result");
                let result = tw_clone.add_task(timestamp, delay, task, arg.clone())
                    .map(|id| format!("Task [{}] add successful", id)) // 成功时包装消息
                    .unwrap_or_else(|e| format!("Error: {}", e)); // 错误时转换为字符串

                // 发送结果（成功或错误）
                result_sender.send(result).expect("Failed to send result");
            }
        });
        
        // 驱动时间轮
        let tw_driver = time_wheel.clone();
        thread::spawn(move || {
            tw_driver.run();
        });

        //(Self { sender }, time_wheel)
        Self { sender }
    }

    // 新接口：支持传入字符串参数
    pub fn schedule<F>(
        &self, 
        timestamp: NaiveDateTime, 
        delay: Duration, 
        arg: String, 
        task: F
    ) -> Receiver<String>
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        let boxed_task = Box::new(task);

        // 创建临时通道，用于接收返回值
        let (result_sender, result_receiver) = channel();
        
        // 发送任务请求，附带 result_sender
        self.sender.send((timestamp, delay, boxed_task, arg, result_sender))
            .expect("Failed to send task");
        
        result_receiver
    }
}