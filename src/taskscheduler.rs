use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use arc_swap::ArcSwap;
use hashbrown::HashMap;
use chrono::{NaiveDateTime, Timelike, Utc};
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
    fn add_task(&self, timestamp: NaiveDateTime, delay: Duration, task: Task, arg: String) -> u64 {
        // 计算NaiveDateTime属于第几格  根据当天的秒数确定，比如当前时间9点45分50秒，属于  9*3600 + 45*60 + 50
        let current_seconds = timestamp.time().num_seconds_from_midnight() % (24 * 3600);
        let current_seconds =  current_seconds as usize;
        // 计算延迟的秒数
        let ticks = (delay.as_nanos() / self.tick_duration.as_nanos()) as usize;
        let target_slot = (current_seconds + ticks) % self.total_slots;
        println!("当前时间: {}, 目标槽: {}  msg:{}", current_seconds, target_slot, arg);
        let task_id = rand::random::<u64>();
        let slot = self.slots[target_slot].load();
        let mut tasks = slot.tasks.lock().unwrap();
        tasks.insert(task_id, (task, arg)); // 存储闭包和参数
        task_id
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
    sender: Sender<(NaiveDateTime, Duration, Task, String)>,
}

impl TaskScheduler {
    pub fn new(tick_duration: Duration, total_slots: usize) -> Self {
        let (sender, receiver) = mpsc::channel();
        let time_wheel = Arc::new(TimeWheel::new(tick_duration, total_slots));
        
        // 接收任务线程
        let tw_clone = time_wheel.clone();
        thread::spawn(move || {
            for (timestamp, delay, task, arg) in receiver {
                tw_clone.add_task(timestamp, delay, task, arg);
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
    pub fn schedule<F>(&self, timestamp: NaiveDateTime, delay: Duration, arg: String, task: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        let boxed_task = Box::new(task);
        self.sender.send((timestamp, delay, boxed_task, arg)).unwrap();
    }
}