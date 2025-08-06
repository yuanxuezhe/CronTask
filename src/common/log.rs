//! 自定义日志宏，通过消息总线异步处理日志，避免日志量过大影响系统运行

use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;
use chrono::{Utc, DateTime};
use chrono_tz::Asia::Shanghai;

// 全局静态变量存储 CronTask 实例的指针
static CRON_TASK_PTR: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());

/// 设置全局 CronTask 实例
pub fn set_cron_task(task: &crate::core::cron_task::CronTask) {
    let ptr = task as *const crate::core::cron_task::CronTask as *mut u8;
    CRON_TASK_PTR.store(ptr, Ordering::Relaxed);
}

/// 获取全局 CronTask 实例
fn get_cron_task() -> Option<&'static crate::core::cron_task::CronTask> {
    let ptr = CRON_TASK_PTR.load(Ordering::Relaxed);
    if ptr.is_null() {
        None
    } else {
        unsafe {
            Some(&*(ptr as *const crate::core::cron_task::CronTask))
        }
    }
}

/// 发送trace级别日志消息
#[macro_export]
macro_rules! trace_log {
    ($($arg:tt)*) => {
        $crate::common::log::log_message(::log::Level::Trace, format!($($arg)*));
    };
}

/// 发送debug级别日志消息
#[macro_export]
macro_rules! debug_log {
    ($($arg:tt)*) => {
        $crate::common::log::log_message(::log::Level::Debug, format!($($arg)*));
    };
}

/// 发送info级别日志消息
#[macro_export]
macro_rules! info_log {
    ($($arg:tt)*) => {
        $crate::common::log::log_message(::log::Level::Info, format!($($arg)*));
    };
}

/// 发送warn级别日志消息
#[macro_export]
macro_rules! warn_log {
    ($($arg:tt)*) => {
        $crate::common::log::log_message(::log::Level::Warn, format!($($arg)*));
    };
}

/// 发送error级别日志消息
#[macro_export]
macro_rules! error_log {
    ($($arg:tt)*) => {
        $crate::common::log::log_message(::log::Level::Error, format!($($arg)*));
    };
}

/// 实际处理日志消息的函数
pub fn log_message(level: ::log::Level, message: String) {
    // 获取北京时间
    let utc_time: DateTime<Utc> = Utc::now();
    let beijing_time: DateTime<chrono_tz::Tz> = utc_time.with_timezone(&Shanghai);
    let time_str = beijing_time.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    
    let thread_id = format!("{:?}", std::thread::current().id());
    let pid = std::process::id() % 10000;
    // 提取线程ID中的数字部分
    let thread_num = thread_id.chars()
        .filter(|c| c.is_digit(10))
        .collect::<String>();
    let thread_short = thread_num.parse::<u64>().unwrap_or(0) % 10000;
    let message_with_thread = format!("[{:04}-{:04}] {}", pid, thread_short, message);
    
    // 尝试获取全局的 CRON_TASK 实例并发送日志消息
    if let Some(cron_task) = get_cron_task() {
        let _ = cron_task.message_bus.send(
            crate::message::message_bus::CronMessage::Log {
                level,
                message: message_with_thread,
            }
        );
    } else {
        // 如果无法获取 CRON_TASK，则直接打印到控制台，包含北京时间
        println!("[{}][{}] {}", time_str, level, message_with_thread);
    }
}