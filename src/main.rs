mod taskscheduler;

use std::time::Duration;
#[allow(unused_imports)]
use chrono::{NaiveDateTime, Utc};
use chrono_tz::Asia::Shanghai;
use tokio::signal;
use taskscheduler::TaskScheduler;

#[tokio::main]
async fn main() {
    // 定义一天的时间轮
    let scheduler = TaskScheduler::new(Duration::from_secs(1), 86400);

    // 示例：添加带不同字符串参数的任务
    for i in 0..10 {
        let eventdata = format!("Task-{}", i);
        let result_receiver = scheduler.schedule(
           //timestamp_to_datetime("20250430152300", "%Y%m%d%H%M%S"),
           //NaiveDateTime::parse_from_str("20250429110800", "%Y%m%d%H%M%S").unwrap(),
           Utc::now().with_timezone(&Shanghai).naive_local(),
            Duration::from_secs(i as u64),
            eventdata.clone(),
            move |eventdata| {
                let now = Utc::now().with_timezone(&Shanghai);
                //println!("[{}] 执行任务: {}", now.format("%Y%m%d %H:%M:%S"), eventdata);
                println!("[{}] 执行任务: {}", now, eventdata);
            },
        );

        match result_receiver.recv() {
            Ok(msg) => println!("Result: {}", msg), // 直接打印消息
            Err(e) => println!("Channel error: {}", e),
        }
    }

    // 示例：添加带不同字符串参数的任务
    for i in 0..10 {
        let eventdata = format!("Task-{}", i);
        let result_receiver = scheduler.schedule(
           //timestamp_to_datetime("20250430152300", "%Y%m%d%H%M%S"),
           NaiveDateTime::parse_from_str("20250508110800", "%Y%m%d%H%M%S").unwrap(),
           //Utc::now().with_timezone(&Shanghai).naive_local(),
            Duration::from_secs(i as u64),
            eventdata.clone(),
            move |eventdata| {
                let now = Utc::now().with_timezone(&Shanghai);
                //println!("[{}] 执行任务: {}", now.format("%Y%m%d %H:%M:%S"), eventdata);
                println!("[{}] 执行任务: {}", now, eventdata);
            },
        );

        match result_receiver.recv() {
            Ok(msg) => println!("Result: {}", msg), // 直接打印消息
            Err(e) => println!("Channel error: {}", e),
        }
    }

    println!("时间轮运行中...");
    // 等待 Ctrl+C 信号
    signal::ctrl_c().await.expect("监听信号失败");
    println!("收到 Ctrl+C，停止所有任务...");
}