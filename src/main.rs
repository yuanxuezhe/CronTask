mod taskscheduler;
mod crontask;
mod task;
mod consts;
mod utils;

use tokio::signal;
use dbcore::Database;
use std::fs;
use task::model::Task;
use crate::crontask::core::CronTask;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    env_logger::init();
    
    let db_path = "evdata.db";
        
    // 确保数据库文件存在
    if !std::path::Path::new(db_path).exists() {
        fs::File::create(db_path)?;
    }

    let db = Database::new(db_path).await?;

    // 初始化表结构
    Task::init(&db).await?;

    // 创建任务管理器
    // 参数说明：重载间隔10秒，tick间隔1秒，总槽位86400，非高精度模式
    let cron_task = CronTask::new(10000, 1000, 86400, false, db);

    log::info!("时间轮运行中...");
    
    // 打印缓存中的任务列表
    let tasks = cron_task.get_all_tasks_from_cache().await;
    log::info!("缓存中的任务列表: {:?}", tasks);
    // 等待 Ctrl+C 信号
    signal::ctrl_c().await.expect("监听信号失败");
    log::info!("收到 Ctrl+C，停止所有任务...");
    
    Ok(())
}



    // let task1 = Task {
    //     taskid: 1,
    //     taskname: "Task1".to_string(),
    //     start_date: "20250512".to_string(),
    //     end_date: "20250612".to_string(),
    //     cycle_type: "0".to_string(),
    //     period: "D".to_string(),
    //     time_point: "133410".to_string(),
    //     retry_type: "1".to_string(),
    //     retry_interval: 10000,
    //     retry_count: 3,
    //     current_trigger_count: 0,
    //     status: "A".to_string(),
    // };

    // // 插入任务
    // match task1.insert(&db).await {
    //     Ok(affected_rows) => {
    //         // 插入成功，返回影响行数
    //         println!("effect rows:{}", affected_rows) // 可根据实际需求返回或处理
    //     },
    //     Err(e) => {
    //         // 插入失败，记录错误
    //         eprintln!("[ERROR] 定时任务插入失败: {}", e);
    //     }
    // }