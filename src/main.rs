mod taskscheduler;
mod crontask;
mod task;
mod comm;
mod bus;

use tokio::signal;
use dbcore::Database;
use std::fs;
use std::path::Path;
use task::model::Task;
use crate::crontask::core::CronTask;
use log::LevelFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志，设置日志级别为 Info
    // 自定义日志格式，移除模块路径
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "[{}][{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                record.args()
            )
        })
        .init();
    
    // 创建任务管理器以获取消息总线
    let db_path = "evdata.db";
    let db = Database::new(db_path).await?;

    // 创建任务管理器
    // 参数说明：重载间隔10秒，tick间隔1秒，总槽位86400，非高精度模式
    let cron_task = CronTask::new(10000, 1000, 86400, false, db);
    // 初始化加载任务
    cron_task.init_load_tasks().await;
    // 休眠3秒
    //tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    
    info_log!("初始化任务管理器");

    // 使用消息总线记录所有日志
    info_log!("数据库文件路径: {}", db_path);
        
    // 如果数据库文件不存在则创建它
    if !Path::new(db_path).exists() {
        info_log!("创建新数据库文件");
        fs::File::create(db_path)?;
    }

    // 连接数据库
    info_log!("连接数据库");
    let db = Database::new(db_path).await?;

    // 初始化表结构
    info_log!("初始化表结构");
    Task::init(&db).await?;

    info_log!("时间轮运行中...");
    
    // 打印缓存中的任务列表
    let tasks = cron_task.get_all_tasks_from_cache().await;
    info_log!("缓存中的任务列表: {:?}", tasks);

    // 等待 Ctrl+C 信号
    info_log!("等待终止信号");
    signal::ctrl_c().await.expect("监听信号失败");
    info_log!("收到 Ctrl+C，停止所有任务...");

    Ok(())
}