// 标准库导入
use std::fs;
use std::path::Path;

// 外部 crate 导入
use tokio::signal;

// 内部模块导入
mod common;
mod core;
mod message;
mod scheduler;
mod task_engine;

// 内部模块使用声明
use crate::common::config::Config;
use crate::core::core::CronTask;
use crate::task_engine::model::Task;

// 外部 crate 使用声明
use dbcore::Database;
use ::log::LevelFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志系统
    init_logger();
    
    // 加载配置
    let config = load_config();
    
    // 初始化数据库
    let db = init_database(&config).await?;
    
    // 创建并启动任务管理器
    let cron_task = start_task_manager(&config, db).await?;
    
    // 等待终止信号
    await_termination_signal(cron_task).await?;

    Ok(())
}

/// 初始化日志系统
fn init_logger() {
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
}

/// 加载配置文件
fn load_config() -> Config {
    Config::from_file("config.toml").unwrap_or_else(|e| {
        eprintln!("警告: 无法加载配置文件 config.toml: {}，使用默认配置", e);
        Config::default()
    })
}

/// 初始化数据库连接
async fn init_database(config: &Config) -> Result<Database, Box<dyn std::error::Error>> {
    let db_path = config.database.path.clone();
    info_log!("数据库文件路径: {}", db_path);
    
    // 如果数据库文件不存在则创建它
    if !Path::new(&db_path).exists() {
        info_log!("创建新数据库文件");
        fs::File::create(&db_path)?;
    }

    // 连接数据库
    info_log!("连接数据库");
    let db = Database::new(&db_path).await?;

    // 初始化表结构
    info_log!("初始化表结构");
    Task::init(&db).await?;
    
    Ok(db)
}

/// 启动任务管理器
async fn start_task_manager(config: &Config, db: Database) -> Result<std::sync::Arc<CronTask>, Box<dyn std::error::Error>> {
    // 创建任务管理器
    let cron_task = CronTask::new(
        config.cron.reload_interval_ms,
        config.scheduler.tick_interval_ms,
        config.scheduler.total_slots,
        db
    );
    
    // 初始化加载任务
    cron_task.init_load_tasks().await;
    
    info_log!("初始化任务管理器");
    info_log!("时间轮运行中...");
    
    Ok(cron_task)
}

/// 等待终止信号并处理
async fn await_termination_signal(_cron_task: std::sync::Arc<CronTask>) -> Result<(), Box<dyn std::error::Error>> {
    // 等待 Ctrl+C 信号
    info_log!("等待终止信号");
    signal::ctrl_c().await.expect("监听信号失败");
    info_log!("收到 Ctrl+C，停止所有任务...");
    
    // 可以在这里添加清理逻辑
    
    Ok(())
}