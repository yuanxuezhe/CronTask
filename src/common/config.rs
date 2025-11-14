use serde::Deserialize;
use crate::common::error::CronTaskError;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub database: DatabaseConfig,
    pub scheduler: SchedulerConfig,
    pub cron: CronConfig,
    pub logging: LoggingConfig,
    pub messaging: MessagingConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SchedulerConfig {
    pub tick_interval_ms: u64,
    pub total_slots: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CronConfig {
    pub reload_interval_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    pub log_level: String,
    pub log_file: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MessagingConfig {
    pub channel_buffer_size: usize,
    pub message_timeout_ms: u64,
    pub test_timeout_ms: u64,
}

impl Config {
    /// 从配置文件加载配置，使用更智能的合并策略
    pub fn from_file(path: &str) -> Result<Self, CronTaskError> {
        let default_config = Self::default();
        
        // 尝试读取配置文件
        let file_contents = match std::fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(_) => {
                // 文件不存在，返回默认配置
                return Ok(default_config);
            }
        };
        
        // 将配置文件内容解析为 TOML 值的哈希表
        let parsed_toml: toml::Value = toml::from_str(&file_contents)
            .map_err(|e| CronTaskError::Other(format!("解析配置文件失败: {}", e)))?;
        
        // 合并配置：配置文件存在的字段使用配置值，不存在的使用默认值
        let merged_config = Self::merge_configs(&default_config, &parsed_toml)?;
        
        merged_config.validate()?;
        Ok(merged_config)
    }
    
    /// 智能合并配置：配置文件存在的字段使用配置值，不存在的使用默认值
    fn merge_configs(default: &Config, file_toml: &toml::Value) -> Result<Config, CronTaskError> {
        let mut merged = default.clone();
        
        // 检查并合并数据库配置
        if let Some(db_table) = file_toml.get("database") {
            if let Some(path) = db_table.get("path").and_then(|v| v.as_str()) {
                merged.database.path = path.to_string();
            }
        }
        
        // 检查并合并调度器配置
        if let Some(scheduler_table) = file_toml.get("scheduler") {
            if let Some(tick_ms) = scheduler_table.get("tick_interval_ms").and_then(|v| v.as_integer()) {
                merged.scheduler.tick_interval_ms = tick_ms as u64;
            }
            if let Some(slots) = scheduler_table.get("total_slots").and_then(|v| v.as_integer()) {
                merged.scheduler.total_slots = slots as usize;
            }
        }
        
        // 检查并合并 Cron 配置
        if let Some(cron_table) = file_toml.get("cron") {
            if let Some(reload_ms) = cron_table.get("reload_interval_ms").and_then(|v| v.as_integer()) {
                merged.cron.reload_interval_ms = reload_ms as u64;
            }
        }
        
        // 检查并合并日志配置
        if let Some(logging_table) = file_toml.get("logging") {
            if let Some(log_level) = logging_table.get("log_level").and_then(|v| v.as_str()) {
                merged.logging.log_level = log_level.to_string();
            }
            if let Some(log_file) = logging_table.get("log_file").and_then(|v| v.as_str()) {
                merged.logging.log_file = Some(log_file.to_string());
            }
        }
        
        // 检查并合并消息配置
        if let Some(messaging_table) = file_toml.get("messaging") {
            if let Some(buffer_size) = messaging_table.get("channel_buffer_size").and_then(|v| v.as_integer()) {
                merged.messaging.channel_buffer_size = buffer_size as usize;
            }
            if let Some(timeout_ms) = messaging_table.get("message_timeout_ms").and_then(|v| v.as_integer()) {
                merged.messaging.message_timeout_ms = timeout_ms as u64;
            }
            if let Some(test_timeout) = messaging_table.get("test_timeout_ms").and_then(|v| v.as_integer()) {
                merged.messaging.test_timeout_ms = test_timeout as u64;
            }
        }
        
        Ok(merged)
    }
    
    /// 获取默认配置
    pub fn default() -> Self {
        Self {
            database: DatabaseConfig {
                path: "evdata.db".to_string(),
            },
            scheduler: SchedulerConfig {
                tick_interval_ms: 1000,
                total_slots: 86400,
            },
            cron: CronConfig {
                reload_interval_ms: 10000,
            },
            logging: LoggingConfig {
                log_level: "info".to_string(),
                log_file: None,
            },
            messaging: MessagingConfig {
                channel_buffer_size: 1000,
                message_timeout_ms: 100,
                test_timeout_ms: 10,
            },
        }
    }
    
    /// 验证配置的有效性
    pub fn validate(&self) -> Result<(), CronTaskError> {
        if self.scheduler.tick_interval_ms == 0 {
            return Err(CronTaskError::Other("调度器时间间隔不能为0".to_string()));
        }
        
        if self.scheduler.total_slots == 0 {
            return Err(CronTaskError::Other("时间轮总槽数不能为0".to_string()));
        }
        
        Ok(())
    }
}