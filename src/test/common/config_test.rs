use crate::common::config::Config;
use std::fs;

#[test]
fn test_config_default() {
    let config = Config::default();
    
    // 验证默认配置的基本结构
    assert_eq!(config.database.path, "evdata.db");
    assert_eq!(config.scheduler.tick_interval_ms, 1000);
    assert_eq!(config.scheduler.total_slots, 86400);
    assert_eq!(config.cron.reload_interval_ms, 10000);
    assert_eq!(config.logging.log_level, "info");
    assert_eq!(config.logging.log_file, None);
    assert_eq!(config.messaging.channel_buffer_size, 1000);
    assert_eq!(config.messaging.message_timeout_ms, 100);
    assert_eq!(config.messaging.test_timeout_ms, 10);
}

#[test]
fn test_config_validate() {
    let config = Config::default();
    
    // 验证默认配置是有效的
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_from_file() {
    // 测试从不存在的文件加载配置，应该返回默认配置
    let config = Config::from_file("non_existent_file.toml").unwrap();
    assert_eq!(config.database.path, "evdata.db");
}

#[test]
fn test_config_from_valid_file() {
    // 创建一个临时配置文件
    let config_content = r#"[database]
path = "test.db"

[scheduler]
tick_interval_ms = 500
total_slots = 43200

[cron]
reload_interval_ms = 5000

[logging]
log_level = "debug"
log_file = "test.log"

[messaging]
channel_buffer_size = 500
message_timeout_ms = 50
test_timeout_ms = 5
"#;
    
    fs::write("test_config.toml", config_content).unwrap();
    
    // 从文件加载配置
    let config = Config::from_file("test_config.toml").unwrap();
    
    // 验证配置加载正确
    assert_eq!(config.database.path, "test.db");
    assert_eq!(config.scheduler.tick_interval_ms, 500);
    assert_eq!(config.scheduler.total_slots, 43200);
    assert_eq!(config.cron.reload_interval_ms, 5000);
    assert_eq!(config.logging.log_level, "debug");
    assert_eq!(config.logging.log_file, Some("test.log".to_string()));
    assert_eq!(config.messaging.channel_buffer_size, 500);
    assert_eq!(config.messaging.message_timeout_ms, 50);
    assert_eq!(config.messaging.test_timeout_ms, 5);
    
    // 删除临时文件
    fs::remove_file("test_config.toml").unwrap();
}

#[test]
fn test_config_invalid_log_level() {
    // 创建一个包含无效日志级别的配置文件
    let config_content = r#"[logging]
log_level = "invalid_level"
"#;
    
    fs::write("test_config_invalid.toml", config_content).unwrap();
    
    // 从文件加载配置，应该返回错误
    let result = Config::from_file("test_config_invalid.toml");
    assert!(result.is_err());
    
    // 删除临时文件
    fs::remove_file("test_config_invalid.toml").unwrap();
}

#[test]
fn test_config_invalid_scheduler() {
    // 创建一个包含无效调度器配置的配置文件
    let config_content = r#"[scheduler]
tick_interval_ms = 0
total_slots = 0
"#;
    
    fs::write("test_config_invalid_scheduler.toml", config_content).unwrap();
    
    // 从文件加载配置，应该返回错误
    let result = Config::from_file("test_config_invalid_scheduler.toml");
    assert!(result.is_err());
    
    // 删除临时文件
    fs::remove_file("test_config_invalid_scheduler.toml").unwrap();
}

#[test]
fn test_config_partial_file() {
    // 创建一个只包含部分配置的文件
    let config_content = r#"[database]
path = "partial.db"

[logging]
log_level = "warn"
"#;
    
    fs::write("test_config_partial.toml", config_content).unwrap();
    
    // 从文件加载配置
    let config = Config::from_file("test_config_partial.toml").unwrap();
    
    // 验证部分配置被正确加载，其余使用默认值
    assert_eq!(config.database.path, "partial.db");
    assert_eq!(config.logging.log_level, "warn");
    // 验证其他配置项使用默认值
    assert_eq!(config.scheduler.tick_interval_ms, 1000);
    assert_eq!(config.cron.reload_interval_ms, 10000);
    
    // 删除临时文件
    fs::remove_file("test_config_partial.toml").unwrap();
}

#[test]
fn test_config_invalid_cron() {
    // 创建一个包含无效 cron 配置的配置文件
    let config_content = r#"[cron]
reload_interval_ms = 0
"#;
    
    fs::write("test_config_invalid_cron.toml", config_content).unwrap();
    
    // 从文件加载配置，应该返回错误
    let result = Config::from_file("test_config_invalid_cron.toml");
    assert!(result.is_err());
    
    // 删除临时文件
    fs::remove_file("test_config_invalid_cron.toml").unwrap();
}

#[test]
fn test_config_invalid_messaging() {
    // 创建一个包含无效 messaging 配置的配置文件
    let config_content = r#"[messaging]
channel_buffer_size = 0
message_timeout_ms = 0
"#;
    
    fs::write("test_config_invalid_messaging.toml", config_content).unwrap();
    
    // 从文件加载配置，应该返回错误
    let result = Config::from_file("test_config_invalid_messaging.toml");
    assert!(result.is_err());
    
    // 删除临时文件
    fs::remove_file("test_config_invalid_messaging.toml").unwrap();
}

#[test]
fn test_config_invalid_toml() {
    // 创建一个包含无效 TOML 格式的文件
    let config_content = r#"[database
path = "invalid.toml"
"#;
    
    fs::write("test_config_invalid_format.toml", config_content).unwrap();
    
    // 从文件加载配置，应该返回错误
    let result = Config::from_file("test_config_invalid_format.toml");
    assert!(result.is_err());
    
    // 删除临时文件
    fs::remove_file("test_config_invalid_format.toml").unwrap();
}

#[test]
fn test_config_all_log_levels() {
    // 测试所有有效的日志级别
    let valid_log_levels = ["error", "warn", "info", "debug", "trace"];
    
    for &level in valid_log_levels.iter() {
        let config_content = format!(r#"[logging]
log_level = "{level}"
"#);
        
        fs::write("test_config_log_level.toml", config_content).unwrap();
        
        let config = Config::from_file("test_config_log_level.toml").unwrap();
        assert!(config.validate().is_ok());
    }
    
    // 删除临时文件
    fs::remove_file("test_config_log_level.toml").unwrap();
}

#[test]
fn test_config_validate_individual_fields() {
    // 测试各个字段的验证逻辑
    let mut config = Config::default();
    
    // 测试无效的 tick_interval_ms
    config.scheduler.tick_interval_ms = 0;
    assert!(config.validate().is_err());
    config.scheduler.tick_interval_ms = 1000;
    
    // 测试无效的 total_slots
    config.scheduler.total_slots = 0;
    assert!(config.validate().is_err());
    config.scheduler.total_slots = 86400;
    
    // 测试无效的 reload_interval_ms
    config.cron.reload_interval_ms = 0;
    assert!(config.validate().is_err());
    config.cron.reload_interval_ms = 10000;
    
    // 测试无效的 log_level
    config.logging.log_level = "invalid".to_string();
    assert!(config.validate().is_err());
    config.logging.log_level = "info".to_string();
    
    // 测试无效的 channel_buffer_size
    config.messaging.channel_buffer_size = 0;
    assert!(config.validate().is_err());
    config.messaging.channel_buffer_size = 1000;
    
    // 测试无效的 message_timeout_ms
    config.messaging.message_timeout_ms = 0;
    assert!(config.validate().is_err());
    config.messaging.message_timeout_ms = 100;
    
    // 恢复所有有效配置，验证通过
    assert!(config.validate().is_ok());
}
