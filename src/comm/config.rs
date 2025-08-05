use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub scheduler: SchedulerConfig,
    pub cron: CronConfig,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct SchedulerConfig {
    pub tick_interval_ms: u64,
    pub total_slots: usize,
}

#[derive(Debug, Deserialize)]
pub struct CronConfig {
    pub reload_interval_ms: u64,
    pub max_schedule_days: u32,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
    
    pub fn default() -> Self {
        Self {
            database: DatabaseConfig {
                path: "evdata.db".to_string(),
            },
            scheduler: SchedulerConfig {
                tick_interval_ms: 1000,
                total_slots: 86400
            },
            cron: CronConfig {
                reload_interval_ms: 10000,
                max_schedule_days: 10,
            },
        }
    }
}