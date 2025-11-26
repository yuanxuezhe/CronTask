// src/consts.rs
// 全局常量集中定义

// 日期时间格式
pub const DATE_FORMAT: &str = "%Y%m%d";
pub const TIME_FORMAT: &str = "%H%M%S";
pub const DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

// 周期类型
pub const CYCLE_TYPE_DAILY: &str = "0";
pub const CYCLE_TYPE_WORKDAY: &str = "1";
pub const CYCLE_TYPE_HOLIDAY: &str = "2";
pub const CYCLE_TYPE_CUSTOM: &str = "3";

// 任务调度状态（内部状态）
pub const TASK_STATUS_UNMONITORED: i32 = 0;
pub const TASK_STATUS_MONITORING: i32 = 1;
pub const TASK_STATUS_RETRY: i32 = 2;

// 任务标签
pub const TASK_TAG_DELETE: i32 = 0;
pub const TASK_TAG_KEEP: i32 = 1;
pub const TASK_TAG_NEW: i32 = 2;

// 其它
pub const TASK_KEY_SEPARATOR: &str = "|";
pub const RELOAD_TASK_NAME: &str = "__reload_tasks__";
