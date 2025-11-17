use thiserror::Error;

/// 自定义错误类型，用于处理 CronTask 中的各种错误情况
#[derive(Debug, Error)]
pub enum CronTaskError {
    /// 初始化失败
    #[error("初始化失败: {0}")]
    InitFailed(String),
    
    /// 配置错误
    #[error("配置错误: {0}")]
    ConfigError(String),
    
    #[error("数据库错误: {0}")]
    DatabaseError(#[from] sqlx::Error),
    
    #[error("任务不存在: {0}")]
    TaskNotFound(String),
    
    #[error("任务已过期")]
    TaskPastDue,
    
    #[error("任务时间超出范围")]
    TaskTooFarInFuture,
    
    #[error("时间计算错误")]
    TimeCalculationError,
    
    #[error("发送任务请求失败")]
    TaskSendError,
    
    #[error("接收响应失败")]
    TaskRecvError,
    
    #[error("时间转换失败: {0}")]
    TimeConversionFailed(String),
    
    #[error("时间溢出")]
    TimeOverflow,
    
    #[error("任务已存在")]
    TaskAlreadyExists,
}
