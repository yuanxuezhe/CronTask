use thiserror::Error;

/// 自定义错误类型，用于处理 CronTask 中的各种错误情况
#[derive(Debug, Error)]
pub enum CronTaskError {
    #[error("数据库错误: {0}")]
    DatabaseError(#[from] sqlx::Error),
    
    #[error("时间轮已停止")]
    TimeWheelStopped,
    
    #[error("时间轮未启动")]
    TimeWheelNotStarted,
    
    #[error("任务已存在")]
    TaskAlreadyExists,
    
    #[error("任务不存在: {0}")]
    TaskNotFound(String),
    
    #[error("任务详情不存在: {0}")]
    TaskDetailNotFound(String),
    
    #[error("时间计算错误")]
    TimeCalculationError,
    
    #[error("发送错误")]
    SendError,
    
    #[error("任务解析错误: {0}")]
    ParseError(String),
    
    #[error("其他错误: {0}")]
    Other(String),
    
    // 以下为原TaskSchedulerError的变体
    #[error("发送任务请求失败")]
    TaskSendError,
    
    #[error("接收响应失败")]
    TaskRecvError,
    
    #[error("时间溢出")]
    TimeOverflow,
    
    #[error("任务时间超出范围")]
    TaskTooFarInFuture,
    
    #[error("任务已过期")]
    TaskPastDue,
    
    #[error("时间转换失败: {0}")]
    TimeConversionFailed(String),
}