use thiserror::Error;

#[derive(Debug, Error)]
pub enum TaskSchedulerError {
    #[error("发送任务请求失败")]
    SendError,
    #[error("接收响应失败")]
    RecvError,
    #[error("时间溢出")]
    TimeOverflow,
    #[error("任务时间超出范围")]
    TaskTooFarInFuture,
    #[error("任务已存在")]
    TaskAlreadyExists,
    #[error("任务已过期")]
    TaskPastDue,
    #[error("时间转换失败: {0}")]
    TimeConversionFailed(String),
}