use crate::common::error::CronTaskError;
use crate::scheduler::time_wheel::Task;
use chrono::NaiveDateTime;
use std::time::Duration;
use tokio::sync::oneshot;

pub enum TaskRequest {
    Add {
        time: NaiveDateTime,
        interval: Duration,
        key: String,
        arg: String,
        task: Task,
        resp: oneshot::Sender<Result<String, CronTaskError>>,
    },
    Cancel {
        time: NaiveDateTime,
        interval: Duration,
        key: String,
        resp: oneshot::Sender<Result<String, CronTaskError>>,
    },
}
