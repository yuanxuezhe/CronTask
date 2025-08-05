use chrono::NaiveDateTime;
use std::time::Duration;
use tokio::sync::oneshot;
use super::timewheel::Task;
use crate::comm::error::CronTaskError;

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