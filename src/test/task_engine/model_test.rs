use crate::task_engine::model::{Task, TaskDetail};

#[test]
fn test_task_detail_task_key() {
    // 创建一个TaskDetail实例
    let task_detail = TaskDetail {
        taskid: 123,
        timepoint: "2023-01-01 00:00:00".to_string(),
        current_trigger_count: 0,
        status: 0,
        tag: 0,
    };
    
    // 测试生成任务键
    let task_key = task_detail.task_key();
    assert_eq!(task_key, "123|2023-01-01 00:00:00");
}

#[test]
fn test_task_detail_update_status() {
    // 创建一个TaskDetail实例
    let mut task_detail = TaskDetail {
        taskid: 123,
        timepoint: "2023-01-01 00:00:00".to_string(),
        current_trigger_count: 0,
        status: 0,
        tag: 0,
    };
    
    // 测试更新状态
    task_detail.update_status(1);
    assert_eq!(task_detail.status, 1);
}

#[test]
fn test_task_detail_increment_trigger_count() {
    // 创建一个TaskDetail实例
    let mut task_detail = TaskDetail {
        taskid: 123,
        timepoint: "2023-01-01 00:00:00".to_string(),
        current_trigger_count: 0,
        status: 0,
        tag: 0,
    };
    
    // 测试增加触发次数
    task_detail.increment_trigger_count();
    assert_eq!(task_detail.current_trigger_count, 1);
    assert_eq!(task_detail.get_trigger_count(), 1);
}

#[test]
fn test_task_default() {
    // 创建一个默认的Task实例
    let task = Task::default();
    
    // 验证默认值
    assert_eq!(task.taskid, 0);
    assert_eq!(task.taskname, "");
    assert_eq!(task.start_date, "");
    assert_eq!(task.end_date, "");
    assert_eq!(task.cycle_type, "");
    assert_eq!(task.period, "");
    assert_eq!(task.time_point, "");
    assert_eq!(task.retry_type, "");
    assert_eq!(task.retry_interval, 0);
    assert_eq!(task.retry_count, 0);
    assert_eq!(task.status, "");
    assert_eq!(task.discribe, "");
}
