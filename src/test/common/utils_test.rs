use crate::common::utils::gen_task_key;

#[test]
fn test_gen_task_key() {
    let task_id = 123;
    let timepoint = "2023-01-01 00:00:00";
    
    let task_key = gen_task_key(task_id, timepoint);
    
    // 验证生成的任务键格式正确
    assert_eq!(task_key, format!("{}{}{}", task_id, crate::common::consts::TASK_KEY_SEPARATOR, timepoint));
}
