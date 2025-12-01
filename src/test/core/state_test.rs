use crate::core::state::InnerState;

#[test]
fn test_inner_state_creation() {
    // 创建一个InnerState实例
    let inner_state = InnerState {
        taskdetails: std::collections::HashMap::new(),
        tasks: std::collections::HashMap::new(),
    };
    
    // 验证InnerState实例的基本属性
    assert!(inner_state.taskdetails.is_empty());
    assert!(inner_state.tasks.is_empty());
}
