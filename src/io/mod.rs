use std::sync::{atomic::AtomicUsize, Arc};

struct StartEnd(pub (usize, usize));


/// 存储序列化的对象，核心实现是二级存储
pub struct GasFile {
    fname: Arc<String>,
    threads: usize,

    current_position: Arc<AtomicUsize>,

}


