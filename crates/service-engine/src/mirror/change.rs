use crate::nats::KvKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct Change {
    pub prefix: &'static str,
    pub key: KvKey,
    pub op: ChangeOp,
}

impl Change {
    pub fn is_delete(&self) -> bool {
        matches!(self.op, ChangeOp::Delete)
    }
}
