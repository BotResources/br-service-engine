use super::bind::Column;

pub struct RowScope {
    columns: Vec<Column>,
}

impl RowScope {
    pub fn by(column: Column) -> Self {
        Self {
            columns: vec![column],
        }
    }

    pub fn and(mut self, column: Column) -> Self {
        self.columns.push(column);
        self
    }

    pub fn whole_table() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub(super) fn columns(&self) -> &[Column] {
        &self.columns
    }
}
