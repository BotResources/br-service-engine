use std::collections::VecDeque;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Block {
    pub event: Option<String>,
    pub data: Option<String>,
    pub id: Option<String>,
    pub comments: Vec<String>,
}

impl Block {
    pub fn is_comment_only(&self) -> bool {
        self.event.is_none() && self.data.is_none() && self.id.is_none()
    }

    fn is_empty(&self) -> bool {
        self.is_comment_only() && self.comments.is_empty()
    }
}

#[derive(Default)]
pub struct EventStreamParser {
    partial_utf8: Vec<u8>,
    line: String,
    after_carriage_return: bool,
    current: Block,
    data_lines: Vec<String>,
    ready: VecDeque<Block>,
}

impl EventStreamParser {
    pub fn push(&mut self, bytes: &[u8]) {
        self.partial_utf8.extend_from_slice(bytes);
        let complete_len = match std::str::from_utf8(&self.partial_utf8) {
            Ok(_) => self.partial_utf8.len(),
            Err(error) if error.error_len().is_none() => error.valid_up_to(),
            Err(error) => panic!("the event stream carries invalid UTF-8: {error}"),
        };
        let complete: Vec<u8> = self.partial_utf8.drain(..complete_len).collect();
        for c in std::str::from_utf8(&complete)
            .expect("validated above")
            .chars()
        {
            match c {
                '\r' => {
                    self.end_line();
                    self.after_carriage_return = true;
                }
                '\n' if self.after_carriage_return => self.after_carriage_return = false,
                '\n' => self.end_line(),
                c => {
                    self.after_carriage_return = false;
                    self.line.push(c);
                }
            }
        }
    }

    pub fn next_block(&mut self) -> Option<Block> {
        self.ready.pop_front()
    }

    pub fn has_unterminated_input(&self) -> bool {
        !self.partial_utf8.is_empty()
            || !self.line.is_empty()
            || !self.current.is_empty()
            || !self.data_lines.is_empty()
    }

    pub fn unterminated(&self) -> String {
        format!(
            "{:?} {:?} {:?} {}",
            self.current,
            self.data_lines,
            self.line,
            String::from_utf8_lossy(&self.partial_utf8)
        )
    }

    fn end_line(&mut self) {
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            self.dispatch();
            return;
        }
        if let Some(comment) = line.strip_prefix(':') {
            self.current.comments.push(comment.to_string());
            return;
        }
        let (field, value) = line.split_once(':').unwrap_or((line.as_str(), ""));
        let value = value.strip_prefix(' ').unwrap_or(value).to_string();
        match field {
            "event" => self.current.event = Some(value),
            "data" => self.data_lines.push(value),
            "id" if !value.contains('\0') => self.current.id = Some(value),
            _ => {}
        }
    }

    fn dispatch(&mut self) {
        if !self.data_lines.is_empty() {
            self.current.data = Some(std::mem::take(&mut self.data_lines).join("\n"));
        }
        let block = std::mem::take(&mut self.current);
        if !block.is_empty() {
            self.ready.push_back(block);
        }
    }
}
