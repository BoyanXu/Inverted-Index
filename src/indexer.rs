use std::collections::HashMap;

pub struct Indexer {
    pub term_to_id: HashMap<String, u32>,
    pub current_term_id: u32,
}

impl Indexer {
    pub fn new() -> Self {
        Self {
            term_to_id: HashMap::new(),
            current_term_id: 0,
        }
    }

    pub fn process_document(&mut self, document: &str) {
        let tokens = crate::parser::parse_line(document);
        for token in tokens {
            if !self.term_to_id.contains_key(&token) {
                self.term_to_id.insert(token, self.current_term_id);
                self.current_term_id += 1;
            }
        }
    }
}
