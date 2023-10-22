use std::collections::HashMap;
use crate::disk_io;

pub struct Indexer {
    // Temporary postings: (token_ID, {document_ID: frequency})
    postings: HashMap<u32, HashMap<usize, u32>>,

    // Metadata about the documents: (docID, (URL, number_of_terms))
    doc_metadata: HashMap<usize, (String, u32)>,

    // Mapping from terms to IDs
    term_to_id: HashMap<String, u32>,
    current_term_id: u32,
}

impl Indexer {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            doc_metadata: HashMap::new(),
            term_to_id: HashMap::new(),
            current_term_id: 0,
        }
    }

    pub fn process_document(&mut self, document: &str) {
        // Parsing the document to get docID, URL, and tokens
        let (doc_id, url, tokens) = crate::parser::parse_document(document);

        // Update doc_metadata
        self.doc_metadata.insert(doc_id.clone(), (url, tokens.len() as u32));

        let mut token_freq = HashMap::new();
        for token in &tokens {
            *token_freq.entry(token).or_insert(0) += 1;
        }

        for (token, freq) in token_freq {
            let term_id = *self.term_to_id.entry(token.to_string()).or_insert_with(|| {
                let id = self.current_term_id;
                self.current_term_id += 1;
                id
            });

            let doc_freq = self.postings.entry(term_id)
                .or_insert_with(HashMap::new)
                .entry(doc_id.clone())
                .or_insert(0);
            *doc_freq += freq;
        }
    }

    // This function will write temporary postings to disk and then clear them.
    pub fn dump_postings_to_disk(&mut self) {
        // Placeholder for the disk writing functionality
        for (term_id, postings) in &self.postings {
            disk_io::write_to_disk(*term_id, postings);
        }
        self.postings.clear();
    }

}