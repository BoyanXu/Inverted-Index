use regex::Regex;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;
use stop_words::{get, LANGUAGE::English}; // Import required components from stop-words crate
use std::collections::HashSet;
use lazy_static::lazy_static;
use std::sync::atomic::{AtomicUsize, Ordering};

// Cache the stop words for the English language
lazy_static! {
    static ref STOP_WORDS: HashSet<String> = {
        let words = get(English);
        words.into_iter().collect()
    };
}

static DOCID_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn parse_document(document: &str) -> (usize, String, Vec<String>) {
    let doc_id = get_doc_id(); // extract_doc_id(document);
    let url = extract_url(document);
    let text = extract_text_content(document);
    let tokens = parse_line(&text);

    (doc_id, url, tokens)
}

// fn extract_doc_id(document: &str) -> usize { // Change return type to usize
//     let re = Regex::new(r"(?s)<DOCNO>D(\d+)</DOCNO>").unwrap(); // Updated regex to directly match the number after 'D'
//     re.captures(document)
//         .and_then(|cap| cap.get(1))
//         .and_then(|m| m.as_str().parse::<usize>().ok()) // Parse the number to usize
//         .unwrap_or_default()
// }

fn get_doc_id() -> usize { // Change return type to usize
    DOCID_COUNTER.fetch_add(1, Ordering::SeqCst)
}


fn extract_url(document: &str) -> String {
    let text_content = extract_text_content(document);
    let first_line = text_content.lines().next().unwrap_or("");
    first_line.trim().to_string()
}

fn extract_text_content(document: &str) -> String {
    let re = Regex::new(r"(?s)<TEXT>(.*?)</TEXT>").unwrap(); // Note the (?s) flag
    re.captures(document)
        .and_then(|cap| cap.get(1))
        .map(|m| m.as_str().trim().to_string())
        .unwrap_or_default()
}

pub fn parse_line(line: &str) -> Vec<String> {
    // Normalize the text to NFKC (Normalization Form KC: Compatibility Composition)
    let normalized = line.nfkc().collect::<String>();

    // Replace "." and "_" and "-" with whitespace to treat them as delimiters
    let replaced = normalized.replace(".", " ")
        .replace("_", " ")
        .replace("-", " ");

    // Tokenize into words using unicode segmentation
    let words = replaced.unicode_words();

    // Convert to lowercase, filter out stop words and collect
    words.map(|word| word.to_lowercase())
        .filter(|word| !STOP_WORDS.contains(word))
        .filter(|word| {
            // Check if it's a number and if it's less than 10
            if let Ok(num) = word.parse::<f64>() {
                num >= 10.0
            } else {
                true // Keep non-numeric words
            }
        })
        .collect()
}
