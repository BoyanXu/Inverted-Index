use regex::Regex;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

pub fn parse_document(document: &str) -> (usize, String, Vec<String>) {
    let doc_id = extract_doc_id(document);
    let url = extract_url(document);
    let text = extract_text_content(document);
    let tokens = parse_line(&text);

    (doc_id, url, tokens)
}

fn extract_doc_id(document: &str) -> usize { // Change return type to usize
    let re = Regex::new(r"(?s)<DOCNO>D(\d+)</DOCNO>").unwrap(); // Updated regex to directly match the number after 'D'
    re.captures(document)
        .and_then(|cap| cap.get(1))
        .and_then(|m| m.as_str().parse::<usize>().ok()) // Parse the number to usize
        .unwrap_or_default()
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

    // Tokenize into words using unicode segmentation
    let words = normalized.unicode_words();

    // Convert to lowercase and collect
    words.map(|word| word.to_lowercase()).collect()
}
