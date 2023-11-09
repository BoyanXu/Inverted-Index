use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Result, BufRead};
use flate2::read::GzDecoder;
use bimap::BiMap;
use std::io::Write;
use std::path::Path;
use chrono::Utc;
use crate::external_sorter::merge_sorted_files;
use std::fs::read_dir;

#[cfg(feature = "debug_unicode")]

#[cfg(not(feature = "debug_unicode"))]
use bincode;
use crate::{indexer, utils};
use crate::utils::BATCH_SIZE; // number of documents to process before dumping to disk

pub fn decompress_gzip_file(file_path: &str) -> Result<Box<dyn BufRead>> {
    let file = File::open(file_path)?;
    let decoder = GzDecoder::new(file);
    Ok(Box::new(BufReader::new(decoder)))
}

pub fn process_gzip_file(file_path: &str) -> std::io::Result<()> {
    let reader = decompress_gzip_file(file_path)?;
    let mut indexer = indexer::Indexer::new();

    let mut current_doc = Vec::new();
    let mut doc_count = 0;

    for line in reader.lines() {
        let line = line?;
        current_doc.push(line.clone());

        if line.contains("</DOC>") {
            let full_doc = current_doc.join("\n");
            indexer.process_document(&full_doc);

            // If we've reached our batch size, dump to disk and clear the current postings.
            doc_count += 1;
            if doc_count % BATCH_SIZE == 0 {
                indexer.dump_postings_to_disk();
            }

            // Clear th e current doc for the next one.
            current_doc.clear();
        }

        if utils::DEBUG_MODE && doc_count > utils::DEBUG_DOC_LIMIT {
            break;
        }
    }

    // After processing all documents, dump any remaining postings that didn't reach the next batch size.
    if !current_doc.is_empty() {
        let full_doc = current_doc.join("\n");
        indexer.process_document(&full_doc);
        indexer.dump_postings_to_disk();
    }

    indexer.dump_lexicon_to_disk();
    indexer.dump_doc_metadata_to_disk();

    Ok(())
}

pub fn write_posting_to_disk(postings: &HashMap<u32, HashMap<u32, u32>>, term_id_map: &BiMap<String, u32>) {
    // Create a vector of term_string, postings_list pairs
    let mut postings_with_terms: Vec<_> = postings.iter()
        .filter_map(|(&token_id, postings_list)| {
            term_id_map.get_by_right(&token_id).map(|term_string| (term_string.clone(), postings_list.clone()))
        })
        .collect();

    // Sort the vector based on term_string
    postings_with_terms.sort_by_key(|(term_string, _)| term_string.clone());

    // Get the current timestamp
    let current_time = Utc::now();
    let filename = format!("postings_{}.data", current_time.format("%Y%m%d%H%M%S%f"));

    // Path to store the postings
    let path = Path::new("postings_data").join(filename);

    // Create the directory if it doesn't exist
    std::fs::create_dir_all(path.parent().unwrap()).expect("Failed to create directory");

    #[cfg(feature = "debug_unicode")]
    {
        // For debugging: save as a readable JSON file, one line per tuple
        let mut file = File::create(&path).expect("Failed to create file");
        for entry in &postings_with_terms {
            let serialized_data = serde_json::to_string(entry).expect("Failed to serialize posting with term as JSON");
            file.write_all(serialized_data.as_bytes()).expect("Failed to write to file");
            file.write_all(b"\n").expect("Failed to write newline");
        }
    }

    #[cfg(not(feature = "debug_unicode"))]
    {
        // Production: save as binary format, one tuple at a time
        let mut file = File::create(&path).expect("Failed to create file");
        for entry in &postings_with_terms {
            let serialized_data = bincode::serialize(entry).expect("Failed to serialize posting with term");

            // Write the length of serialized data first
            file.write_all(&(serialized_data.len() as u64).to_le_bytes()).expect("Failed to write data length");
            file.write_all(&serialized_data).expect("Failed to write to file");
        }
    }
}



pub fn write_lexicon_to_disk(lexicon: &BiMap<String, u32>) {
    // Sort the lexicon based on the terms (left values)
    let mut sorted_terms: Vec<_> = lexicon.left_values().cloned().collect();
    sorted_terms.sort();

    // Convert the sorted terms into a Vec<(String, u32)>
    let terms_with_ids: Vec<(String, u32)> = sorted_terms.iter()
        .map(|term| (term.clone(), *lexicon.get_by_left(term).unwrap()))
        .collect();

    // Path to store the lexicon
    let path = Path::new("data").join("lexicon.data");
    std::fs::create_dir_all(path.parent().unwrap()).expect("Failed to create directory");

    #[cfg(feature = "debug_unicode")]
    {
        let serialized_data = serde_json::to_string(&terms_with_ids).expect("Failed to serialize lexicon as JSON");
        let mut file = File::create(&path).expect("Failed to create file");
        file.write_all(serialized_data.as_bytes()).expect("Failed to write to file");
    }

    #[cfg(not(feature = "debug_unicode"))]
    {
        let serialized_data = bincode::serialize(&terms_with_ids).expect("Failed to serialize lexicon");
        let mut file = File::create(&path).expect("Failed to create file");
        file.write_all(&serialized_data).expect("Failed to write to file");
    }
}

pub fn write_doc_metadata_to_disk(metadata: &HashMap<u32, (String, u32)>) {
    // Path to store the document metadata
    let path = Path::new("data").join("doc_metadata.data");
    std::fs::create_dir_all(path.parent().unwrap()).expect("Failed to create directory");

    #[cfg(feature = "debug_unicode")]
    {
        let serialized_data = serde_json::to_string(metadata).expect("Failed to serialize doc_metadata as JSON");
        let mut file = File::create(&path).expect("Failed to create file");
        file.write_all(serialized_data.as_bytes()).expect("Failed to write to file");
    }

    #[cfg(not(feature = "debug_unicode"))]
    {
        let serialized_data = bincode::serialize(metadata).expect("Failed to serialize doc_metadata");
        let mut file = File::create(&path).expect("Failed to create file");
        file.write_all(&serialized_data).expect("Failed to write to file");
    }
}


pub fn merge_sorted_postings() -> std::io::Result<()> {
    let dir = Path::new("postings_data");
    let output_dir = Path::new("data");

    // Get all batches (files) in the postings_data directory
    let files: Vec<_> = read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .collect();

    // Merge these batches into the desired output directory
    let merged_output_path = output_dir.join("merged_postings.data");
    merge_sorted_files(&merged_output_path.to_string_lossy(), files)
}
