use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::io::BufRead;
use chrono::Utc;


#[cfg(feature = "debug_unicode")]
use serde_json;

#[cfg(not(feature = "debug_unicode"))]
use bincode;
use crate::{decompressor, indexer, utils};

const BATCH_SIZE: usize = 100; // number of documents to process before dumping to disk

pub fn process_gzip_file(file_path: &str) -> std::io::Result<()> {
    let reader = decompressor::decompress_gzip_file(file_path)?;
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

pub fn write_to_disk(postings: &HashMap<u32, HashMap<usize, u32>>) {
    // Get the current timestamp
    let current_time = Utc::now();
    let filename = format!("postings_{}.data", current_time.format("%Y%m%d%H%M%S%f"));

    // Path to store the postings
    let path = Path::new("postings_data").join(filename);

    // Create the directory if it doesn't exist
    std::fs::create_dir_all(path.parent().unwrap()).expect("Failed to create directory");

    #[cfg(feature = "debug_unicode")]
    {
        // For debugging: save as a readable JSON file
        let serialized_data = serde_json::to_string_pretty(&postings).expect("Failed to serialize postings as JSON");
        let mut file = File::create(&path).expect("Failed to create file");
        file.write_all(serialized_data.as_bytes()).expect("Failed to write to file");
    }

    #[cfg(not(feature = "debug_unicode"))]
    {
        // Production: save as binary format
        let serialized_data = bincode::serialize(&postings).expect("Failed to serialize postings");
        let mut file = File::create(&path).expect("Failed to create file");
        file.write_all(&serialized_data).expect("Failed to write to file");
    }
}

pub fn write_lexicon_to_disk(lexicon: &HashMap<String, u32>) {
    // Sort the lexicon
    let mut sorted_terms: Vec<_> = lexicon.iter().collect();
    sorted_terms.sort_by(|a, b| a.0.cmp(b.0));

    // Path to store the lexicon
    let path = Path::new("data").join("lexicon.data");
    std::fs::create_dir_all(path.parent().unwrap()).expect("Failed to create directory");

    #[cfg(feature = "debug_unicode")]
    {
        let serialized_data = serde_json::to_string_pretty(&sorted_terms).expect("Failed to serialize lexicon as JSON");
        let mut file = File::create(&path).expect("Failed to create file");
        file.write_all(serialized_data.as_bytes()).expect("Failed to write to file");
    }

    #[cfg(not(feature = "debug_unicode"))]
    {
        let serialized_data = bincode::serialize(&sorted_terms).expect("Failed to serialize lexicon");
        let mut file = File::create(&path).expect("Failed to create file");
        file.write_all(&serialized_data).expect("Failed to write to file");
    }
}

pub fn write_doc_metadata_to_disk(metadata: &HashMap<usize, (String, u32)>) {
    // Path to store the document metadata
    let path = Path::new("data").join("doc_metadata.data");
    std::fs::create_dir_all(path.parent().unwrap()).expect("Failed to create directory");

    #[cfg(feature = "debug_unicode")]
    {
        let serialized_data = serde_json::to_string_pretty(metadata).expect("Failed to serialize doc_metadata as JSON");
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
