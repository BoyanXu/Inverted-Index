mod decompressor;
mod parser;
mod indexer;
mod compressor;
mod unicode_handler;
mod utils;
mod disk_io;

use std::fs;
use std::path::Path;
use disk_io::process_gzip_file;

// Function to clean up the postings_data folder
fn cleanup_postings_data_folder() -> std::io::Result<()> {
    let dir = Path::new("postings_data");
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    Ok(())
}

fn main() {
    // Clean up postings_data folder if debug_unicode feature is enabled
    #[cfg(feature = "debug_unicode")]
    {
        if let Err(e) = cleanup_postings_data_folder() {
            eprintln!("Error cleaning up postings_data folder: {}", e);
        }
    }

    let file_path = "data/msmarco-docs.trec.gz";
    if let Err(e) = process_gzip_file(file_path) {
        eprintln!("Error processing file: {}", e);
    }
}
