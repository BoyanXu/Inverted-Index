mod decompressor;
mod parser;
mod indexer;
mod compressor;
mod unicode_handler;
mod utils;
use std::io::BufRead;

fn process_gzip_file(file_path: &str) -> std::io::Result<()> {
    let reader = decompressor::decompress_gzip_file(file_path)?;
    let mut indexer = indexer::Indexer::new();

    for (doc_count, line) in reader.lines().enumerate() {
        let line = line?;
        if utils::DEBUG_MODE && doc_count > utils::DEBUG_DOC_LIMIT {
            break;
        }

        indexer.process_document(&line);
    }

    Ok(())
}

fn main() {
    let file_path = "data/msmarco-docs.trec.gz";
    if let Err(e) = process_gzip_file(file_path) {
        eprintln!("Error processing file: {}", e);
    }

}
