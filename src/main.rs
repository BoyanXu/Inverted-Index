mod decompressor;
mod parser;
mod indexer;
mod compressor;
mod unicode_handler;
mod utils;

fn process_gzip_file(file_path: &str) -> std::io::Result<()> {
    let reader = decompressor::decompress_gzip_file(file_path)?;
    let mut indexer = indexer::Indexer::new();
    let mut doc_count = 0;

    for line in reader.lines() {
        let line = line?;
        if utils::DEBUG_MODE && doc_count > utils::DEBUG_DOC_LIMIT {
            break;
        }

        indexer.process_document(&line);
        doc_count += 1;
    }

    Ok(())
}

fn main() {
    let file_path = "msmarco-docs.trec.gz";
    if let Err(e) = process_gzip_file(file_path) {
        eprintln!("Error processing file: {}", e);
    }
}
