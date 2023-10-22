use std::fs::File;
use std::io::{BufReader, Result, BufRead};
use flate2::read::GzDecoder;

pub fn decompress_gzip_file(file_path: &str) -> Result<Box<dyn BufRead>> {
    let file = File::open(file_path)?;
    let decoder = GzDecoder::new(file);
    Ok(Box::new(BufReader::new(decoder)))
}
