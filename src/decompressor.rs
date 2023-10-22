use std::fs::File;
use std::io::{BufReader, Result, BufRead};
use flate2::read::GzDecoder;

pub fn decompress_gzip_file(file_path: &str) -> Result<impl BufRead> {
    let file = File::open(file_path)?;
    let decoder = GzDecoder::new(file);
    Ok(BufReader::new(decoder))
}
