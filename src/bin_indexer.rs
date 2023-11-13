extern crate serde;
extern crate serde_json;
extern crate stream_vbyte;
extern crate byteorder;
extern crate bincode;

use std::fs::File;
use std::io::Read;
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use byteorder::{LittleEndian, WriteBytesExt};
use serde_json::Value;

use stream_vbyte::{
    encode::encode,
    scalar::Scalar
};
use crate::utils::DIRECTORY_NTH_TERM;


const BLOCK_SIZE: usize = 64;

pub struct TermMetadata {
    pub(crate) term_id: u32,
    pub(crate) doc_freq: u32,
    pub(crate) total_term_freq: u32,
    pub(crate) term_start_pointer: u64,
    pub(crate) num_blocks: u32,
    pub(crate) num_posting_in_last_block: u32,
    pub(crate) last_doc_id: u32,
    pub(crate) compressed_docids_per_block: Vec<u64>,
    pub(crate) block_offsets: Vec<u64>,
    pub(crate) block_maxima: Vec<u32>,
}

pub fn build_bin_index(posting_path: &str, index_path: &str, lexicon_path: &str, directory_path: &str) -> std::io::Result<()> {
    let mut file = File::open(posting_path)?;
    let mut index_file = BufWriter::new(File::create(index_path)?);
    let mut lexicon_file = BufWriter::new(File::create(lexicon_path)?);
    let mut directory_file = BufWriter::new(File::create(directory_path)?);

    // A placeholder for the total number of terms processed
    directory_file.write_u32::<LittleEndian>(0)?;
    lexicon_file.write_u32::<LittleEndian>(0)?;

    let mut total_terms = 0;

    #[cfg(feature = "debug_unicode")]
    {
        let mut reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            let data: Value = serde_json::from_str(&line)?;
            let term = data[0].as_str().unwrap();
            let postings: Vec<(u32, u32)> = data[1].as_array().unwrap().iter().map(|x| {
                let docid = x[0].as_u64().unwrap() as u32;
                let freq = x[1].as_u64().unwrap() as u32;
                (docid, freq)
            }).collect();

            process_postings(&mut index_file, &mut lexicon_file, &mut directory_file, term, postings, &mut total_terms)?;
        }
    }

    #[cfg(not(feature = "debug_unicode"))]
    {
        let mut reader = BufReader::new(file);

        loop {
            // Read the length of the serialized tuple
            let mut length_buffer = [0u8; 8];
            if reader.read_exact(&mut length_buffer).is_err() {
                break; // Exit loop if we reach the end of the file or encounter any error
            }

            let length = u64::from_le_bytes(length_buffer);

            let mut buffer = vec![0u8; length as usize];
            if reader.read_exact(&mut buffer).is_err() {
                break; // Exit loop if we can't read the full serialized tuple or encounter any error
            }

            match bincode::deserialize::<(String, Vec<(u32, u32)>)>(&buffer) {
                Ok((term, postings)) => {
                    process_postings(&mut index_file, &mut lexicon_file, &mut directory_file, &term, postings, &mut total_terms)?;
                },
                Err(e) => {
                    eprintln!("(bin indexer) Failed to deserialize binary data: {}", e);
                }
            }
        }
    }

    lexicon_file.seek(SeekFrom::Start(0))?;
    lexicon_file.write_u32::<LittleEndian>(total_terms)?;

    let total_directories = (total_terms + DIRECTORY_NTH_TERM - 1) / DIRECTORY_NTH_TERM;
    directory_file.seek(SeekFrom::Start(0))?;
    directory_file.write_u32::<LittleEndian>(total_directories)?;
    Ok(())
}

// For each term
fn process_postings(
    index_file: &mut BufWriter<File>,
    lexicon_file: &mut BufWriter<File>,
    directory_file: &mut BufWriter<File>,
    term: &str,
    postings: Vec<(u32, u32)>,
    total_terms: &mut u32,
) -> std::io::Result<()> {

    // Add new directory entry when necessary
    if *total_terms % DIRECTORY_NTH_TERM == 0 {
        directory_file.write_u32::<LittleEndian>(term.len() as u32)?;
        directory_file.write_all(term.as_bytes())?;
        directory_file.write_u64::<LittleEndian>(lexicon_file.stream_position()?)?;
    }

    // Construct TermMetadata
    let mut metadata = TermMetadata {
        term_id: *total_terms,
        doc_freq: postings.len() as u32,
        total_term_freq: postings.iter().map(|&(_, freq)| freq).sum(),
        term_start_pointer: index_file.stream_position()?,
        num_blocks: (postings.len() as f32 / BLOCK_SIZE as f32).ceil() as u32,
        num_posting_in_last_block: (postings.len() % BLOCK_SIZE) as u32,
        last_doc_id: postings.last().unwrap().0,
        compressed_docids_per_block: Vec::new(),
        block_offsets: Vec::new(),
        block_maxima: Vec::new(),
    };


    // Processing each block
    for block in postings.chunks(BLOCK_SIZE) {
        let block_docids: Vec<u32> = block.iter().map(|&(docid, _)| docid).collect();
        let block_freqs: Vec<u32> = block.iter().map(|&(_, freq)| freq).collect();

        metadata.block_offsets.push(index_file.stream_position()?);
        metadata.block_maxima.push(*block_docids.last().unwrap());

        // Compress and write docids for the block
        let mut compressed_docids = vec![0u8; block_docids.len() * 5];
        let bytes_written = encode::<Scalar>(&block_docids, &mut compressed_docids);
        compressed_docids.truncate(bytes_written);
        index_file.write_all(&compressed_docids)?;

        metadata.compressed_docids_per_block.push(bytes_written as u64);

        // Write frequencies for the block
        for &freq in &block_freqs {
            index_file.write_u32::<LittleEndian>(freq)?;
        }

    }

    // Write metadata to lexicon
    lexicon_file.write_u32::<LittleEndian>(term.len() as u32)?;
    lexicon_file.write_all(term.as_bytes())?;
    lexicon_file.write_u32::<LittleEndian>(metadata.term_id)?;
    lexicon_file.write_u32::<LittleEndian>(metadata.doc_freq)?;
    lexicon_file.write_u32::<LittleEndian>(metadata.total_term_freq)?;
    lexicon_file.write_u64::<LittleEndian>(metadata.term_start_pointer)?;
    lexicon_file.write_u32::<LittleEndian>(metadata.num_blocks)?;
    lexicon_file.write_u32::<LittleEndian>(metadata.num_posting_in_last_block)?;
    lexicon_file.write_u32::<LittleEndian>(metadata.last_doc_id)?;
    for &size in &metadata.compressed_docids_per_block {
        lexicon_file.write_u64::<LittleEndian>(size)?;
    }
    for &offset in &metadata.block_offsets {
        lexicon_file.write_u64::<LittleEndian>(offset)?;
    }
    for &max in &metadata.block_maxima {
        lexicon_file.write_u32::<LittleEndian>(max)?;
    }


    *total_terms += 1;

    Ok(())
}
