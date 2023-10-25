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

struct TermMetadata {
    term_id: u32,
    doc_freq: u32,
    total_term_freq: u32,
    pointer: u64,
    num_blocks: u32,
    size_last_block: u32,
    last_doc_id: u32,
    total_bytes: u64,
    block_offsets: Vec<u64>,
    block_maxima: Vec<u32>,
}

pub fn build_bin_index(posting_path: &str, index_path: &str, lexicon_path: &str, directory_path: &str) -> std::io::Result<()> {
    let mut file = File::open(posting_path)?;
    let mut index_file = BufWriter::new(File::create(index_path)?);
    let mut lexicon_file = BufWriter::new(File::create(lexicon_path)?);
    let mut directory_file = BufWriter::new(File::create(directory_path)?);

    directory_file.write_u32::<LittleEndian>(0)?;

    let mut total_terms = 0;

    #[cfg(feature = "debug_unicode")]
    {
        let mut reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            total_terms += 1;

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
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        match bincode::deserialize::<Vec<(String, Vec<(u32, u32)>)>>(&buffer) {

            Ok(vec_data) => {
                for (term, postings) in vec_data {
                    total_terms += 1;
                    process_postings(&mut index_file, &mut lexicon_file, &mut directory_file, &term, postings, &mut total_terms)?;
                }
            },
            Err(e) => {
                eprintln!("Failed to deserialize binary data: {}", e);
            }
        }
    }

    directory_file.seek(SeekFrom::Start(0))?;
    directory_file.write_u32::<LittleEndian>(total_terms)?;

    Ok(())
}


fn process_postings(index_file: &mut BufWriter<File>, lexicon_file: &mut BufWriter<File>,
                    directory_file: &mut BufWriter<File>, term: &str, postings: Vec<(u32, u32)>,
                    total_terms: &mut u32) -> std::io::Result<()> {
    // Compress docIDs
    let docids: Vec<u32> = postings.iter().map(|&(docid, _)| docid).collect();
    let mut compressed_docids = vec![0u8; 5 * docids.len() + docids.len() / 4]; // Worst case scenario
    let bytes_written = encode::<Scalar>(&docids, &mut compressed_docids);
    compressed_docids.truncate(bytes_written); // Truncate to actual size

    // Write to index file
    index_file.write_all(&compressed_docids)?;

    // Write frequencies
    for &(_, freq) in &postings {
        index_file.write_u32::<LittleEndian>(freq)?;
    }

    // Construct TermMetadata
    let mut metadata = TermMetadata {
        term_id: *total_terms,
        doc_freq: postings.len() as u32,
        total_term_freq: postings.iter().map(|&(_, freq)| freq).sum(),
        pointer: index_file.stream_position()?,
        num_blocks: (postings.len() as f32 / BLOCK_SIZE as f32).ceil() as u32,
        size_last_block: (postings.len() % BLOCK_SIZE) as u32,
        last_doc_id: postings.last().unwrap().0,
        total_bytes: compressed_docids.len() as u64,
        block_offsets: Vec::new(),
        block_maxima: Vec::new(),
    };

    // Calculate block maxima and offsets
    for (i, posting) in postings.iter().enumerate() {
        if i % BLOCK_SIZE == 0 {
            metadata.block_offsets.push(index_file.stream_position()?);
        }
        if (i + 1) % BLOCK_SIZE == 0 || i + 1 == postings.len() {
            metadata.block_maxima.push(posting.0);
        }
    }

    // Write metadata to lexicon
    // Write the length of the term first
    lexicon_file.write_u32::<LittleEndian>(term.len() as u32)?;
    // Then write the term string
    lexicon_file.write_all(term.as_bytes())?;
    lexicon_file.write_u32::<LittleEndian>(metadata.term_id)?;
    lexicon_file.write_u32::<LittleEndian>(metadata.doc_freq)?;
    lexicon_file.write_u32::<LittleEndian>(metadata.total_term_freq)?;
    lexicon_file.write_u64::<LittleEndian>(metadata.pointer)?;
    lexicon_file.write_u32::<LittleEndian>(metadata.num_blocks)?;
    lexicon_file.write_u32::<LittleEndian>(metadata.size_last_block)?;
    lexicon_file.write_u32::<LittleEndian>(metadata.last_doc_id)?;
    lexicon_file.write_u64::<LittleEndian>(metadata.total_bytes)?;
    for &offset in &metadata.block_offsets {
        lexicon_file.write_u64::<LittleEndian>(offset)?;
    }
    for &max in &metadata.block_maxima {
        lexicon_file.write_u32::<LittleEndian>(max)?;
    }

    // Directory entry
    if *total_terms % DIRECTORY_NTH_TERM == 0 {
        // Write the length of the term first
        directory_file.write_u32::<LittleEndian>(term.len() as u32)?;
        // Then write the term string
        directory_file.write_all(term.as_bytes())?;
        directory_file.write_u64::<LittleEndian>(lexicon_file.stream_position()?)?;
    }

    Ok(())
}