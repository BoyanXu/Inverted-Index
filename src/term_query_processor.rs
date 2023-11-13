extern crate stream_vbyte;
extern crate byteorder;

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use byteorder::{LittleEndian, ReadBytesExt};
use stream_vbyte::decode::decode;
use stream_vbyte::scalar::Scalar;
use crate::utils::DIRECTORY_NTH_TERM;
use crate::bin_indexer::TermMetadata;

const BLOCK_SIZE: usize = 64;

pub(crate) fn query_term_postings(term: &str, index_path: &str, lexicon_path: &str, directory_path: &str) -> Result<Vec<(u32, u32)>, std::io::Error> {
    let mut directory_file = BufReader::new(File::open(directory_path)?);
    let mut lexicon_file = BufReader::new(File::open(lexicon_path)?);
    let mut index_file = BufReader::new(File::open(index_path)?);

    let lexicon_position = query_term_directory(&mut directory_file, term)?;

    lexicon_file.seek(SeekFrom::Start(lexicon_position))?;

    let term_metadata = query_term_lexicon(&mut lexicon_file, term)?;

    let postings = query_term_index(&mut index_file, &term_metadata)?;

    Ok(postings)
}


fn query_term_directory(directory_file: &mut BufReader<File>, term: &str) -> Result<u64, std::io::Error> {
    let total_dirs = directory_file.read_u32::<LittleEndian>()?;

    let mut lexicon_position = 0;
    let mut last_lexicon_position = 0;

    for _ in 0..total_dirs {
        let term_length = directory_file.read_u32::<LittleEndian>()? as usize;
        let mut term_buffer = vec![0u8; term_length];
        directory_file.read_exact(&mut term_buffer)?;
        let dir_term = String::from_utf8_lossy(&term_buffer);

        last_lexicon_position = lexicon_position;
        lexicon_position = directory_file.read_u64::<LittleEndian>()?;

        if dir_term.as_ref() >= term {
            return Ok(if dir_term.as_ref() == term { lexicon_position } else { last_lexicon_position });
        }
    }

    Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Term not found in directory"))
}

fn query_term_lexicon(
    lexicon_file: &mut BufReader<File>,
    term: &str,
) -> Result<TermMetadata, std::io::Error> {
    loop {
        // Read the length of the term first
        let term_length = match lexicon_file.read_u32::<LittleEndian>() {
            Ok(length) => length as usize,
            Err(e) => return Err(e),
        };

        let mut term_buffer = vec![0u8; term_length];
        lexicon_file.read_exact(&mut term_buffer)?;

        let lex_term = String::from_utf8_lossy(&term_buffer);

        if lex_term.as_ref() > term {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Term not found in lexicon"));
        }

        let term_id = lexicon_file.read_u32::<LittleEndian>()?;
        let doc_freq = lexicon_file.read_u32::<LittleEndian>()?;
        let total_term_freq = lexicon_file.read_u32::<LittleEndian>()?;
        let term_start_pointer = lexicon_file.read_u64::<LittleEndian>()?;
        let num_blocks = lexicon_file.read_u32::<LittleEndian>()?;
        let num_posting_in_last_block = lexicon_file.read_u32::<LittleEndian>()?;
        let last_doc_id = lexicon_file.read_u32::<LittleEndian>()?;

        let mut compressed_docids_sizes_per_block = vec![0u64; num_blocks as usize];
        for size in &mut compressed_docids_sizes_per_block {
            *size = lexicon_file.read_u64::<LittleEndian>()?;
        }

        let mut block_offsets = vec![0u64; num_blocks as usize];
        for offset in &mut block_offsets {
            *offset = lexicon_file.read_u64::<LittleEndian>()?;
        }

        let mut block_maxima = vec![0u32; num_blocks as usize];
        for max in &mut block_maxima {
            *max = lexicon_file.read_u32::<LittleEndian>()?;
        }

        if lex_term == term {
            return Ok(TermMetadata {
                term_id,
                doc_freq,
                total_term_freq,
                term_start_pointer,
                num_blocks,
                num_posting_in_last_block,
                last_doc_id,
                compressed_docids_sizes_per_block,
                block_offsets,
                block_maxima,
            });
        }
    }
}

fn query_term_index(
    index_file: &mut BufReader<File>,
    term_metadata: &TermMetadata,
) -> std::io::Result<Vec<(u32, u32)>> {
    index_file.seek(SeekFrom::Start(term_metadata.term_start_pointer))?;

    let mut postings = Vec::with_capacity(term_metadata.doc_freq as usize);

    for (i, &compressed_size) in term_metadata.compressed_docids_sizes_per_block.iter().enumerate() {
        // Determine the number of docids in this block
        let block_size = if i == term_metadata.compressed_docids_sizes_per_block.len() - 1 {
            term_metadata.num_posting_in_last_block as usize
        } else {
            BLOCK_SIZE
        };

        // Read and decompress docids for this block
        let mut compressed_docids = vec![0u8; compressed_size as usize];
        index_file.read_exact(&mut compressed_docids)?;

        let mut docids = vec![0u32; block_size];
        decode::<Scalar>(&compressed_docids, block_size, &mut docids);

        // Read frequencies for this block
        let mut frequencies = vec![0u32; block_size];
        for freq in &mut frequencies {
            *freq = index_file.read_u32::<LittleEndian>()?;
        }

        // Combine docids and frequencies into postings
        postings.extend(docids.into_iter().zip(frequencies.into_iter()));
    }

    Ok(postings)
}


