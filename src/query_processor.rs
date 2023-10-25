extern crate stream_vbyte;
extern crate byteorder;

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use byteorder::{LittleEndian, ReadBytesExt};
use stream_vbyte::decode::decode;
use stream_vbyte::scalar::Scalar;
use crate::utils::DIRECTORY_NTH_TERM;

const BLOCK_SIZE: usize = 64;

pub(crate) fn query_term(term: &str, index_path: &str, lexicon_path: &str, directory_path: &str) -> Result<Vec<(u32, u32)>, std::io::Error> {
    let mut directory_file = BufReader::new(File::open(directory_path)?);
    let total_terms = directory_file.read_u32::<LittleEndian>()?;

    let mut lexicon_position = 0;
    let mut last_lexicon_position = 0;
    for _ in 0..total_terms / DIRECTORY_NTH_TERM {
        // Read the length of the term first
        let term_length = directory_file.read_u32::<LittleEndian>()? as usize;
        let mut term_buffer = vec![0u8; term_length];
        directory_file.read_exact(&mut term_buffer)?;
        let dir_term = String::from_utf8_lossy(&term_buffer);
        last_lexicon_position = lexicon_position;
        lexicon_position = directory_file.read_u64::<LittleEndian>()?;

        if dir_term.as_ref() >= term {
            lexicon_position = last_lexicon_position;
            break;
        }
    }

    let mut lexicon_file = BufReader::new(File::open(lexicon_path)?);
    lexicon_file.seek(SeekFrom::Start(lexicon_position))?;

    loop {
        // Read the length of the term first
        let term_length = lexicon_file.read_u32::<LittleEndian>()?;
        let mut term_buffer = vec![0u8; term_length as usize];
        lexicon_file.read_exact(&mut term_buffer)?;

        let lex_term = String::from_utf8_lossy(&term_buffer);

        if lex_term.as_ref() > term {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Term not found in lexicon"));
        }

        let term_id = lexicon_file.read_u32::<LittleEndian>()?;
        let doc_freq = lexicon_file.read_u32::<LittleEndian>()?;
        let total_term_freq = lexicon_file.read_u32::<LittleEndian>()?;
        let pointer = lexicon_file.read_u64::<LittleEndian>()?;
        let num_blocks = lexicon_file.read_u32::<LittleEndian>()?;
        let size_last_block = lexicon_file.read_u32::<LittleEndian>()?;
        let last_doc_id = lexicon_file.read_u32::<LittleEndian>()?;
        let total_bytes = lexicon_file.read_u64::<LittleEndian>()?;

        let mut block_offsets = vec![0u64; num_blocks as usize];
        for offset in &mut block_offsets {
            *offset = lexicon_file.read_u64::<LittleEndian>()?;
        }

        let mut block_maxima = vec![0u32; num_blocks as usize];
        for max in &mut block_maxima {
            *max = lexicon_file.read_u32::<LittleEndian>()?;
        }

        let mut index_file = BufReader::new(File::open(index_path)?);
        index_file.seek(SeekFrom::Start(pointer))?;

        let mut compressed_docids = vec![0u8; (doc_freq as usize) * 5 + doc_freq as usize / 4];  // worst-case size
        index_file.read_exact(&mut compressed_docids)?;

        let mut docids = vec![0u32; doc_freq as usize];
        let _ = decode::<Scalar>(&compressed_docids, doc_freq as usize, &mut docids);

        let mut frequencies = vec![0u32; doc_freq as usize];
        for freq in &mut frequencies {
            *freq = index_file.read_u32::<LittleEndian>()?;
        }

        if lex_term == term {
            return Ok(docids.into_iter().zip(frequencies).collect());
        }
    }
}
