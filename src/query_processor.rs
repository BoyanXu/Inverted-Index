extern crate stream_vbyte;
extern crate byteorder;

use std::borrow::Cow;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use byteorder::{LittleEndian, ReadBytesExt};
use stream_vbyte::decode::decode;
use stream_vbyte::scalar::Scalar;

const BLOCK_SIZE: usize = 64;
const DIRECTORY_NTH_TERM: u32 = 100;

pub(crate) fn query_term(term: &str, index_path: &str, lexicon_path: &str, directory_path: &str) -> Result<Vec<(u32, u32)>, std::io::Error> {
    let mut directory_file = BufReader::new(File::open(directory_path)?);
    let total_terms = directory_file.read_u32::<LittleEndian>()?;

    let mut lexicon_position = 0;
    for _ in 0..total_terms / DIRECTORY_NTH_TERM {
        // Read the length of the term first
        let term_length = directory_file.read_u32::<LittleEndian>()? as usize;
        let mut term_buffer = vec![0u8; term_length];
        directory_file.read_exact(&mut term_buffer)?;
        let dir_term = String::from_utf8_lossy(&term_buffer);
        lexicon_position = directory_file.read_u64::<LittleEndian>()?;
        if dir_term.as_ref() >= term {
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

        if lex_term == term {
            let term_id = lexicon_file.read_u32::<LittleEndian>()?;
            let doc_freq = lexicon_file.read_u32::<LittleEndian>()?;
            let total_term_freq = lexicon_file.read_u32::<LittleEndian>()?;
            let pointer = lexicon_file.read_u64::<LittleEndian>()?;

            // Remaining fields would be read similarly...

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

            return Ok(docids.into_iter().zip(frequencies).collect());
        }
    }
}
