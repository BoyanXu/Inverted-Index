extern crate stream_vbyte;
extern crate byteorder;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use byteorder::{LittleEndian, ReadBytesExt};
use stream_vbyte::decode::decode;
use stream_vbyte::scalar::Scalar;
use crate::utils::DIRECTORY_NTH_TERM;
use crate::bin_indexer::TermMetadata;
use crate::disk_io::load_doc_metadata;
use crate::parser::parse_line as tokenize;
use crate::utils::{BM25_K1, BM25_B};

const BLOCK_SIZE: usize = 64;

pub struct TermQueryProcessor {
    directory_file: BufReader<File>,
    lexicon_file: BufReader<File>,
    index_file: BufReader<File>,
    doc_metadata: HashMap<u32, (String, u32)>,
    directory_cache: HashMap<String, u64>,
    metadata_cache: HashMap<String, TermMetadata>,
    total_docs: u32,
    avg_doc_len: u32,
}
impl TermQueryProcessor {
    pub fn new(index_path: &str, lexicon_path: &str, directory_path: &str, doc_metadata_path: &str) -> Self {
        let doc_metadata = load_doc_metadata(doc_metadata_path).unwrap();
        let total_docs = doc_metadata.keys().max().cloned().unwrap_or(0);
        let total_length: u32 = doc_metadata.values()
            .map(|(_, length)| length)
            .sum();
        let avg_doc_len = if total_docs > 0 {
            total_length / total_docs
        } else {
            0 // Return 0 if there are no documents
        };

        Self {
            directory_file: BufReader::new(File::open(directory_path).unwrap()),
            lexicon_file: BufReader::new(File::open(lexicon_path).unwrap()),
            index_file: BufReader::new(File::open(index_path).unwrap()),
            doc_metadata,
            directory_cache: Default::default(),
            metadata_cache: Default::default(),
            total_docs,
            avg_doc_len,
        }
    }

    pub fn query_term_directory(&mut self, term: &str) -> Result<u64, std::io::Error> {
        // Check the cache first
        if let Some(&position) = self.directory_cache.get(term) {
            return Ok(position);
        }

        self.directory_file.seek(SeekFrom::Start(0))?; // reset the file pointer to the beginning of the file
        let total_dirs = self.directory_file.read_u32::<LittleEndian>()?;

        let mut lexicon_position = 0;
        let mut last_lexicon_position = 0;

        for _ in 0..total_dirs {
            let term_length = self.directory_file.read_u32::<LittleEndian>()? as usize;
            let mut term_buffer = vec![0u8; term_length];
            self.directory_file.read_exact(&mut term_buffer)?;
            let dir_term = String::from_utf8_lossy(&term_buffer);

            last_lexicon_position = lexicon_position;
            lexicon_position = self.directory_file.read_u64::<LittleEndian>()?;

            match dir_term.as_ref().cmp(term) {
                std::cmp::Ordering::Equal => {
                    // Term matches, update cache with the current lexicon position
                    self.directory_cache.insert(term.to_string(), lexicon_position);
                    return Ok(lexicon_position)
                },
                std::cmp::Ordering::Greater => {
                    // Term is greater than the searched term, update cache with the last lexicon position
                    self.directory_cache.insert(term.to_string(), last_lexicon_position);
                    return Ok(last_lexicon_position)
                },
                std::cmp::Ordering::Less => {
                    // Continue searching if the directory term is less than the searched term
                    continue;
                },
            }

        }

        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Term not found in directory"))
    }

    pub fn query_term_metadata(&mut self, term: &str) -> Result<TermMetadata, std::io::Error> {
        // Check the cache first
        if let Some(metadata) = self.metadata_cache.get(term) {
            return Ok(metadata.clone()); // Clone the metadata as it's being returned by reference
        }

        let lexicon_directory = self.query_term_directory(term)?;
        self.lexicon_file.seek(SeekFrom::Start(lexicon_directory))?;

        loop {
            // Read the length of the term first
            let term_length = match self.lexicon_file.read_u32::<LittleEndian>() {
                Ok(length) => length as usize,
                Err(e) => return Err(e),
            };

            let mut term_buffer = vec![0u8; term_length];
            self.lexicon_file.read_exact(&mut term_buffer)?;

            let lex_term = String::from_utf8_lossy(&term_buffer);

            if lex_term.as_ref() > term {
                return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Term not found in lexicon"));
            }

            let term_id = self.lexicon_file.read_u32::<LittleEndian>()?;
            let doc_freq = self.lexicon_file.read_u32::<LittleEndian>()?;
            let total_term_freq = self.lexicon_file.read_u32::<LittleEndian>()?;
            let term_start_pointer = self.lexicon_file.read_u64::<LittleEndian>()?;
            let num_blocks = self.lexicon_file.read_u32::<LittleEndian>()?;
            let num_posting_in_last_block = self.lexicon_file.read_u32::<LittleEndian>()?;
            let last_doc_id = self.lexicon_file.read_u32::<LittleEndian>()?;

            let mut compressed_docids_sizes_per_block = vec![0u64; num_blocks as usize];
            for size in &mut compressed_docids_sizes_per_block {
                *size = self.lexicon_file.read_u64::<LittleEndian>()?;
            }

            let mut block_offsets = vec![0u64; num_blocks as usize];
            for offset in &mut block_offsets {
                *offset = self.lexicon_file.read_u64::<LittleEndian>()?;
            }

            let mut block_maxima = vec![0u32; num_blocks as usize];
            for max in &mut block_maxima {
                *max = self.lexicon_file.read_u32::<LittleEndian>()?;
            }

            if lex_term == term {
                let metadata = TermMetadata {
                    term_id,
                    doc_freq,
                    total_term_freq,
                    term_start_pointer,
                    num_blocks,
                    num_posting_in_last_block,
                    last_doc_id,
                    compressed_docids_per_block: compressed_docids_sizes_per_block,
                    block_offsets,
                    block_maxima,
                };

                // Insert the metadata into the cache
                self.metadata_cache.insert(term.to_string(), metadata.clone());

                // Return the metadata
                return Ok(metadata);
            }
        }
    }

    pub fn query_term_all_postings(&mut self, term: &str) -> std::io::Result<Vec<(u32, u32)>> {
        let term_metadata = self.query_term_metadata(term)?;

        self.index_file.seek(SeekFrom::Start(term_metadata.term_start_pointer))?;

        let mut postings = Vec::with_capacity(term_metadata.doc_freq as usize);

        // For each block
        for (i, &compressed_size) in term_metadata.compressed_docids_per_block.iter().enumerate() {
            // Determine the number of docids in this block
            let block_size = if i == term_metadata.compressed_docids_per_block.len() - 1 {
                term_metadata.num_posting_in_last_block as usize
            } else {
                BLOCK_SIZE
            };

            // Read and decompress docids for this block
            let mut compressed_docids = vec![0u8; compressed_size as usize];
            self.index_file.read_exact(&mut compressed_docids)?;

            let mut docids = vec![0u32; block_size];
            decode::<Scalar>(&compressed_docids, block_size, &mut docids);

            // Read frequencies for this block
            let mut frequencies = vec![0u32; block_size];
            for freq in &mut frequencies {
                *freq = self.index_file.read_u32::<LittleEndian>()?;
            }

            // Combine docids and frequencies into postings
            postings.extend(docids.into_iter().zip(frequencies.into_iter()));
        }
        Ok(postings)
    }

    pub fn query_term_postings_after_doc_k(&mut self, term: &str, k: u32) -> std::io::Result<Vec<(u32, u32)>> {
        let term_metadata = self.query_term_metadata(term)?;

        let mut postings = Vec::new();

        // Iterate through the blocks to find the starting block
        for (i, &max_docid) in term_metadata.block_maxima.iter().enumerate() {
            if max_docid < k {
                continue; // Skip blocks where max_docid is less than k
            }

            // Start processing from this block
            for (block_index, &offset) in term_metadata.block_offsets.iter().enumerate().skip(i) {
                // Seek to the start of the block
                self.index_file.seek(SeekFrom::Start(offset))?;

                // Determine the number of docids in this block
                let block_size = if block_index == term_metadata.compressed_docids_per_block.len() - 1 {
                    term_metadata.num_posting_in_last_block as usize
                } else {
                    BLOCK_SIZE
                };

                // Read and decompress docids for this block
                let compressed_size = term_metadata.compressed_docids_per_block[block_index];
                let mut compressed_docids = vec![0u8; compressed_size as usize];
                self.index_file.read_exact(&mut compressed_docids)?;

                let mut docids = vec![0u32; block_size];
                decode::<Scalar>(&compressed_docids, block_size, &mut docids);

                // Read frequencies for this block
                let mut frequencies = vec![0u32; block_size];
                for freq in &mut frequencies {
                    *freq = self.index_file.read_u32::<LittleEndian>()?;
                }

                // Combine docids and frequencies into postings
                postings.extend(docids.into_iter().zip(frequencies.into_iter()).filter(|&(docid, _)| docid >= k));
            }
            break; // Break after processing the required blocks
        }

        Ok(postings)
    }

    pub fn conjunctive_query(&mut self, query: &str) {
        println!("Total docs: {}", self.total_docs);
        println!("Average doc length: {}", self.avg_doc_len);
        let query_terms = tokenize(query);
        for term in query_terms {
            println!("Querying term: {}", term);
            let metadata = self.query_term_metadata(&term).unwrap();
            println!("Metadata: {:?}", metadata);
            for (doc_id, freq) in self.query_term_all_postings(&term).unwrap() {
                let bm25 = self.bm25(freq, metadata.doc_freq, doc_id);
                println!("doc_id: {}, freq: {}, bm25: {}", doc_id, freq, bm25);
            }
        }


    }

    pub fn bm25(&mut self, tf: u32, df: u32, doc_id: u32) -> f32 {

        let k1: f32 = BM25_K1;
        let b: f32 = BM25_B;
        let doc_len = self.doc_metadata.get(&doc_id).unwrap().1;
        let idf = ((self.total_docs as f32 - df as f32 + 0.5) / (df as f32 + 0.5)).ln() + 1.0;
        let term_freq_component = (tf as f32) * (k1 + 1.0);
        let denominator = tf as f32 + k1 * (1.0 - b + b * (doc_len as f32 / self.avg_doc_len as f32));

        idf * (term_freq_component / denominator)
    }

}
