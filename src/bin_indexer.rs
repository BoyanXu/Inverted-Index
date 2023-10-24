use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, Read, Write, BufRead};
use stream_vbyte::{
    encode::encode as vbyte_encode,
    scalar::Scalar
};

const BLOCK_SIZE: usize = 64;

fn delta_encode(postings: &[(usize, u32)]) -> Vec<(usize, u32)> {
    let mut last_doc_id = 0;
    let mut delta_encoded: Vec<(usize, u32)> = Vec::new();
    for (doc_id, freq) in postings {
        let delta = doc_id - last_doc_id;
        delta_encoded.push((delta, *freq));
        last_doc_id = *doc_id;
    }
    delta_encoded
}

fn compress_block(block: &[(usize, u32)]) -> (Vec<u8>, Vec<u8>, usize) {
    let (doc_ids_usize, freqs): (Vec<_>, Vec<_>) = block.iter().cloned().unzip();
    let doc_ids: Vec<u32> = doc_ids_usize.into_iter().map(|x| x as u32).collect();

    let mut compressed_doc_ids = vec![0; 5 * doc_ids.len()];
    let encoded_len_docs = vbyte_encode::<Scalar>(&doc_ids, &mut compressed_doc_ids);
    compressed_doc_ids.truncate(encoded_len_docs);

    let mut compressed_freqs = vec![0; 5 * freqs.len()];
    let encoded_len_freqs = vbyte_encode::<Scalar>(&freqs, &mut compressed_freqs);
    compressed_freqs.truncate(encoded_len_freqs);

    let last_doc_id = *doc_ids.last().unwrap_or(&0);
    (compressed_doc_ids, compressed_freqs, last_doc_id as usize)
}

fn write_term_to_index(term: &str, delta_encoded: &[(usize, u32)], writer: &mut BufWriter<File>, lexicon_writer: &mut BufWriter<File>) -> std::io::Result<()> {
    let offset = writer.stream_position()?;
    lexicon_writer.write_all(format!("{}\t{}\n", term, offset).as_bytes())?;

    let num_blocks = (delta_encoded.len() + BLOCK_SIZE - 1) / BLOCK_SIZE;
    let mut last_doc_ids = Vec::<u32>::new();
    let mut doc_block_sizes = Vec::with_capacity(num_blocks);
    let mut freq_block_sizes = Vec::with_capacity(num_blocks);

    for block in delta_encoded.chunks(BLOCK_SIZE) {
        let (compressed_doc_ids, compressed_freqs, last_doc_id) = compress_block(block);
        last_doc_ids.push(last_doc_id as u32);
        doc_block_sizes.push(compressed_doc_ids.len() as u32);
        freq_block_sizes.push(compressed_freqs.len() as u32);

        let metadata = [&last_doc_ids[..], &doc_block_sizes[..], &freq_block_sizes[..]];
        let mut compressed_metadata = vec![0; 5 * metadata.len()];
        let encoded_len_metadata = vbyte_encode::<Scalar>(&metadata.concat(), &mut compressed_metadata);
        compressed_metadata.truncate(encoded_len_metadata);

        writer.write_all(&compressed_metadata)?;
        writer.write_all(&compressed_doc_ids)?;
        writer.write_all(&compressed_freqs)?;
    }

    Ok(())
}

#[cfg(feature = "debug_unicode")]
fn deserialize_from_reader<R: Read, T: serde::de::DeserializeOwned>(reader: &mut R) -> Result<T, Box<dyn std::error::Error>> {
    use serde_json::from_reader;
    Ok(from_reader(reader)?)
}

#[cfg(not(feature = "debug_unicode"))]
fn deserialize_from_reader<R: Read, T: serde::de::DeserializeOwned>(reader: &mut R) -> Result<T, Box<dyn std::error::Error>> {
    use bincode::deserialize_from;
    Ok(deserialize_from(reader)?)
}

pub fn build_binary_inverted_index(input_file: &str, output_file: &str, lexicon_file: &str) -> std::io::Result<()> {
    let input = File::open(input_file)?;
    let mut reader = BufReader::new(input);

    let output = OpenOptions::new().create(true).write(true).truncate(true).open(output_file)?;
    let mut writer = BufWriter::new(output);

    let lexicon = OpenOptions::new().create(true).write(true).truncate(true).open(lexicon_file)?;
    let mut lexicon_writer = BufWriter::new(lexicon);

    #[cfg(feature = "debug_unicode")]
    {
        let mut buffer = String::new();
        while reader.read_line(&mut buffer)? > 0 {
            if let Ok((term, postings)) = serde_json::from_str::<(String, Vec<(usize, u32)>)>(&buffer) {
                let delta_encoded = delta_encode(&postings);
                write_term_to_index(&term, &delta_encoded, &mut writer, &mut lexicon_writer)?;
            }
            buffer.clear();
        }
    }

    #[cfg(not(feature = "debug_unicode"))]
    {
        while let Ok((term, postings)) = bincode::deserialize_from::<_, (String, Vec<(usize, u32)>)>(&mut reader) {
            let delta_encoded = delta_encode(&postings);
            write_term_to_index(&term, &delta_encoded, &mut writer, &mut lexicon_writer)?;
        }
    }

    Ok(())
}
