use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

struct MergingIterator {
    data: Vec<(String, HashMap<usize, u32>)>,
    position: usize,
}

impl MergingIterator {
    fn new(mut file: File) -> std::io::Result<Self> {
        let data = {
            #[cfg(feature = "debug_unicode")]
            {
                let mut reader = BufReader::new(&file);
                let line = reader.by_ref().lines().next().and_then(|l| l.ok());
                match serde_json::from_str::<Vec<(String, HashMap<usize, u32>)>>(&line.unwrap_or_default()) {
                    Ok(vec_data) => vec_data,
                    Err(e) => {
                        eprintln!("Failed to deserialize data from file: {}", e);
                        Vec::new()
                    }
                }
            }

            #[cfg(not(feature = "debug_unicode"))]
            {
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                match bincode::deserialize::<Vec<(String, HashMap<usize, u32>)>>(&buffer) {
                    Ok(vec_data) => vec_data,
                    Err(e) => {
                        eprintln!("Failed to deserialize binary data: {}", e);
                        Vec::new()
                    }
                }
            }
        };

        Ok(MergingIterator { data, position: 0 })
    }

    fn next(&mut self) -> Option<(String, HashMap<usize, u32>)> {
        if self.position < self.data.len() {
            let result = self.data[self.position].clone();
            self.position += 1;
            Some(result)
        } else {
            None
        }
    }
}

struct ReverseOrdered {
    value: (String, HashMap<usize, u32>),
    idx: usize,
}


impl PartialEq for ReverseOrdered {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for ReverseOrdered {}

impl Ord for ReverseOrdered {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.value.0.cmp(&self.value.0).then_with(|| self.idx.cmp(&other.idx))
    }
}

impl PartialOrd for ReverseOrdered {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn merge_sorted_files(output_file_path: &str, input_files: Vec<PathBuf>) -> std::io::Result<()> {
    let mut merging_iters: Vec<MergingIterator> = input_files.into_iter()
        .map(File::open)
        .filter_map(|f| f.ok())
        .map(MergingIterator::new)
        .filter_map(Result::ok)
        .collect();

    let mut heap = BinaryHeap::new();
    for (idx, iter) in merging_iters.iter_mut().enumerate() {
        if let Some(val) = iter.next() {
            heap.push(ReverseOrdered {
                value: val,
                idx,
            });
        }
    }

    let output_file = File::create(output_file_path)?;
    let mut writer = BufWriter::new(output_file);

    let mut current_term: Option<String> = None;
    let mut current_buffer: HashMap<usize, u32> = HashMap::new();

    while let Some(ReverseOrdered { value, idx }) = heap.pop() {
        let (term, postings) = value;

        if let Some(ref current_t) = current_term {
            if &term != current_t { // Term changed
                // Write current buffer to disk and reset
                write_posting(&mut writer, (current_t.clone(), current_buffer.clone()))?;
                current_buffer.clear();
            }
        }

        current_term = Some(term.clone());

        for (doc_id, freq) in postings {
            *current_buffer.entry(doc_id).or_insert(0) += freq;
        }

        if let Some(val) = merging_iters[idx].next() {
            heap.push(ReverseOrdered {
                value: val,
                idx,
            });
        }
    }

    // Write any remaining data in the buffer
    if let Some(term) = current_term {
        write_posting(&mut writer, (term, current_buffer))?;
    }

    Ok(())
}

// Helper function to write postings to file
fn write_posting(writer: &mut BufWriter<File>, posting: (String, HashMap<usize, u32>)) -> std::io::Result<()> {
    // Sort by doc_ID (although HashMap doesn't guarantee order, it's helpful to do it explicitly)
    let mut sorted_posting: Vec<(usize, u32)> = posting.1.into_iter().collect();
    sorted_posting.sort_by_key(|&(doc_id, _)| doc_id);

    #[cfg(feature = "debug_unicode")]
    {
        let serialized_data = serde_json::to_string(&(posting.0, sorted_posting))?;
        writer.write_all(serialized_data.as_bytes())?;
        writer.write_all(b"\n")?;
    }

    #[cfg(not(feature = "debug_unicode"))]
    {
        let serialized_data = bincode::serialize(&(posting.0, sorted_posting))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        writer.write_all(&serialized_data)?;
    }

    Ok(())
}