use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

struct MergingIterator {
    next_data: Option<HashMap<u32, HashMap<usize, u32>>>,
}

impl MergingIterator {
    fn new(mut file: File) -> std::io::Result<Self> {
        let next_data = {
            #[cfg(feature = "debug_unicode")]
            {
                let mut reader = BufReader::new(&file);
                reader.by_ref().lines().next().and_then(|l| l.ok()).and_then(|line| {
                    serde_json::from_str::<HashMap<u32, HashMap<usize, u32>>>(&line).ok()
                })
            }

            #[cfg(not(feature = "debug_unicode"))]
            {
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                Some(bincode::deserialize::<HashMap<u32, HashMap<usize, u32>>>(&buffer).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?)
            }
        };

        Ok(MergingIterator { next_data })
    }
    fn next(&mut self) -> Option<HashMap<u32, HashMap<usize, u32>>> {
        self.next_data.take()
    }
}


struct ReverseOrdered {
    value: HashMap<u32, HashMap<usize, u32>>,
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
        let self_key = *self.value.keys().next().unwrap_or(&0);
        let other_key = *other.value.keys().next().unwrap_or(&0);
        other_key.cmp(&self_key).then_with(|| self.idx.cmp(&other.idx))
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
        .filter_map(Result::ok) // Keep only Ok iterators, discard Err ones
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

    while let Some(ReverseOrdered { value, idx }) = heap.pop() {
        #[cfg(feature = "debug_unicode")]
        {
            let serialized_data = serde_json::to_string(&value)?;
            writer.write_all(serialized_data.as_bytes())?;
            writer.write_all(b"\n")?;
        }

        #[cfg(not(feature = "debug_unicode"))]
        {
            let serialized_data = bincode::serialize(&value)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            writer.write_all(&serialized_data)?;
        }

        if let Some(val) = merging_iters[idx].next() {
            heap.push(ReverseOrdered {
                value: val,
                idx,
            });
        }
    }

    Ok(())
}
