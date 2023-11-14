pub const DEBUG_MODE: bool = true;
pub const DEBUG_DOC_LIMIT: usize = 1000;
pub const BATCH_SIZE: usize = DEBUG_DOC_LIMIT / 100;
pub const BLOCK_SIZE: usize = 64;
pub const DIRECTORY_NTH_TERM: u32 = 100;

pub const BM25_K1: f32 = 1.2;

pub const BM25_B: f32 = 0.75;