pub fn parse_line(line: &str) -> Vec<String> {
    line.split_whitespace()
        .map(|word| word.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase())
        .collect()
}
