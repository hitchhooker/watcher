//src/config.rs
use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
pub struct Config {
    pub ws_url: String,
    pub db_path: String,
    pub reg_seed: String,
    pub reg_index: u32,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    }
}
