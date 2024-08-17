//src/main.rs
mod config;
mod watcher;

use anyhow::Result;
use config::Config;
use watcher::Watcher;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load("config.toml")?;
    let watcher = Watcher::new(&config).await?;
    watcher.run().await?;
    Ok(())
}
