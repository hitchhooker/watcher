use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use subxt::{OnlineClient, PolkadotConfig};
use tokio::sync::{mpsc, Mutex};
use std::sync::Arc;

use crate::config::Config;

#[subxt::subxt(runtime_metadata_path = "metadata.scale")]
pub mod rococo_people {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IdentityEvent {
    block_number: u32,
    event_type: String,
    who: String,
    registrar_index: u32,
    identity_info: String,
    identity_hash: String,
    judgement: String,
    timestamp: i64,
}

pub struct Watcher {
    client: OnlineClient<PolkadotConfig>,
    db: Arc<Mutex<Connection>>,
}

impl Watcher {
    pub async fn new(config: &Config) -> Result<Self> {
        let client = OnlineClient::from_url(&config.ws_url).await?;
        let db = Connection::open(&config.db_path)?;
        Self::initialize_db(&db)?;
        let db = Arc::new(Mutex::new(db));

        Ok(Self { client, db })
    }

    fn initialize_db(conn: &Connection) -> Result<()> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS identity_events (
                id INTEGER PRIMARY KEY,
                block_number INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                who TEXT NOT NULL,
                registrar_index INTEGER NOT NULL,
                identity_info TEXT NOT NULL,
                identity_hash TEXT NOT NULL,
                judgement TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_identity_events_who ON identity_events(who)",
            [],
        )?;

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(100);

        // Clone the necessary data before spawning the task.
        let client = self.client.clone();
        let db = Arc::clone(&self.db);

        // Create a new Watcher instance with the cloned data.
        let watcher = Watcher { client, db };

        // Spawn the task with the new Watcher instance.
        tokio::spawn(async move {
            if let Err(e) = watcher.watch_events(tx).await {
                eprintln!("Error watching events: {:?}", e);
            }
        });

        while let Some(event) = rx.recv().await {
            println!("add event to db with store_event");
            self.store_event(event).await?;
        }

        Ok(())
    }

    async fn watch_events(&self, tx: mpsc::Sender<IdentityEvent>) -> Result<()> {
        let mut blocks = self.client.blocks().subscribe_finalized().await?;
        println!("Watching for events...");

        while let Some(block) = blocks.next().await {
            let block = block?;
            let events = block.events().await?;
            let timestamp = chrono::Utc::now().timestamp();
            println!("{} block: {}, events: {}", timestamp, block.number(), events.len());

            for event in events.iter() {
                let event = event?;
                if event.pallet_name() == "Identity" {
                    match event.variant_name() {
                        "JudgementRequested" | "JudgementGiven" | "JudgementUnrequested" | 
                            "JudgementForced" | "IdentitySet" | "IdentityCleared" | "IdentityKilled" => {
                                println!("Found judgement-related event: {}", event.variant_name());
                                if let Ok(parsed_event) = self.parse_event(event, block.number(), timestamp) {
                                    println!("Parsed event: {:?}", parsed_event);
                                    tx.send(parsed_event).await?;
                                }
                            }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_event(
        &self,
        event: subxt::events::EventDetails<PolkadotConfig>,
        block_number: u32,
        timestamp: i64,
    ) -> Result<IdentityEvent> {
        let event_type = event.variant_name().to_string();

        // Get fields as bytes from the event
        let fields = event.field_bytes();

        match event_type.as_str() {
            "JudgementRequested" => {
                let who = String::from_utf8(fields[0..32].to_vec())?;
                let registrar_index = u32::from_le_bytes(fields[32..36].try_into()?);
                Ok(IdentityEvent {
                    block_number,
                    event_type,
                    who,
                    registrar_index,
                    identity_info: String::new(),
                    identity_hash: String::new(),
                    judgement: String::new(),
                    timestamp,
                })
            }
            "IdentitySet" => {
                let who = String::from_utf8(fields[0..32].to_vec())?;
                Ok(IdentityEvent {
                    block_number,
                    event_type,
                    who,
                    registrar_index: 0, // change to reg_index as config variable
                    identity_info: String::new(),
                    identity_hash: String::new(),
                    judgement: String::new(),
                    timestamp,
                })
            }
            "JudgementGiven" => {
                // address
                let who = String::from_utf8(fields[0..32].to_vec())?;
                // registrar index(u32)
                let registrar_index = u32::from_le_bytes(fields[32..36].try_into()?);
                // judgement hash
                let judgement = String::from_utf8(fields[36..68].to_vec())?;
                Ok(IdentityEvent {
                    block_number,
                    event_type,
                    who,
                    registrar_index,
                    identity_info: String::new(),
                    identity_hash: String::new(),
                    judgement,
                    timestamp,
                })
            }
            _ => Err(anyhow!("Unknown event type")),
        }
    }


    async fn store_event(&self, event: IdentityEvent) -> Result<()> {
        let db = self.db.lock().await;
        db.execute(
            "INSERT INTO identity_events (block_number, event_type, who, registrar_index, identity_info, identity_hash, judgement, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
             params![
             event.block_number,
             event.event_type,
             event.who,
             event.registrar_index,
             event.identity_info,
             event.identity_hash,
             event.judgement,
             event.timestamp,
             ],
        )?;

        Ok(())
    }

    pub async fn get_identity_history(&self, who: &str) -> Result<Vec<IdentityEvent>> {
        let db = self.db.lock().await;
        let mut stmt = db.prepare(
            "SELECT * FROM identity_events WHERE who = ? ORDER BY block_number DESC"
        )?;
        let events = stmt.query_map(params![who], |row| {
            Ok(IdentityEvent {
                block_number: row.get(1)?,
                event_type: row.get(2)?,
                who: row.get(3)?,
                registrar_index: row.get(4)?,
                identity_info: row.get(5)?,
                identity_hash: row.get(6)?,
                judgement: row.get(7)?,
                timestamp: row.get(8)?,
            })
        })?;

        let mut history = Vec::new();
        for event in events {
            history.push(event?);
        }

        Ok(history)
    }

    pub async fn get_latest_identity(&self, who: &str) -> Result<Option<IdentityEvent>> {
        let db = self.db.lock().await;
        let mut stmt = db.prepare(
            "SELECT * FROM identity_events WHERE who = ? ORDER BY block_number DESC LIMIT 1"
        )?;
        let event = stmt.query_row(params![who], |row| {
            Ok(IdentityEvent {
                block_number: row.get(1)?,
                event_type: row.get(2)?,
                who: row.get(3)?,
                registrar_index: row.get(4)?,
                identity_info: row.get(5)?,
                identity_hash: row.get(6)?,
                judgement: row.get(7)?,
                timestamp: row.get(8)?,
            })
        }).optional()?;

        Ok(event)
    }
}
