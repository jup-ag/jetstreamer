use std::sync::Arc;

use clickhouse::{Client, Row};
use dashmap::DashMap;
use futures_util::FutureExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use solana_address::Address;
use solana_message::VersionedMessage;

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, TransactionData};

/// Per-slot accumulator: maps each pubkey to its mention count within that slot.
static PENDING_BY_SLOT: Lazy<
    DashMap<u64, DashMap<Address, u32, ahash::RandomState>, ahash::RandomState>,
> = Lazy::new(|| DashMap::with_hasher(ahash::RandomState::new()));

#[derive(Row, Deserialize, Serialize, Copy, Clone, Debug)]
struct PubkeyMention {
    slot: u32,
    timestamp: u32,
    pubkey: Address,
    num_mentions: u32,
}

#[derive(Debug, Clone)]
/// Tracks per-slot pubkey mention counts and writes them to ClickHouse.
///
/// For every transaction, all account keys referenced in the message (both static and loaded)
/// are counted. A ClickHouse `pubkey_mentions` table stores the aggregated count per
/// `(slot, pubkey)` pair using `ReplacingMergeTree` for safe parallel ingestion.
///
/// A companion `pubkeys` table assigns a unique auto-incremented id to each pubkey, maintained
/// via a materialised view so lookups by id are efficient.
pub struct PubkeyStatsPlugin;

impl PubkeyStatsPlugin {
    /// Creates a new instance.
    pub const fn new() -> Self {
        Self
    }

    fn take_slot_events(slot: u64, block_time: Option<i64>) -> Vec<PubkeyMention> {
        let timestamp = clamp_block_time(block_time);
        if let Some((_, pubkey_counts)) = PENDING_BY_SLOT.remove(&slot) {
            return pubkey_counts
                .into_iter()
                .map(|(pubkey, num_mentions)| PubkeyMention {
                    slot: slot.min(u32::MAX as u64) as u32,
                    timestamp,
                    pubkey,
                    num_mentions,
                })
                .collect();
        }
        Vec::new()
    }

    fn drain_all_pending(block_time: Option<i64>) -> Vec<PubkeyMention> {
        let timestamp = clamp_block_time(block_time);
        let slots: Vec<u64> = PENDING_BY_SLOT.iter().map(|entry| *entry.key()).collect();
        let mut rows = Vec::new();
        for slot in slots {
            if let Some((_, pubkey_counts)) = PENDING_BY_SLOT.remove(&slot) {
                rows.extend(pubkey_counts.into_iter().map(|(pubkey, num_mentions)| {
                    PubkeyMention {
                        slot: slot.min(u32::MAX as u64) as u32,
                        timestamp,
                        pubkey,
                        num_mentions,
                    }
                }));
            }
        }
        rows
    }
}

impl Default for PubkeyStatsPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for PubkeyStatsPlugin {
    #[inline(always)]
    fn name(&self) -> &'static str {
        "Pubkey Stats"
    }

    #[inline(always)]
    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            let account_keys = match &transaction.transaction.message {
                VersionedMessage::Legacy(msg) => &msg.account_keys,
                VersionedMessage::V0(msg) => &msg.account_keys,
            };
            if account_keys.is_empty() {
                return Ok(());
            }

            let slot = transaction.slot;
            let slot_entry = PENDING_BY_SLOT
                .entry(slot)
                .or_insert_with(|| DashMap::with_hasher(ahash::RandomState::new()));
            for pubkey in account_keys {
                *slot_entry.entry(*pubkey).or_insert(0) += 1;
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_block(
        &self,
        _thread_id: usize,
        db: Option<Arc<Client>>,
        block: &BlockData,
    ) -> PluginFuture<'_> {
        let slot = block.slot();
        let block_time = block.block_time();
        let was_skipped = block.was_skipped();
        async move {
            if was_skipped {
                return Ok(());
            }

            let rows = Self::take_slot_events(slot, block_time);

            if let Some(db_client) = db
                && !rows.is_empty()
            {
                tokio::spawn(async move {
                    if let Err(err) = write_pubkey_mentions(db_client, rows).await {
                        log::error!("failed to write pubkey mentions: {}", err);
                    }
                });
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_load(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            log::info!("Pubkey Stats Plugin loaded.");
            if let Some(db) = db {
                log::info!("Creating pubkey_mentions table if it does not exist...");
                db.query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pubkey_mentions (
                        slot          UInt32,
                        timestamp     DateTime('UTC'),
                        pubkey        FixedString(32),
                        num_mentions  UInt32
                    )
                    ENGINE = ReplacingMergeTree(timestamp)
                    ORDER BY (slot, pubkey)
                    "#,
                )
                .execute()
                .await?;

                log::info!("Creating pubkeys table if it does not exist...");
                db.query(
                    r#"
                    CREATE TABLE IF NOT EXISTS pubkeys (
                        pubkey  FixedString(32),
                        id      UInt64
                    )
                    ENGINE = ReplacingMergeTree()
                    ORDER BY pubkey
                    "#,
                )
                .execute()
                .await?;

                log::info!("Creating pubkeys materialised view if it does not exist...");
                db.query(
                    r#"
                    CREATE MATERIALIZED VIEW IF NOT EXISTS pubkeys_mv TO pubkeys AS
                    SELECT
                        pubkey,
                        sipHash64(pubkey) AS id
                    FROM pubkey_mentions
                    GROUP BY pubkey
                    "#,
                )
                .execute()
                .await?;

                log::info!("done.");
            } else {
                log::warn!(
                    "Pubkey Stats Plugin running without ClickHouse; data will not be persisted."
                );
            }
            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_exit(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            if let Some(db_client) = db {
                let rows = Self::drain_all_pending(None);
                if !rows.is_empty() {
                    write_pubkey_mentions(Arc::clone(&db_client), rows)
                        .await
                        .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(err)
                        })?;
                }
                backfill_pubkey_timestamps(db_client)
                    .await
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
            }
            Ok(())
        }
        .boxed()
    }
}

async fn write_pubkey_mentions(
    db: Arc<Client>,
    rows: Vec<PubkeyMention>,
) -> Result<(), clickhouse::error::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = db.insert::<PubkeyMention>("pubkey_mentions").await?;
    for row in rows {
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(())
}

fn clamp_block_time(block_time: Option<i64>) -> u32 {
    let Some(raw_ts) = block_time else {
        return 0;
    };
    if raw_ts < 0 {
        0
    } else if raw_ts > u32::MAX as i64 {
        u32::MAX
    } else {
        raw_ts as u32
    }
}

async fn backfill_pubkey_timestamps(db: Arc<Client>) -> Result<(), clickhouse::error::Error> {
    db.query(
        r#"
        INSERT INTO pubkey_mentions
        SELECT pm.slot,
               ss.block_time,
               pm.pubkey,
               pm.num_mentions
        FROM pubkey_mentions AS pm
        ANY INNER JOIN jetstreamer_slot_status AS ss USING (slot)
        WHERE pm.timestamp = toDateTime(0)
          AND ss.block_time > toDateTime(0)
        "#,
    )
    .execute()
    .await?;

    Ok(())
}
