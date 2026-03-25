//! Datapipes bridge plugin.
//!
//! Converts Jetstreamer block/transaction data into length-delimited protobuf
//! [`BlockMessage`]s on stdout, matching the format that datapipes' Go
//! `poolactionparser` consumes.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use clickhouse::Client;
use dashmap::DashMap;
use futures_util::FutureExt;
use prost::Message;
use solana_storage_proto::convert::generated;
use solana_transaction_status::{TransactionWithStatusMeta, VersionedTransactionWithStatusMeta};

use crate::{Plugin, PluginFuture};
use jetstreamer_firehose::firehose::{BlockData, TransactionData};

type PluginResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Pending transaction awaiting its block's `on_block` flush.
struct PendingTx {
    index: u32,
    confirmed_tx: generated::ConfirmedTransaction,
}

/// Plugin that writes datapipes-compatible protobuf BlockMessages to stdout.
pub struct DatapipesBridgePlugin {
    pending_txs: DashMap<u64, Vec<PendingTx>>,
    blocks_emitted: AtomicU64,
    txs_emitted: AtomicU64,
}

impl DatapipesBridgePlugin {
    /// Creates a new bridge plugin.
    pub fn new() -> Self {
        Self {
            pending_txs: DashMap::new(),
            blocks_emitted: AtomicU64::new(0),
            txs_emitted: AtomicU64::new(0),
        }
    }

    /// Encodes a block and its transactions as a length-delimited protobuf BlockMessage
    /// and writes it to stdout.
    fn process_block(&self, block: &BlockData) -> PluginResult {
        let (slot, parent_slot, parent_blockhash, blockhash, block_time, block_height, rewards) =
            match block {
                BlockData::PossibleLeaderSkipped { .. } => return Ok(()),
                BlockData::Block {
                    slot,
                    parent_slot,
                    parent_blockhash,
                    blockhash,
                    block_time,
                    block_height,
                    rewards,
                    ..
                } => (
                    *slot,
                    *parent_slot,
                    parent_blockhash,
                    blockhash,
                    *block_time,
                    *block_height,
                    rewards,
                ),
            };

        // Take and sort pending transactions.
        let mut txs: Vec<PendingTx> = self
            .pending_txs
            .remove(&slot)
            .map(|(_, v)| v)
            .unwrap_or_default();
        txs.sort_by_key(|t| t.index);

        let tx_count = txs.len() as u64;

        // Encode each ConfirmedTransaction with the datapipes-specific `index` field
        // (proto field 100000, uint32). The standard Agave proto doesn't have this field,
        // so we encode the transaction normally, then append the index as raw bytes.
        // Protobuf is order-independent — decoders find fields by number, not position,
        // so appending works fine.
        let mut encoded_txs: Vec<Vec<u8>> = Vec::with_capacity(txs.len());
        for t in &txs {
            let mut tx_bytes = t.confirmed_tx.encode_to_vec();
            prost::encoding::uint32::encode(100000, &t.index, &mut tx_bytes);
            encoded_txs.push(tx_bytes);
        }

        // Build the ConfirmedBlock (everything except transactions).
        let rewards_proto: Vec<generated::Reward> = rewards
            .keyed_rewards
            .iter()
            .map(|(pubkey, r)| generated::Reward {
                pubkey: pubkey.to_string(),
                lamports: r.lamports,
                post_balance: r.post_balance,
                reward_type: match r.reward_type {
                    solana_reward_info::RewardType::Fee => generated::RewardType::Fee as i32,
                    solana_reward_info::RewardType::Rent => generated::RewardType::Rent as i32,
                    solana_reward_info::RewardType::Staking => {
                        generated::RewardType::Staking as i32
                    }
                    solana_reward_info::RewardType::Voting => {
                        generated::RewardType::Voting as i32
                    }
                },
                commission: r.commission.map(|c| c.to_string()).unwrap_or_default(),
            })
            .collect();

        // Encode ConfirmedBlock without transactions, then manually append
        // the pre-encoded transactions (which include the index field).
        let block_shell = generated::ConfirmedBlock {
            previous_blockhash: parent_blockhash.to_string(),
            blockhash: blockhash.to_string(),
            parent_slot,
            transactions: vec![], // encoded separately below
            rewards: rewards_proto,
            block_time: block_time.map(|ts| generated::UnixTimestamp { timestamp: ts }),
            block_height: block_height.map(|h| generated::BlockHeight { block_height: h }),
            num_partitions: rewards
                .num_partitions
                .map(|n| generated::NumPartitions { num_partitions: n }),
        };
        let mut block_bytes = block_shell.encode_to_vec();

        // Append each transaction as field 4 (repeated ConfirmedTransaction) of ConfirmedBlock.
        // In protobuf, repeated message fields are encoded as: [tag][length][message_bytes].
        for tx_bytes in &encoded_txs {
            prost::encoding::bytes::encode(4, tx_bytes, &mut block_bytes);
        }

        // Wrap in BlockMessage: { slot: uint64 (field 1), block: bytes (field 2) }.
        let mut msg_buf = Vec::with_capacity(block_bytes.len() + 20);
        prost::encoding::uint64::encode(1, &slot, &mut msg_buf);
        prost::encoding::bytes::encode(2, &block_bytes, &mut msg_buf);

        // Write length-delimited to stdout.
        let len = msg_buf.len() as u32;
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(&len.to_be_bytes())?;
        stdout.write_all(&msg_buf)?;
        stdout.flush()?;

        // Metrics.
        let total_blocks = self.blocks_emitted.fetch_add(1, Ordering::Relaxed) + 1;
        self.txs_emitted.fetch_add(tx_count, Ordering::Relaxed);

        if total_blocks % 1000 == 0 {
            let total_txs = self.txs_emitted.load(Ordering::Relaxed);
            eprintln!("[datapipes-bridge] slot={slot} blocks={total_blocks} txs={total_txs}");
        }

        Ok(())
    }
}

impl Plugin for DatapipesBridgePlugin {
    fn name(&self) -> &'static str {
        "datapipes-bridge"
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        if transaction.is_vote {
            return async { Ok(()) }.boxed();
        }

        let slot = transaction.slot;
        let index = transaction.transaction_slot_index;

        // Use solana-storage-proto's built-in conversion from SDK types → protobuf.
        let versioned_tx = transaction.transaction.clone();
        let meta = transaction.transaction_status_meta.clone();
        let versioned_with_meta = VersionedTransactionWithStatusMeta {
            transaction: versioned_tx,
            meta,
        };
        let confirmed_tx = generated::ConfirmedTransaction::from(
            TransactionWithStatusMeta::Complete(versioned_with_meta),
        );

        self.pending_txs.entry(slot).or_default().push(PendingTx {
            index: index as u32,
            confirmed_tx,
        });

        async { Ok(()) }.boxed()
    }

    fn on_block<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        let result = self.process_block(block);
        async move { result }.boxed()
    }

    fn on_load(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async {
            eprintln!("[datapipes-bridge] plugin loaded");
            Ok(())
        }
        .boxed()
    }

    fn on_exit(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async {
            let blocks = self.blocks_emitted.load(Ordering::Relaxed);
            let txs = self.txs_emitted.load(Ordering::Relaxed);
            eprintln!("[datapipes-bridge] done — blocks={blocks} txs={txs}");
            Ok(())
        }
        .boxed()
    }
}
