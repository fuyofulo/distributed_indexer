use std::collections::HashSet;

use base64::{Engine as _, engine::general_purpose};
use prost::Message as _;
use serde::Serialize;
use solana_sdk::bs58;
use yellowstone_grpc_proto::prelude::{
    Message, SubscribeUpdate, SubscribeUpdateTransactionInfo, subscribe_update::UpdateOneof,
};

#[derive(Debug, Serialize)]
pub struct KafkaPayload {
    pub event_id: String,
    pub event_type: String,
    pub slot: Option<u64>,
    pub signature: Option<String>,
    pub program_ids: Vec<String>,
    pub filters: Vec<String>,
    pub created_at: Option<String>,
    pub account_pubkey: Option<String>,
    pub account_owner: Option<String>,
    pub raw_base64: String,
}

pub fn build_payload(update: &SubscribeUpdate) -> KafkaPayload {
    normalize(update)
}

pub fn serialize_payload(payload: &KafkaPayload) -> String {
    serde_json::to_string(payload).expect("Kafka payload serialization failed")
}

fn normalize(update: &SubscribeUpdate) -> KafkaPayload {
    let filters = update.filters.clone();
    let created_at = update
        .created_at
        .as_ref()
        .map(|ts| format!("{}.{}", ts.seconds, ts.nanos));
    let raw_base64 = general_purpose::STANDARD.encode(update.encode_to_vec());

    match update.update_oneof.as_ref() {
        Some(UpdateOneof::Account(account_update)) => {
            let slot = Some(account_update.slot);
            let mut account_pubkey = None;
            let mut account_owner = None;
            let mut signature = None;
            let mut program_ids = Vec::new();
            let mut write_version = None;

            if let Some(account) = account_update.account.as_ref() {
                let pubkey = bs58::encode(&account.pubkey).into_string();
                let owner = bs58::encode(&account.owner).into_string();
                account_pubkey = Some(pubkey.clone());
                account_owner = Some(owner.clone());
                program_ids.push(owner);
                write_version = Some(account.write_version);
                signature = account
                    .txn_signature
                    .as_ref()
                    .map(|sig| bs58::encode(sig).into_string());
            }

            let event_id = match (&account_pubkey, write_version) {
                (Some(pubkey), Some(version)) => {
                    format!("account:{}:{}:{}", pubkey, account_update.slot, version)
                }
                _ => format!("account:unknown:{}", account_update.slot),
            };

            KafkaPayload {
                event_id,
                event_type: "account".to_string(),
                slot,
                signature,
                program_ids,
                filters,
                created_at,
                account_pubkey,
                account_owner,
                raw_base64,
            }
        }
        Some(UpdateOneof::Transaction(tx_update)) => {
            let slot = Some(tx_update.slot);
            let mut signature = None;
            let mut index = None;
            let mut program_ids = Vec::new();

            if let Some(tx_info) = tx_update.transaction.as_ref() {
                signature = Some(bs58::encode(&tx_info.signature).into_string());
                index = Some(tx_info.index);
                program_ids = extract_program_ids_from_transaction(tx_info);
            }

            let event_id = match (&signature, index) {
                (Some(sig), Some(idx)) => format!("transaction:{}:{}:{}", sig, tx_update.slot, idx),
                _ => format!("transaction:unknown:{}", tx_update.slot),
            };

            KafkaPayload {
                event_id,
                event_type: "transaction".to_string(),
                slot,
                signature,
                program_ids,
                filters,
                created_at,
                account_pubkey: None,
                account_owner: None,
                raw_base64,
            }
        }
        Some(UpdateOneof::TransactionStatus(tx_status)) => {
            let slot = Some(tx_status.slot);
            let signature = Some(bs58::encode(&tx_status.signature).into_string());
            let event_id = format!(
                "transaction_status:{}:{}:{}",
                signature.as_deref().unwrap_or("unknown"),
                tx_status.slot,
                tx_status.index
            );

            KafkaPayload {
                event_id,
                event_type: "transaction_status".to_string(),
                slot,
                signature,
                program_ids: Vec::new(),
                filters,
                created_at,
                account_pubkey: None,
                account_owner: None,
                raw_base64,
            }
        }
        Some(UpdateOneof::Slot(slot_update)) => {
            let event_id = format!("slot:{}:{}", slot_update.slot, slot_update.status);

            KafkaPayload {
                event_id,
                event_type: "slot".to_string(),
                slot: Some(slot_update.slot),
                signature: None,
                program_ids: Vec::new(),
                filters,
                created_at,
                account_pubkey: None,
                account_owner: None,
                raw_base64,
            }
        }
        Some(UpdateOneof::Block(block_update)) => {
            let event_id = format!("block:{}", block_update.slot);

            KafkaPayload {
                event_id,
                event_type: "block".to_string(),
                slot: Some(block_update.slot),
                signature: None,
                program_ids: Vec::new(),
                filters,
                created_at,
                account_pubkey: None,
                account_owner: None,
                raw_base64,
            }
        }
        Some(UpdateOneof::BlockMeta(block_meta)) => {
            let event_id = format!("block_meta:{}", block_meta.slot);

            KafkaPayload {
                event_id,
                event_type: "block_meta".to_string(),
                slot: Some(block_meta.slot),
                signature: None,
                program_ids: Vec::new(),
                filters,
                created_at,
                account_pubkey: None,
                account_owner: None,
                raw_base64,
            }
        }
        Some(UpdateOneof::Entry(entry)) => {
            let event_id = format!("entry:{}:{}", entry.slot, entry.index);

            KafkaPayload {
                event_id,
                event_type: "entry".to_string(),
                slot: Some(entry.slot),
                signature: None,
                program_ids: Vec::new(),
                filters,
                created_at,
                account_pubkey: None,
                account_owner: None,
                raw_base64,
            }
        }
        Some(UpdateOneof::Ping(_)) => KafkaPayload {
            event_id: "ping".to_string(),
            event_type: "ping".to_string(),
            slot: None,
            signature: None,
            program_ids: Vec::new(),
            filters,
            created_at,
            account_pubkey: None,
            account_owner: None,
            raw_base64,
        },
        Some(UpdateOneof::Pong(pong)) => KafkaPayload {
            event_id: format!("pong:{}", pong.id),
            event_type: "pong".to_string(),
            slot: None,
            signature: None,
            program_ids: Vec::new(),
            filters,
            created_at,
            account_pubkey: None,
            account_owner: None,
            raw_base64,
        },
        None => KafkaPayload {
            event_id: "unknown".to_string(),
            event_type: "unknown".to_string(),
            slot: None,
            signature: None,
            program_ids: Vec::new(),
            filters,
            created_at,
            account_pubkey: None,
            account_owner: None,
            raw_base64,
        },
    }
}

fn extract_program_ids_from_transaction(tx_info: &SubscribeUpdateTransactionInfo) -> Vec<String> {
    let Some(tx) = tx_info.transaction.as_ref() else {
        return Vec::new();
    };
    let Some(message) = tx.message.as_ref() else {
        return Vec::new();
    };

    program_ids_from_message(message)
}

fn program_ids_from_message(message: &Message) -> Vec<String> {
    let mut program_ids = HashSet::new();

    for instruction in &message.instructions {
        let program_index = instruction.program_id_index as usize;
        if let Some(program_id_bytes) = message.account_keys.get(program_index) {
            program_ids.insert(bs58::encode(program_id_bytes).into_string());
        }
    }

    program_ids.into_iter().collect()
}
