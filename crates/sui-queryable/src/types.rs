use serde::{Deserialize, Serialize};
use sui_types::batch::TxSequenceNumber;

pub const ENTITY_TRANSACTIONS_NAME: &str = "transactions";
pub const ENTITY_EVENTS_NAME: &str = "events";
pub const ENTITY_CALL_TRACES_NAME: &str = "call_traces";

pub const ENTITY_FIELD_ID: &str = "_id";
pub const ENTITY_FIELD_RECORD_VERSION: &str = "record_version";
pub const ENTITY_FIELD_TIME_INDEX: &str = "time_index";
pub const ENTITY_FIELD_TX_INDEX: &str = "tx_index";
pub const ENTITY_FIELD_TX_HASH: &str = "tx_hash";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastExportData {
    pub last_known_tx_version: TxSequenceNumber,
}
