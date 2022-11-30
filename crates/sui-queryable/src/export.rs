use crate::types::{LastExportData, ENTITY_CALL_TRACES_NAME, ENTITY_EVENTS_NAME, ENTITY_FIELD_ID, ENTITY_FIELD_RECORD_VERSION, ENTITY_FIELD_TIME_INDEX, ENTITY_FIELD_TX_HASH, ENTITY_FIELD_TX_INDEX, ENTITY_TRANSACTIONS_NAME, ENTITY_OBJECTS_NAME};
use bcs;
use move_core_types::identifier::Identifier;
use move_core_types::trace::{CallTrace, CallType};
use move_core_types::value::{MoveStruct, MoveValue};
use parking_lot::RwLock;
use queryable_core::constant::PARQUET_METADATA_FIELD_NETWORK_ID;
use queryable_core::datasource_exporter::DatasourceExporter;
use queryable_core::datasource_writer::DatasourceWriter;
use queryable_core::types::entity::{
    Entity, EntityField, EntityFieldEncoding, EntityFieldType, EntityRelation, EntityRelationType,
};
use queryable_core::writer_context::WriterContext;
use std::collections::HashMap;
use std::path::PathBuf;
use queryable_core::entity_writer::EntityWriter;
use sui_types::base_types::{ObjectRef, TransactionDigest};
use sui_types::event::{BalanceChangeType, Event};
use sui_types::messages::{CallArg, SignedTransactionEffects, SingleTransactionKind, TransactionKind, VerifiedCertificate};
use sui_types::object::{Object, Owner};
use sui_types::{batch::TxSequenceNumber, intent::ChainId};
use tracing::{debug, error, info, trace, warn};

#[derive(Debug)]
pub struct QueryableExporter {
    last_tx_version: TxSequenceNumber,

    last_successful_export_tx_version: TxSequenceNumber,

    chain_id: Option<u8>,

    cached_transactions_count: u32,

    datasource_exporter: RwLock<DatasourceExporter>,
    datasource_writer: RwLock<DatasourceWriter>,

    // arg_values_len: u64,
}

// @TODO: export about blockchain overall statistic (peers, validators, mempool size)

impl QueryableExporter {
    pub fn new(config_file_path: PathBuf, chain_id: Option<ChainId>) -> anyhow::Result<Self> {
        info!("Initializing Queryable Exporter");

        let transactions_name = String::from(ENTITY_TRANSACTIONS_NAME);
        let events_name = String::from(ENTITY_EVENTS_NAME);
        let objects_name = String::from(ENTITY_OBJECTS_NAME);
        let call_traces_name = String::from(ENTITY_CALL_TRACES_NAME);

        let entities = vec![
            Entity::new(
                transactions_name.clone(),
                vec![
                    EntityRelation {
                        local_field_name: String::from("relation_events"),
                        remote_entity_name: events_name.clone(),
                        remote_field_name: String::from(ENTITY_FIELD_ID),
                        relation_type: EntityRelationType::OneToMany,
                        eager_fetch: false,
                        nullable: false,
                    },
                    EntityRelation {
                        local_field_name: String::from("relation_objects"),
                        remote_entity_name: objects_name.clone(),
                        remote_field_name: String::from(ENTITY_FIELD_ID),
                        relation_type: EntityRelationType::OneToMany,
                        eager_fetch: false,
                        nullable: false,
                    },
                    EntityRelation {
                        local_field_name: String::from("relation_call_traces"),
                        remote_entity_name: call_traces_name.clone(),
                        remote_field_name: String::from(ENTITY_FIELD_ID),
                        relation_type: EntityRelationType::OneToMany,
                        eager_fetch: false,
                        nullable: false,
                    },
                ],
                vec![
                    EntityField::create_field(
                        String::from(ENTITY_FIELD_ID),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_list_field(
                        String::from("relation_events"),
                        true,
                        EntityFieldType::Uint64(false),
                    )?,
                    EntityField::create_list_field(
                        String::from("relation_objects"),
                        true,
                        EntityFieldType::Uint64(false),
                    )?,
                    EntityField::create_list_field(
                        String::from("relation_call_traces"),
                        true,
                        EntityFieldType::Uint64(false),
                    )?,
                    EntityField::create_field(
                        String::from("record_version"),
                        EntityFieldType::Uint8(true),
                    ),
                    EntityField::create_field(
                        String::from("tx_index"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("time_index"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("certificate_epoch"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("certificate_signature"),
                        EntityFieldType::Binary(true),
                    ),
                    EntityField::create_field(
                        String::from("certificate_signers_map"),
                        EntityFieldType::Binary(true),
                    ),
                    EntityField::create_field(
                        String::from(ENTITY_FIELD_TX_HASH),
                        EntityFieldType::Binary(true),
                    ),
                    EntityField::create_field(String::from("epoch"), EntityFieldType::Uint64(true)),
                    EntityField::create_field(
                        String::from("chain_id"),
                        EntityFieldType::Uint8(true),
                    ),
                    EntityField::create_field(
                        String::from("sender"),
                        EntityFieldType::Binary(false),
                    ),

                    EntityField::create_list_field(
                        String::from("shared_object_refs"),
                        true,
                        EntityFieldType::Binary(false),
                    )?,

                    EntityField::create_list_field(
                        String::from("created_object_refs"),
                        true,
                        EntityFieldType::Binary(false),
                    )?,

                    EntityField::create_list_field(
                        String::from("mutated_object_refs"),
                        true,
                        EntityFieldType::Binary(false),
                    )?,

                    EntityField::create_list_field(
                        String::from("deleted_object_refs"),
                        true,
                        EntityFieldType::Binary(false),
                    )?,

                    EntityField::create_list_field(
                        String::from("unwrapped_object_refs"),
                        true,
                        EntityFieldType::Binary(false),
                    )?,

                    EntityField::create_list_field(
                        String::from("wrapped_object_refs"),
                        true,
                        EntityFieldType::Binary(false),
                    )?,

                    EntityField::create_field(
                        String::from("gas_object"),
                        EntityFieldType::Binary(true),
                    ),

                    EntityField::create_field(
                        String::from("gas_limit"),
                        EntityFieldType::Uint64(false),
                    ),
                    EntityField::create_field(
                        String::from("gas_price"),
                        EntityFieldType::Uint64(false),
                    ),
                    EntityField::create_field(
                        String::from("gas_used"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("gas_computation_cost"),
                        EntityFieldType::Uint64(false),
                    ),
                    EntityField::create_field(
                        String::from("gas_storage_cost"),
                        EntityFieldType::Uint64(false),
                    ),
                    EntityField::create_field(
                        String::from("gas_storage_rebate"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(String::from("type"), EntityFieldType::Uint8(false)),
                    EntityField::create_list_field(
                        String::from("payload"),
                        true,
                        EntityFieldType::Binary(false),
                    )?,
                    EntityField::create_field(
                        String::from("signature"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("success"),
                        EntityFieldType::Boolean(true),
                    ),
                    EntityField::create_field(
                        String::from("detailed_status"),
                        EntityFieldType::Binary(true),
                    ),

                    EntityField::create_list_field(
                        String::from("dependencies"),
                        true,
                        EntityFieldType::Binary(false),
                    )?,

                    EntityField::create_field(String::from("size"), EntityFieldType::Uint32(false)),
                    EntityField::create_field(String::from("time"), EntityFieldType::Uint64(true)),
                    EntityField::create_field(String::from("fee"), EntityFieldType::Uint64(false)),
                ],
                HashMap::from([
                    (
                        String::from(ENTITY_FIELD_ID),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_RECORD_VERSION),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_TX_INDEX),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_TIME_INDEX),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from("relation_events.list.item"),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from("relation_objects.list.item"),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from("relation_call_traces.list.item"),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from("epoch"),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from("chain_id"),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (String::from("time"), EntityFieldEncoding::DeltaBinaryPacked),
                ]),
            )?,
            Entity::new(
                events_name,
                vec![EntityRelation {
                    local_field_name: String::from("relation_transaction"),
                    remote_entity_name: transactions_name.clone(),
                    remote_field_name: String::from(ENTITY_FIELD_ID),
                    relation_type: EntityRelationType::ManyToOne,
                    eager_fetch: true,
                    nullable: false,
                }],
                vec![
                    EntityField::create_field(
                        String::from(ENTITY_FIELD_ID),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("relation_transaction"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("record_version"),
                        EntityFieldType::Uint8(true),
                    ),
                    EntityField::create_field(
                        String::from("tx_index"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("time_index"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("tx_event_id"),
                        EntityFieldType::Uint32(true),
                    ),
                    EntityField::create_field(
                        String::from(ENTITY_FIELD_TX_HASH),
                        EntityFieldType::Binary(true),
                    ),
                    EntityField::create_field(String::from("time"), EntityFieldType::Uint64(true)),
                    EntityField::create_field(
                        String::from("event_type"),
                        EntityFieldType::Uint8(true),
                    ),
                    // shared
                    EntityField::create_field(
                        String::from("common_package_id"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("common_transaction_module"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("common_sender"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("common_recipient_type"),
                        EntityFieldType::Uint8(false),
                    ),
                    EntityField::create_field(
                        String::from("common_recipient"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("common_object_type"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("common_object_id"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("common_version"),
                        EntityFieldType::Uint64(false),
                    ),
                    EntityField::create_field(
                        String::from("move_module_id"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("move_event_name"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_list_field(
                        String::from("move_arg_types"),
                        false,
                        EntityFieldType::Binary(false),
                    )?,
                    EntityField::create_list_field(
                        String::from("move_arg_names"),
                        false,
                        EntityFieldType::Binary(false),
                    )?,
                    EntityField::create_list_field(
                        String::from("move_arg_values"),
                        false,
                        EntityFieldType::Binary(false),
                    )?,
                    EntityField::create_field(
                        String::from("coin_balance_change_change_type"),
                        EntityFieldType::Uint8(false),
                    ),
                    EntityField::create_field(
                        String::from("coin_balance_change_coin_type"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("coin_balance_change_coin_object_id"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("coin_balance_change_version"),
                        EntityFieldType::Uint64(false),
                    ),
                    EntityField::create_field(
                        String::from("coin_balance_change_amount"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("epoch_change_id"),
                        EntityFieldType::Uint64(false),
                    ),
                    EntityField::create_field(
                        String::from("checkpoint"),
                        EntityFieldType::Uint64(false),
                    ),
                ],
                HashMap::from([
                    (
                        String::from(ENTITY_FIELD_ID),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_RECORD_VERSION),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_TX_INDEX),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_TIME_INDEX),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from("relation_transaction"),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_TX_HASH),
                        EntityFieldEncoding::RLEDictionary,
                    ),
                    (String::from("time"), EntityFieldEncoding::DeltaBinaryPacked),
                    (
                        String::from("common_package_id"),
                        EntityFieldEncoding::RLEDictionary,
                    ),
                    (
                        String::from("common_transaction_module"),
                        EntityFieldEncoding::RLEDictionary,
                    ),
                    (
                        String::from("common_sender"),
                        EntityFieldEncoding::RLEDictionary,
                    ),
                    (
                        String::from("common_recipient"),
                        EntityFieldEncoding::RLEDictionary,
                    ),
                    (
                        String::from("common_object_type"),
                        EntityFieldEncoding::RLEDictionary,
                    ),
                    (
                        String::from("common_object_id"),
                        EntityFieldEncoding::RLEDictionary,
                    ),
                    (
                        String::from("move_type"),
                        EntityFieldEncoding::RLEDictionary,
                    ),
                ]),
            )?,
            Entity::new(
                objects_name,
                vec![EntityRelation {
                    local_field_name: String::from("relation_transaction"),
                    remote_entity_name: transactions_name.clone(),
                    remote_field_name: String::from(ENTITY_FIELD_ID),
                    relation_type: EntityRelationType::ManyToOne,
                    eager_fetch: true,
                    nullable: false,
                }],
                vec![
                    EntityField::create_field(
                        String::from(ENTITY_FIELD_ID),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("relation_transaction"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("record_version"),
                        EntityFieldType::Uint8(true),
                    ),
                    EntityField::create_field(
                        String::from("tx_index"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("time_index"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from(ENTITY_FIELD_TX_HASH),
                        EntityFieldType::Binary(true),
                    ),
                    EntityField::create_field(String::from("time"), EntityFieldType::Uint64(true)),

                    EntityField::create_field(
                        String::from("operation_type"),
                        EntityFieldType::Uint8(true),
                    ),

                    EntityField::create_field(
                        String::from("module_address"),
                        EntityFieldType::Binary(false),
                    ),

                    EntityField::create_field(
                        String::from("object_name"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("object_id"),
                        EntityFieldType::Binary(true),
                    ),
                    EntityField::create_field(
                        String::from("object_version"),
                        EntityFieldType::Uint64(true),
                    ),

                    EntityField::create_field(
                        String::from("owner_type"),
                        EntityFieldType::Uint8(false),
                    ),

                    EntityField::create_field(
                        String::from("owner_id"),
                        EntityFieldType::Binary(false),
                    ),

                    EntityField::create_field(
                        String::from("content"),
                        EntityFieldType::Binary(false),
                    ),

                    EntityField::create_field(
                        String::from("storage_rebate"),
                        EntityFieldType::Uint64(false),
                    ),
                ],
                HashMap::from([
                    (
                        String::from(ENTITY_FIELD_ID),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_RECORD_VERSION),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_TIME_INDEX),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from("relation_transaction"),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_TX_HASH),
                        EntityFieldEncoding::RLEDictionary,
                    ),
                ]),
            )?,
            Entity::new(
                call_traces_name,
                vec![EntityRelation {
                    local_field_name: String::from("relation_transaction"),
                    remote_entity_name: transactions_name,
                    remote_field_name: String::from(ENTITY_FIELD_ID),
                    relation_type: EntityRelationType::ManyToOne,
                    eager_fetch: true,
                    nullable: false,
                }],
                vec![
                    EntityField::create_field(
                        String::from(ENTITY_FIELD_ID),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("relation_transaction"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("record_version"),
                        EntityFieldType::Uint8(true),
                    ),
                    EntityField::create_field(
                        String::from("tx_index"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from("time_index"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(
                        String::from(ENTITY_FIELD_TX_HASH),
                        EntityFieldType::Binary(true),
                    ),
                    EntityField::create_field(
                        String::from("tx_type"),
                        EntityFieldType::Uint8(true),
                    ),
                    EntityField::create_field(String::from("depth"), EntityFieldType::Uint32(true)),
                    EntityField::create_field(
                        String::from("call_type"),
                        EntityFieldType::Uint8(true),
                    ),
                    EntityField::create_field(
                        String::from("module_address"),
                        EntityFieldType::Binary(false),
                    ),
                    EntityField::create_field(
                        String::from("method_name"),
                        EntityFieldType::Binary(true),
                    ),
                    EntityField::create_list_field(
                        String::from("ty_args"),
                        false,
                        EntityFieldType::Binary(false),
                    )?,
                    // EntityField::create_list_field(
                    //     String::from("arg_types"),
                    //     false,
                    //     EntityFieldType::Binary(false),
                    // )?,
                    // EntityField::create_list_field(
                    //     String::from("arg_values"),
                    //     false,
                    //     EntityFieldType::LargeBinary(false),
                    // )?,
                    EntityField::create_field(
                        String::from("gas_used"),
                        EntityFieldType::Uint64(true),
                    ),
                    EntityField::create_field(String::from("err"), EntityFieldType::Binary(false)),
                ],
                HashMap::from([
                    (
                        String::from(ENTITY_FIELD_ID),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_RECORD_VERSION),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_TIME_INDEX),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from("relation_transaction"),
                        EntityFieldEncoding::DeltaBinaryPacked,
                    ),
                    (
                        String::from(ENTITY_FIELD_TX_HASH),
                        EntityFieldEncoding::RLEDictionary,
                    ),
                    (
                        String::from("package_id"),
                        EntityFieldEncoding::PlainDictionary,
                    ),
                    (
                        String::from("module_address"),
                        EntityFieldEncoding::PlainDictionary,
                    ),
                    (
                        String::from("method_name"),
                        EntityFieldEncoding::PlainDictionary,
                    ),
                ]),
            )?,
        ];

        let writer_context = WriterContext::read_from_file(config_file_path)?;

        let name = String::from("Sui");
        let description = String::from("Sui DAG Datasource");
        let logo_url = String::from("https://sui.io/img/sui-logo2.svg");

        let datasource_exporter = DatasourceExporter::new(
            writer_context.clone(),
            String::from("sui"),
            entities.clone(),
            name.clone(),
            description.clone(),
            logo_url.clone(),
        );

        let datasource_writer =
            DatasourceWriter::new(writer_context, name, description, logo_url, entities)?;

        let last_exported_data = datasource_writer.get_last_exported_data();

        let last_tx_version: TxSequenceNumber;

        if let Some(last_exported_data) = last_exported_data {
            let last_exported_data: LastExportData = serde_json::from_str(&last_exported_data)?;

            last_tx_version = last_exported_data.last_known_tx_version;
        } else {
            last_tx_version = 0;
        }

        Ok(Self {
            last_tx_version,

            last_successful_export_tx_version: last_tx_version,

            chain_id: chain_id.map(|chain_id| chain_id as u8),

            cached_transactions_count: 0,

            datasource_exporter: RwLock::new(datasource_exporter),
            datasource_writer: RwLock::new(datasource_writer),

            // arg_values_len: 0,
        })
    }

    pub fn set_chain_id(&mut self, chain_id: ChainId) {
        self.chain_id = Some(chain_id as u8);
    }

    pub fn get_cached_transactions_count(&self) -> u32 {
        self.cached_transactions_count
    }

    pub fn get_transactions_per_export(&self) -> u32 {
        let datasource_writer = self.datasource_writer.read();

        datasource_writer.get_blocks_per_export()
    }

    fn export_object(
        entity_writer: &mut EntityWriter,
        record_id: u64,
        transaction_id: u64,
        tx_index: u64,
        time_index: u64,
        tx_hash: &Vec<u8>,
        timestamp_ms: u64,
        operation: u8,
        object_ref: Option<ObjectRef>,
        owner_ref: Option<Owner>,
        object: Option<Object>
    ) -> anyhow::Result<u64> {
        entity_writer.add_value_u64(String::from(ENTITY_FIELD_ID), Some(record_id))?;

        entity_writer
            .add_value_u64(String::from("relation_transaction"), Some(transaction_id))?;

        entity_writer.add_value_u8(String::from(ENTITY_FIELD_RECORD_VERSION), Some(1))?;

        entity_writer.add_value_u64(String::from(ENTITY_FIELD_TX_INDEX), Some(tx_index))?;
        entity_writer.add_value_u64(String::from(ENTITY_FIELD_TIME_INDEX), Some(time_index))?;

        entity_writer
            .add_value_binary(String::from(ENTITY_FIELD_TX_HASH), Some(tx_hash.clone()))?;

        entity_writer.add_value_u64(String::from("time"), Some(timestamp_ms))?;

        entity_writer.add_value_u8(String::from("operation_type"), Some(operation))?;

        let mut owner: Option<Owner> = None;

        let mut module_address: Option<Vec<u8>> = None;
        let mut object_name: Option<Vec<u8>> = None;

        let mut object_id: Option<Vec<u8>> = None;
        let mut object_version: Option<u64> = None;

        let mut owner_type: Option<u8> = None;
        let mut owner_id: Option<Vec<u8>> = None;
        let mut content: Option<Vec<u8>> = None;
        let mut storage_rebate: Option<u64> = None;

        if let Some(object_ref) = object_ref {
            object_id = Some(object_ref.0.to_vec());
            object_version = Some(object_ref.1.value());

            owner = owner_ref;

            content = None;
            storage_rebate = None;
        } else if let Some(object) = object {
            if let Some(struct_type) = object.type_() {
                module_address = Some(struct_type.module_id().short_str_lossless().into_bytes());
                object_name = Some(struct_type.name.clone().into_bytes());
            }

            object_id = Some(object.id().to_vec());
            object_version = Some(object.version().value());

            owner = Some(object.owner);

            content = Some(bcs::to_bytes(&object.data)?);
            storage_rebate = Some(object.storage_rebate);
        }

        if let Some(owner) = owner {
            match owner {
                Owner::ObjectOwner(address) => {
                    owner_type = Some(0);
                    owner_id = Some(address.to_vec());
                }
                Owner::AddressOwner(address) => {
                    owner_type = Some(1);
                    owner_id = Some(address.to_vec());
                }
                Owner::Shared {
                    initial_shared_version,
                } => {
                    owner_type = Some(2);
                    owner_id = Some(Vec::from(initial_shared_version.value().to_be_bytes()));
                }
                Owner::Immutable => {
                    owner_type = Some(3);
                    owner_id = None;
                }
            }
        }

        entity_writer.add_value_binary(String::from("module_address"), module_address)?;
        entity_writer.add_value_binary(String::from("object_name"), object_name)?;
        entity_writer.add_value_binary(String::from("object_id"), object_id)?;
        entity_writer.add_value_u64(String::from("object_version"), object_version)?;

        entity_writer.add_value_u8(String::from("owner_type"), owner_type)?;
        entity_writer.add_value_binary(String::from("owner_id"), owner_id)?;
        entity_writer.add_value_binary(String::from("content"), content)?;
        entity_writer.add_value_u64(String::from("storage_rebate"), storage_rebate)?;

        Ok(record_id)
    }

    pub fn add_transaction(
        &mut self,
        seq: &TxSequenceNumber,
        digest: &TransactionDigest,
        cert: &VerifiedCertificate,
        effects: &SignedTransactionEffects,
        event_move_structs: Vec<Option<MoveStruct>>,
        created_objects: Vec<Option<Object>>,
        mutated_objects: Vec<Option<Object>>,
        call_traces_per_tx_payload: Vec<Vec<CallTrace>>,
        timestamp_ms: u64,
    ) -> anyhow::Result<()> {
        let mut datasource_writer = self.datasource_writer.write();

        debug!("Adding transaction seq {}, digest {:?}", seq, digest);

        let mut event_ids: Vec<Option<u64>> = vec![];
        let mut object_ids: Vec<Option<u64>> = vec![];
        let mut call_trace_ids: Vec<Option<u64>> = vec![];

        let tx_index = *seq;
        let time_index = timestamp_ms / 1000;
        let tx_hash = digest.to_bytes();
        let epoch = cert.auth_sig().epoch;

        let transaction_id =
            datasource_writer.get_next_id(String::from(ENTITY_TRANSACTIONS_NAME))?;

        let transaction_writer = datasource_writer
            .get_writer(String::from(ENTITY_TRANSACTIONS_NAME))
            .unwrap();

        transaction_writer.add_value_u64(String::from(ENTITY_FIELD_ID), Some(transaction_id))?;
        transaction_writer.add_value_u8(String::from(ENTITY_FIELD_RECORD_VERSION), Some(1))?;

        transaction_writer.add_value_u64(String::from(ENTITY_FIELD_TX_INDEX), Some(tx_index))?;
        transaction_writer
            .add_value_u64(String::from(ENTITY_FIELD_TIME_INDEX), Some(time_index))?;

        let cert_epoch = cert.auth_sig().epoch;
        let certificate_signature = Vec::from(cert.auth_sig().signature.as_ref());

        transaction_writer.add_value_u64(String::from("certificate_epoch"), Some(cert_epoch))?;

        transaction_writer.add_value_binary(
            String::from("certificate_signature"),
            Some(certificate_signature),
        )?;

        let mut certificate_signers_map = vec![];

        cert.auth_sig()
            .signers_map
            .serialize_into(&mut certificate_signers_map)?;

        transaction_writer.add_value_binary(
            String::from("certificate_signers_map"),
            Some(certificate_signers_map),
        )?;

        transaction_writer
            .add_value_binary(String::from(ENTITY_FIELD_TX_HASH), Some(tx_hash.clone()))?;

        transaction_writer.add_value_u64(String::from("epoch"), Some(epoch))?;
        transaction_writer.add_value_u8(String::from("chain_id"), self.chain_id)?;

        transaction_writer
            .add_value_binary(String::from("sender"), Some(cert.sender_address().to_vec()))?;

        let mut shared_object_refs = vec![];

        for object in &effects.effects.shared_objects {
            shared_object_refs.push(Some(bcs::to_bytes(object)?));
        }

        transaction_writer
            .add_list_value_binary(String::from("shared_object_refs"), Some(
                shared_object_refs
            ))?;

        let mut created_object_refs = vec![];

        for object in &effects.effects.created {
            created_object_refs.push(Some(bcs::to_bytes(object)?));
        }

        transaction_writer
            .add_list_value_binary(String::from("created_object_refs"), Some(
                created_object_refs
            ))?;

        let mut mutated_object_refs = vec![];

        for object in &effects.effects.mutated {
            mutated_object_refs.push(Some(bcs::to_bytes(object)?));
        }

        transaction_writer
            .add_list_value_binary(String::from("mutated_object_refs"), Some(
                mutated_object_refs
            ))?;

        let mut deleted_object_refs = vec![];

        for object in &effects.effects.deleted {
            deleted_object_refs.push(Some(bcs::to_bytes(object)?));
        }

        transaction_writer
            .add_list_value_binary(String::from("deleted_object_refs"), Some(
                deleted_object_refs
            ))?;

        let mut unwrapped_object_refs = vec![];

        for object in &effects.effects.unwrapped {
            unwrapped_object_refs.push(Some(bcs::to_bytes(object)?));
        }

        transaction_writer
            .add_list_value_binary(String::from("unwrapped_object_refs"), Some(
                unwrapped_object_refs
            ))?;

        let mut wrapped_object_refs = vec![];

        for object in &effects.effects.wrapped {
            wrapped_object_refs.push(Some(bcs::to_bytes(object)?));
        }

        transaction_writer
            .add_list_value_binary(String::from("wrapped_object_refs"), Some(
                wrapped_object_refs
            ))?;

        transaction_writer.add_value_binary(
            String::from("gas_object"),
            Some(bcs::to_bytes(&cert.gas_payment_object_ref())?),
        )?;

        transaction_writer
            .add_value_u64(String::from("gas_limit"), Some(cert.data().data.gas_budget))?;

        transaction_writer
            .add_value_u64(String::from("gas_price"), Some(cert.data().data.gas_price))?;

        transaction_writer.add_value_u64(
            String::from("gas_used"),
            Some(effects.effects.gas_used.gas_used()),
        )?;

        transaction_writer.add_value_u64(
            String::from("gas_computation_cost"),
            Some(effects.effects.gas_cost_summary().computation_cost),
        )?;

        transaction_writer.add_value_u64(
            String::from("gas_storage_cost"),
            Some(effects.effects.gas_cost_summary().storage_cost),
        )?;

        transaction_writer.add_value_u64(
            String::from("gas_storage_rebate"),
            Some(effects.effects.gas_cost_summary().storage_rebate),
        )?;

        transaction_writer
            .add_list_value_binary(String::from("dependencies"), Some(
                effects.effects.dependencies.iter().map(|dependency| Some(dependency.to_bytes())).collect()
            ))?;

        transaction_writer.add_value_u8(
            String::from("type"),
            match cert.data().data.kind {
                TransactionKind::Single(_) => Some(0),
                TransactionKind::Batch(_) => Some(1),
            },
        )?;

        let mut payload: Vec<Option<Vec<u8>>> = vec![];

        for data in cert.data().data.kind.single_transactions() {
            payload.push(Some(serde_json::to_vec(data)?));
        }

        transaction_writer.add_list_value_binary(String::from("payload"), Some(payload))?;

        // EntityField::create_list_field(String::from("type"), true, EntityFieldType::Uint8(false)),
        //
        // EntityField::create_list_field(String::from("transfer_object_recipient"), false,EntityFieldType::Binary(false)),
        // EntityField::create_list_field(String::from("transfer_object_object_ref"), false,EntityFieldType::Binary(false)),
        //
        // EntityField::create_list_field(String::from("pay_coins_object_ids"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        // EntityField::create_list_field(String::from("pay_coins_versions"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Uint64(false))))?,
        // EntityField::create_list_field(String::from("pay_coins_digests"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        // EntityField::create_list_field(String::from("pay_recipients"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        // EntityField::create_list_field(String::from("pay_amounts"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        //
        // EntityField::create_list_field(String::from("pay_sui_coins_object_ids"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        // EntityField::create_list_field(String::from("pay_sui_coins_versions"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Uint64(false))))?,
        // EntityField::create_list_field(String::from("pay_sui_coins_digests"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        // EntityField::create_list_field(String::from("pay_sui_recipients"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        // EntityField::create_list_field(String::from("pay_sui_amounts"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        //
        // EntityField::create_list_field(String::from("pay_all_sui_coins_object_ids"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        // EntityField::create_list_field(String::from("pay_all_sui_coins_versions"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Uint64(false))))?,
        // EntityField::create_list_field(String::from("pay_all_sui_coins_digests"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        // EntityField::create_field(String::from("pay_all_sui_recipient"), EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false)))),
        //
        // EntityField::create_list_field(String::from("publish_package"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        //
        // EntityField::create_field(String::from("call_package_id"), EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false)))),
        // EntityField::create_field(String::from("call_module"), EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false)))),
        // EntityField::create_field(String::from("call_function"), EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false)))),
        // EntityField::create_list_field(String::from("call_ty_args"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        // EntityField::create_list_field(String::from("call_args"), false, EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false))))?,
        //
        // EntityField::create_field(String::from("transfer_sui_recipient"), EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false)))),
        // EntityField::create_field(String::from("transfer_sui_amount"), EntityFieldType::List(false, Box::new(EntityFieldType::Uint64(false)))),
        //
        // EntityField::create_field(String::from("change_epoch_id"), EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false)))),
        // EntityField::create_field(String::from("change_storage_charge"), EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false)))),
        // EntityField::create_field(String::from("change_computation_charge"), EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false)))),
        // EntityField::create_field(String::from("change_storage_rebate"), EntityFieldType::List(false, Box::new(EntityFieldType::Binary(false)))),

        // EntityField::create_list_field(
        //     String::from("payload"),
        //     true,
        //     EntityFieldType::Struct_(
        //         false,
        //         vec![
        //             EntityField::create_field(String::from("type"), EntityFieldType::Uint8(true)),
        //
        //             EntityField::create_field(String::from("transfer_object_recipient"), EntityFieldType::Binary(false)),
        //             EntityField::create_field(String::from("transfer_object_object_ref"), EntityFieldType::Binary(false)),
        //
        //             EntityField::create_list_field(String::from("pay_coins_object_ids"), false, EntityFieldType::Binary(false))?,
        //             EntityField::create_list_field(String::from("pay_coins_versions"), false, EntityFieldType::Uint64(false))?,
        //             EntityField::create_list_field(String::from("pay_coins_digests"), false, EntityFieldType::Binary(false))?,
        //             EntityField::create_list_field(String::from("pay_recipients"), false, EntityFieldType::Binary(false))?,
        //             EntityField::create_list_field(String::from("pay_amounts"), false, EntityFieldType::Binary(false))?,
        //
        //             EntityField::create_list_field(String::from("pay_sui_coins_object_ids"), false, EntityFieldType::Binary(false))?,
        //             EntityField::create_list_field(String::from("pay_sui_coins_versions"), false, EntityFieldType::Uint64(false))?,
        //             EntityField::create_list_field(String::from("pay_sui_coins_digests"), false, EntityFieldType::Binary(false))?,
        //             EntityField::create_list_field(String::from("pay_sui_recipients"), false, EntityFieldType::Binary(false))?,
        //             EntityField::create_list_field(String::from("pay_sui_amounts"), false, EntityFieldType::Binary(false))?,
        //
        //             EntityField::create_list_field(String::from("pay_all_sui_coins_object_ids"), false, EntityFieldType::Binary(false))?,
        //             EntityField::create_list_field(String::from("pay_all_sui_coins_versions"), false, EntityFieldType::Uint64(false))?,
        //             EntityField::create_list_field(String::from("pay_all_sui_coins_digests"), false, EntityFieldType::Binary(false))?,
        //             EntityField::create_field(String::from("pay_all_sui_recipient"), EntityFieldType::Binary(false)),
        //
        //             EntityField::create_list_field(String::from("publish_package"), false, EntityFieldType::Binary(false))?,
        //
        //             EntityField::create_field(String::from("call_package_id"), EntityFieldType::Binary(false)),
        //             EntityField::create_field(String::from("call_module"), EntityFieldType::Binary(false)),
        //             EntityField::create_field(String::from("call_function"), EntityFieldType::Binary(false)),
        //             EntityField::create_list_field(String::from("call_ty_args"), false, EntityFieldType::Binary(false))?,
        //             EntityField::create_list_field(String::from("call_args"), false, EntityFieldType::Binary(false))?,
        //
        //             EntityField::create_field(String::from("transfer_sui_recipient"), EntityFieldType::Binary(false)),
        //             EntityField::create_field(String::from("transfer_sui_amount"), EntityFieldType::Uint64(false)),
        //
        //             EntityField::create_field(String::from("change_epoch_id"), EntityFieldType::Binary(false)),
        //             EntityField::create_field(String::from("change_storage_charge"), EntityFieldType::Binary(false)),
        //             EntityField::create_field(String::from("change_computation_charge"), EntityFieldType::Binary(false)),
        //             EntityField::create_field(String::from("change_storage_rebate"), EntityFieldType::Binary(false)),
        //         ]
        //     )
        // )?,

        transaction_writer.add_value_binary(
            String::from("signature"),
            Some(Vec::from(cert.tx_signature.as_ref())),
        )?;

        transaction_writer.add_value_bool(
            String::from("success"),
            Some(effects.effects.status.is_ok()),
        )?;
        transaction_writer.add_value_binary(
            String::from("detailed_status"),
            Some(serde_json::to_vec(&effects.effects.status)?),
        )?;

        transaction_writer.add_value_u64(String::from("time"), Some(timestamp_ms))?;

        transaction_writer.add_value_u64(
            String::from("fee"),
            Some(effects.effects.gas_used.gas_used() * cert.data().data.gas_price),
        )?;

        transaction_writer.add_value_u32(
            String::from("size"),
            Some(cert.data().data.to_bytes().len() as u32),
        )?;

        ////////////////
        ////////////////
        //////////////// EVENTS
        ////////////////
        ////////////////

        for (j, move_struct) in event_move_structs
            .iter()
            .enumerate()
            .take(effects.effects.events.len())
        {
            let tx_event_id = j as u32;
            let event = &effects.effects.events[j];

            let event_id = datasource_writer.get_next_id(String::from(ENTITY_EVENTS_NAME))?;

            event_ids.push(Some(event_id));

            let event_writer = datasource_writer
                .get_writer(String::from(ENTITY_EVENTS_NAME))
                .unwrap();

            event_writer.add_value_u64(String::from(ENTITY_FIELD_ID), Some(event_id))?;

            event_writer
                .add_value_u64(String::from("relation_transaction"), Some(transaction_id))?;

            event_writer.add_value_u8(String::from(ENTITY_FIELD_RECORD_VERSION), Some(1))?;

            event_writer.add_value_u64(String::from(ENTITY_FIELD_TX_INDEX), Some(tx_index))?;
            event_writer.add_value_u64(String::from(ENTITY_FIELD_TIME_INDEX), Some(time_index))?;

            event_writer.add_value_u32(String::from("tx_event_id"), Some(tx_event_id))?;

            event_writer
                .add_value_binary(String::from(ENTITY_FIELD_TX_HASH), Some(tx_hash.clone()))?;

            event_writer.add_value_u64(String::from("time"), Some(timestamp_ms))?;

            let event_type: Option<u8>;

            let mut common_package_id: Option<Vec<u8>> = None;
            let mut common_transaction_module: Option<Vec<u8>> = None;
            let mut common_sender: Option<Vec<u8>> = None;
            let mut common_recipient_type: Option<u8> = None;
            let mut common_recipient: Option<Vec<u8>> = None;
            let mut common_object_type: Option<Vec<u8>> = None;
            let mut common_object_id: Option<Vec<u8>> = None;
            let mut common_version: Option<u64> = None;

            let mut move_module_id: Option<Vec<u8>> = None;
            let mut move_event_name: Option<Vec<u8>> = None;
            let mut move_arg_types: Option<Vec<Option<Vec<u8>>>> = None;
            let mut move_arg_names: Option<Vec<Option<Vec<u8>>>> = None;
            let mut move_arg_values: Option<Vec<Option<Vec<u8>>>> = None;

            let mut coin_balance_change_change_type: Option<u8> = None;
            let mut coin_balance_change_coin_type: Option<Vec<u8>> = None;
            let mut coin_balance_change_coin_object_id: Option<Vec<u8>> = None;
            let mut coin_balance_change_version: Option<u64> = None;
            let mut coin_balance_change_amount: Option<Vec<u8>> = None;

            let mut epoch_change_id: Option<u64> = None;
            let mut checkpoint: Option<u64> = None;

            match event {
                Event::MoveEvent {
                    package_id,
                    transaction_module,
                    sender,
                    type_,
                    contents: _,
                } => {
                    event_type = Some(0);

                    common_package_id = Some(package_id.to_vec());
                    common_transaction_module = Some(transaction_module.clone().into_bytes());
                    common_sender = Some(sender.to_vec());

                    move_module_id = Some(type_.module_id().short_str_lossless().into_bytes());
                    move_event_name = Some(type_.name.clone().into_bytes());

                    let mut event_payload_types: Vec<Option<Vec<u8>>> = vec![];
                    let mut event_payload_names: Vec<Option<Vec<u8>>> = vec![];
                    let mut event_payload_values: Vec<Option<Vec<u8>>> = vec![];

                    let mut field_identifiers: Vec<&Identifier> = vec![];
                    let mut field_values: Vec<&MoveValue> = vec![];

                    if let Some(move_struct) = &move_struct {
                        match move_struct {
                            MoveStruct::Runtime(values) => {
                                for value in values {
                                    field_values.push(value);
                                }
                            }
                            MoveStruct::WithFields(fields) => {
                                for field in fields {
                                    field_identifiers.push(&field.0);
                                    field_values.push(&field.1);
                                }
                            }
                            MoveStruct::WithTypes { type_: _, fields } => {
                                for field in fields {
                                    field_identifiers.push(&field.0);
                                    field_values.push(&field.1);
                                }
                            }
                        }

                        for (field_index, field_value) in field_values.iter().enumerate() {
                            if let Some(identifier) = field_identifiers.get(field_index) {
                                event_payload_names.push(Some((*identifier).clone().into_bytes()));
                            }

                            // let field_value = field_values[field_index];

                            match field_value {
                                MoveValue::U8(value) => {
                                    event_payload_types.push(Some(Vec::from("u8")));
                                    event_payload_values.push(Some(Vec::from(value.to_be_bytes())));
                                }
                                MoveValue::U64(value) => {
                                    event_payload_types.push(Some(Vec::from("u64")));
                                    event_payload_values.push(Some(Vec::from(value.to_be_bytes())));
                                }
                                MoveValue::U128(value) => {
                                    event_payload_types.push(Some(Vec::from("u128")));
                                    event_payload_values.push(Some(Vec::from(value.to_be_bytes())));
                                }
                                MoveValue::Bool(value) => {
                                    event_payload_types.push(Some(Vec::from("bool")));
                                    event_payload_values.push(Some(Vec::from(if *value {
                                        [1]
                                    } else {
                                        [0]
                                    })));
                                }
                                MoveValue::Address(value) => {
                                    event_payload_types.push(Some(Vec::from("address")));
                                    event_payload_values.push(Some(value.to_vec()));
                                }
                                MoveValue::Vector(values) => {
                                    event_payload_types
                                        .push(Some(Vec::from("vec_json".as_bytes())));

                                    event_payload_values.push(Some(serde_json::to_vec(values)?));
                                }
                                MoveValue::Struct(value) => {
                                    event_payload_types
                                        .push(Some(Vec::from("struct_json".as_bytes())));

                                    event_payload_values.push(Some(serde_json::to_vec(value)?));
                                }

                                MoveValue::Signer(address) => {
                                    event_payload_types.push(Some(Vec::from("signer")));
                                    event_payload_values.push(Some(address.to_vec()));
                                }
                                MoveValue::U16(value) => {
                                    event_payload_types.push(Some(Vec::from("u16")));
                                    event_payload_values.push(Some(Vec::from(value.to_be_bytes())));
                                }
                                MoveValue::U32(value) => {
                                    event_payload_types.push(Some(Vec::from("u32")));
                                    event_payload_values.push(Some(Vec::from(value.to_be_bytes())));
                                }
                                MoveValue::U256(value) => {
                                    event_payload_types.push(Some(Vec::from("u256_le")));
                                    event_payload_values.push(Some(Vec::from(value.to_le_bytes())));
                                }
                            }
                        }
                    } else {
                        warn!(
                            "Failed to find move struct for event by index {}",
                            j.clone()
                        );
                    }

                    move_arg_types = Some(event_payload_types);
                    move_arg_names = Some(event_payload_names);
                    move_arg_values = Some(event_payload_values);
                }
                Event::Publish { sender, package_id } => {
                    event_type = Some(1);

                    common_sender = Some(sender.to_vec());
                    common_package_id = Some(package_id.to_vec());
                }
                Event::CoinBalanceChange {
                    package_id,
                    transaction_module,
                    sender,
                    change_type,
                    owner,
                    coin_type,
                    coin_object_id,
                    version,
                    /// The amount indicate the coin value changes for this event,
                    /// negative amount means spending coin value and positive means receiving coin value.
                    amount,
                } => {
                    event_type = Some(2);

                    common_package_id = Some(package_id.to_vec());
                    common_transaction_module = Some(transaction_module.clone().into_bytes());
                    common_sender = Some(sender.to_vec());
                    coin_balance_change_change_type = match change_type {
                        BalanceChangeType::Gas => Some(0),
                        BalanceChangeType::Pay => Some(1),
                        BalanceChangeType::Receive => Some(2),
                    };

                    match owner {
                        Owner::ObjectOwner(address) => {
                            common_recipient_type = Some(0);
                            common_recipient = Some(address.to_vec());
                        }
                        Owner::AddressOwner(address) => {
                            common_recipient_type = Some(1);
                            common_recipient = Some(address.to_vec());
                        }
                        Owner::Shared {
                            initial_shared_version,
                        } => {
                            common_recipient_type = Some(2);
                            common_recipient =
                                Some(Vec::from(initial_shared_version.value().to_be_bytes()));
                        }
                        Owner::Immutable => {
                            common_recipient_type = Some(3);
                        }
                    }

                    coin_balance_change_coin_type = Some(coin_type.clone().into_bytes());
                    coin_balance_change_coin_object_id = Some(coin_object_id.to_vec());
                    coin_balance_change_version = Some(version.value());
                    coin_balance_change_amount = Some(Vec::from(amount.to_be_bytes()));
                }
                Event::EpochChange(epoch_id) => {
                    event_type = Some(3);

                    epoch_change_id = Some(*epoch_id);
                }
                Event::Checkpoint(checkpoint_sequence_number) => {
                    event_type = Some(4);

                    checkpoint = Some(*checkpoint_sequence_number);
                }
                Event::TransferObject {
                    package_id,
                    transaction_module,
                    sender,
                    recipient,
                    object_type,
                    object_id,
                    version,
                } => {
                    event_type = Some(5);

                    common_package_id = Some(package_id.to_vec());
                    common_transaction_module = Some(transaction_module.clone().into_bytes());
                    common_sender = Some(sender.to_vec());

                    match recipient {
                        Owner::ObjectOwner(address) => {
                            common_recipient_type = Some(0);
                            common_recipient = Some(address.to_vec());
                        }
                        Owner::AddressOwner(address) => {
                            common_recipient_type = Some(1);
                            common_recipient = Some(address.to_vec());
                        }
                        Owner::Shared {
                            initial_shared_version,
                        } => {
                            common_recipient_type = Some(2);
                            common_recipient =
                                Some(Vec::from(initial_shared_version.value().to_be_bytes()));
                        }
                        Owner::Immutable => {
                            common_recipient_type = Some(3);
                        }
                    }

                    common_object_type = Some(object_type.clone().into_bytes());
                    common_object_id = Some(object_id.to_vec());
                    common_version = Some(version.value());
                }
                Event::MutateObject {
                    package_id,
                    transaction_module,
                    sender,
                    object_type,
                    object_id,
                    version,
                } => {
                    event_type = Some(6);

                    common_package_id = Some(package_id.to_vec());
                    common_transaction_module = Some(transaction_module.clone().into_bytes());
                    common_sender = Some(sender.to_vec());
                    common_object_type = Some(object_type.clone().into_bytes());
                    common_object_id = Some(object_id.to_vec());
                    common_version = Some(version.value());
                }
                Event::DeleteObject {
                    package_id,
                    transaction_module,
                    sender,
                    object_id,
                    version,
                } => {
                    event_type = Some(7);

                    common_package_id = Some(package_id.to_vec());
                    common_transaction_module = Some(transaction_module.clone().into_bytes());
                    common_sender = Some(sender.to_vec());
                    common_object_id = Some(object_id.to_vec());
                    common_version = Some(version.value());
                }
                Event::NewObject {
                    package_id,
                    transaction_module,
                    sender,
                    recipient,
                    object_type,
                    object_id,
                    version,
                } => {
                    event_type = Some(8);

                    common_package_id = Some(package_id.to_vec());
                    common_transaction_module = Some(transaction_module.clone().into_bytes());
                    common_sender = Some(sender.to_vec());

                    match recipient {
                        Owner::ObjectOwner(address) => {
                            common_recipient_type = Some(0);
                            common_recipient = Some(address.to_vec());
                        }
                        Owner::AddressOwner(address) => {
                            common_recipient_type = Some(1);
                            common_recipient = Some(address.to_vec());
                        }
                        Owner::Shared {
                            initial_shared_version,
                        } => {
                            common_recipient_type = Some(2);
                            common_recipient =
                                Some(Vec::from(initial_shared_version.value().to_be_bytes()));
                        }
                        Owner::Immutable => {
                            common_recipient_type = Some(3);
                        }
                    }

                    common_object_type = Some(object_type.clone().into_bytes());
                    common_object_id = Some(object_id.to_vec());
                    common_version = Some(version.value());
                }
            }

            event_writer.add_value_u8(String::from("event_type"), event_type)?;

            event_writer.add_value_binary(String::from("common_package_id"), common_package_id)?;
            event_writer.add_value_binary(
                String::from("common_transaction_module"),
                common_transaction_module,
            )?;
            event_writer.add_value_binary(String::from("common_sender"), common_sender)?;
            event_writer
                .add_value_u8(String::from("common_recipient_type"), common_recipient_type)?;
            event_writer.add_value_binary(String::from("common_recipient"), common_recipient)?;
            event_writer
                .add_value_binary(String::from("common_object_type"), common_object_type)?;
            event_writer.add_value_binary(String::from("common_object_id"), common_object_id)?;
            event_writer.add_value_u64(String::from("common_version"), common_version)?;

            event_writer.add_value_binary(String::from("move_module_id"), move_module_id)?;
            event_writer.add_value_binary(String::from("move_event_name"), move_event_name)?;
            event_writer.add_list_value_binary(String::from("move_arg_types"), move_arg_types)?;
            event_writer.add_list_value_binary(String::from("move_arg_names"), move_arg_names)?;
            event_writer.add_list_value_binary(String::from("move_arg_values"), move_arg_values)?;

            event_writer.add_value_u8(
                String::from("coin_balance_change_change_type"),
                coin_balance_change_change_type,
            )?;
            event_writer.add_value_binary(
                String::from("coin_balance_change_coin_type"),
                coin_balance_change_coin_type,
            )?;
            event_writer.add_value_binary(
                String::from("coin_balance_change_coin_object_id"),
                coin_balance_change_coin_object_id,
            )?;
            event_writer.add_value_u64(
                String::from("coin_balance_change_version"),
                coin_balance_change_version,
            )?;
            event_writer.add_value_binary(
                String::from("coin_balance_change_amount"),
                coin_balance_change_amount,
            )?;

            event_writer.add_value_u64(String::from("epoch_change_id"), epoch_change_id)?;
            event_writer.add_value_u64(String::from("checkpoint"), checkpoint)?;

            event_writer.append_record();
        }

        ////////////////
        ////////////////
        //////////////// OBJECTS
        ////////////////
        ////////////////

        for created_object in created_objects {
            if let Some(object) = created_object {
                let object_id= datasource_writer.get_next_id(String::from(ENTITY_OBJECTS_NAME))?;

                let object_writer = datasource_writer
                    .get_writer(String::from(ENTITY_OBJECTS_NAME))
                    .unwrap();

                let object_id = Self::export_object(
                    object_writer,
                    object_id,
                    transaction_id,
                    tx_index,
                    time_index,
                    &tx_hash,
                    timestamp_ms,
                    0,
                    None,
                    None,
                    Some(object)
                )?;

                object_ids.push(Some(object_id));
            }
        }

        for mutated_object in mutated_objects {
            if let Some(object) = mutated_object {
                let object_id= datasource_writer.get_next_id(String::from(ENTITY_OBJECTS_NAME))?;

                let object_writer = datasource_writer
                    .get_writer(String::from(ENTITY_OBJECTS_NAME))
                    .unwrap();

                Self::export_object(
                    object_writer,
                    object_id,
                    transaction_id,
                    tx_index,
                    time_index,
                    &tx_hash,
                    timestamp_ms,
                    1,
                    None,
                    None,
                    Some(object)
                )?;

                object_ids.push(Some(object_id));
            }
        }

        // @TODO: track wraps
        // @TODO: track unwraps
        // @TODO: track transfers

        ////////////////
        ////////////////
        //////////////// CALL TRACES
        ////////////////
        ////////////////

        let single_transactions: Vec<&SingleTransactionKind> = cert.data().data.kind.single_transactions().collect();
        let mut matched_single_tx_index: usize = 0;

        for call_traces in call_traces_per_tx_payload.iter() {
            for call_trace in call_traces {
                let call_trace_id =
                    datasource_writer.get_next_id(String::from(ENTITY_CALL_TRACES_NAME))?;

                call_trace_ids.push(Some(call_trace_id));

                let call_trace_writer = datasource_writer
                    .get_writer(String::from(ENTITY_CALL_TRACES_NAME))
                    .unwrap();

                call_trace_writer.add_value_u64(String::from(ENTITY_FIELD_ID), Some(call_trace_id))?;

                call_trace_writer
                    .add_value_u64(String::from("relation_transaction"), Some(transaction_id))?;

                call_trace_writer.add_value_u8(String::from(ENTITY_FIELD_RECORD_VERSION), Some(1))?;

                call_trace_writer.add_value_u64(String::from(ENTITY_FIELD_TX_INDEX), Some(tx_index))?;
                call_trace_writer
                    .add_value_u64(String::from(ENTITY_FIELD_TIME_INDEX), Some(time_index))?;

                call_trace_writer
                    .add_value_binary(String::from(ENTITY_FIELD_TX_HASH), Some(tx_hash.clone()))?;

                call_trace_writer.add_value_u8(
                    String::from("tx_type"),
                    Some(match cert.is_system_tx() {
                        true => 0,
                        false => 1,
                    }),
                )?;

                call_trace_writer
                    .add_value_u32(String::from("depth"), Some(call_trace.depth as u32))?;
                call_trace_writer.add_value_u8(
                    String::from("call_type"),
                    Some(match call_trace.call_type {
                        CallType::Call => 1,
                        CallType::CallGeneric => 2,
                    }),
                )?;

                call_trace_writer.add_value_binary(
                    String::from("module_address"),
                    if call_trace.module_id.is_some() {
                        Some(Vec::from(call_trace.module_id.clone().unwrap().as_bytes()))
                    } else {
                        None
                    },
                )?;
                call_trace_writer.add_value_binary(
                    String::from("method_name"),
                    Some(call_trace.function.clone().into_bytes()),
                )?;
                call_trace_writer.add_list_value_binary(
                    String::from("ty_args"),
                    Some(
                        call_trace
                            .ty_args
                            .iter()
                            .map(|value| Some(value.clone()))
                            .collect(),
                    ),
                )?;

                // let mut arg_types: Vec<Option<Vec<u8>>> = call_trace
                //     .args_types
                //     .iter()
                //     .map(|value| Some(value.clone()))
                //     .collect();
                //
                // let mut arg_values: Vec<Option<Vec<u8>>> = call_trace
                //     .args_values
                //     .iter()
                //     .map(|value| Some(value.clone()))
                //     .collect();
                //
                // if call_trace.depth == 0 {
                //     loop {
                //         if single_transactions.len() <= matched_single_tx_index {
                //             error!("Unexpected behaviour, skipping optimisation, tx digest {:?}", digest);
                //
                //             break;
                //         }
                //
                //         match &single_transactions[matched_single_tx_index] {
                //             SingleTransactionKind::Call(move_call) => {
                //
                //                 if move_call.arguments.len() + 1 == arg_types.len() {
                //                     // last param is TxContext that is optional
                //
                //                     arg_types.pop();
                //                     arg_values.pop();
                //                 } else if move_call.arguments.len() > arg_types.len() {
                //                     warn!("Unexpected number of arguments in captured call trace");
                //
                //                     break;
                //                 }
                //
                //                 for (index, argument) in move_call.arguments.iter().enumerate() {
                //                     match argument {
                //                         CallArg::Pure(_) => {
                //                             continue;
                //                         }
                //                         CallArg::Object(object_arg) => {
                //                             // arg_types[index] = Some(Vec::from("object_ref".as_bytes()));
                //                             // arg_values[index] = Some(bcs::to_bytes(&object_arg)?);
                //                         }
                //                         CallArg::ObjVec(object_args) => {
                //                             // arg_types[index] = Some(Vec::from("object_refs".as_bytes()));
                //                             // arg_values[index] = Some(bcs::to_bytes(&object_args)?);
                //                         }
                //                     }
                //                 }
                //
                //                 break;
                //             }
                //             SingleTransactionKind::Publish(_) => {
                //                 break;
                //             }
                //             _ => {
                //                 matched_single_tx_index += 1;
                //             }
                //         }
                //     }
                // }
                //
                // call_trace_writer.add_list_value_binary(
                //     String::from("arg_types"),
                //     Some(arg_types),
                // )?;
                //
                // call_trace_writer.add_list_value_large_binary(
                //     String::from("arg_values"),
                //     Some(arg_values),
                // )?;

                let gas_used = call_trace.gas_used;

                call_trace_writer.add_value_u64(String::from("gas_used"), Some(gas_used))?;

                call_trace_writer.add_value_binary(
                    String::from("err"),
                    if call_trace.err.is_some() {
                        Some(serde_json::to_vec(&call_trace.err.as_ref().unwrap())?)
                    } else {
                        None
                    },
                )?;

                call_trace_writer.append_record();
            }

            matched_single_tx_index += 1;
        }

        let transaction_writer = datasource_writer
            .get_writer(String::from(ENTITY_TRANSACTIONS_NAME))
            .unwrap();

        transaction_writer.add_list_value_u64(
            String::from("relation_events"),
            Some(event_ids.clone())
        )?;
        transaction_writer.add_list_value_u64(
            String::from("relation_objects"),
            Some(object_ids.clone())
        )?;
        transaction_writer.add_list_value_u64(
            String::from("relation_call_traces"),
            Some(call_trace_ids.clone()),
        )?;

        transaction_writer.append_record();

        self.last_tx_version = *seq;

        self.cached_transactions_count += 1;

        Ok(())
    }

    pub async fn export(&mut self) -> anyhow::Result<()> {
        info!(
            "Exporting till transaction seq {:?}",
            self.last_tx_version()
        );

        // Cleaning
        self.cached_transactions_count = 0;

        // Exporting
        let exported_data_state = LastExportData {
            last_known_tx_version: self.last_tx_version,
        };

        info!("Exporting Queryable data stream");

        debug!("Trying to lock datasource");
        let mut datasource_writer = self.datasource_writer.write();

        debug!("Locked datasource");

        let exported_state = serde_json::to_string(&exported_data_state)?;
        let result = datasource_writer.export_local();

        match result {
            Err(err) => {
                error!("Failed to export, error: {:?}", err);

                datasource_writer.clean();

                return Err(err.into());
            }
            _ => {
                let local_block_items = datasource_writer.local_block_items();
                let mut published_block_items = datasource_writer.published_block_items().clone();

                debug!("Locking datasource exporter");

                let datasource_exporter = self.datasource_exporter.write();

                debug!("Locked datasource exporter");

                datasource_exporter
                    .export(
                        local_block_items,
                        &mut published_block_items,
                        HashMap::from([(
                            String::from(PARQUET_METADATA_FIELD_NETWORK_ID),
                            (self.chain_id.unwrap_or(ChainId::Testing as u8)).to_string(),
                        )]),
                    )
                    .await?;

                debug!("Datasource exporter exported");

                datasource_writer.confirm_export(published_block_items, Some(exported_state))?;

                debug!("Datasource writer confirmed export");
            }
        }

        self.last_successful_export_tx_version = self.last_tx_version;

        info!("Exported Queryable data stream");

        // self.arg_values_len = 0;

        Ok(())
    }

    pub fn last_tx_version(&self) -> TxSequenceNumber {
        self.last_tx_version
    }

    pub fn last_successful_export_tx_version(&self) -> TxSequenceNumber {
        self.last_successful_export_tx_version
    }

    pub fn reset_cached_data(&mut self) -> anyhow::Result<()> {
        self.last_tx_version = self.last_successful_export_tx_version;

        self.cached_transactions_count = 0;

        let mut datasource_writer = self.datasource_writer.write();

        datasource_writer.clean();

        Ok(())
    }
}
