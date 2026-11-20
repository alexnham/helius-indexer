use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct Capture {
    pub schema_version: u32,
    pub cluster: String,
    pub commitment: String,
    pub source: String,
    pub result: RpcTransaction,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransaction {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub version: Value,
    pub transaction: Transaction,
    pub meta: Meta,
}

#[derive(Debug, Deserialize)]
pub struct Transaction {
    pub signatures: Vec<String>,
    pub message: Message,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub account_keys: Vec<String>,
    pub instructions: Vec<Instruction>,
    #[serde(default)]
    pub address_table_lookups: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Instruction {
    pub program_id_index: usize,
    pub accounts: Vec<usize>,
    pub data: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Meta {
    pub err: Value,
    pub log_messages: Option<Vec<String>>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub pre_token_balances: Option<Vec<TokenBalance>>,
    pub post_token_balances: Option<Vec<TokenBalance>>,
    pub loaded_addresses: Option<LoadedAddresses>,
}

#[derive(Debug, Deserialize)]
pub struct InnerInstructions {
    pub index: usize,
    pub instructions: Vec<Instruction>,
}

#[derive(Debug, Deserialize)]
pub struct LoadedAddresses {
    pub writable: Vec<String>,
    pub readonly: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalance {
    pub account_index: usize,
    pub mint: String,
    pub owner: Option<String>,
    pub program_id: Option<String>,
    pub ui_token_amount: TokenAmount,
}

#[derive(Debug, Deserialize)]
pub struct TokenAmount {
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Transfer {
    pub transfer_id: String,
    pub signature: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub instruction_index: usize,
    pub inner_instruction_index: Option<usize>,
    /// Position inside a p-token `Batch` instruction; absent for ordinary instructions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<usize>,
    pub mint: String,
    pub token_program: String,
    pub source_token_account: String,
    pub destination_token_account: String,
    pub source_owner: Option<String>,
    pub destination_owner: Option<String>,
    #[serde(with = "optional_decimal")]
    pub amount_raw: Option<u64>,
    pub decimals: Option<u8>,
    #[serde(with = "optional_decimal")]
    pub fee_raw: Option<u64>,
    pub confidential: bool,
    pub decoder_version: String,
}

// Decimal JSON strings preserve all u64 values for clients with IEEE-754 numbers.
mod optional_decimal {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};
    pub fn serialize<S: Serializer>(value: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error> {
        value.map(|n| n.to_string()).serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|n| n.parse().map_err(D::Error::custom))
            .transpose()
    }
    use serde::Serialize;
}
