//! Compare chain identity separately from source-specific execution metadata.
use crate::{Error, Result, model::Transfer};
use serde_json::{Value, json};

pub fn compatible_results(prior: &Value, incoming: &Value) -> Result<()> {
    for path in [
        "/slot",
        "/version",
        "/transaction/signatures",
        "/transaction/message/accountKeys",
    ] {
        if prior.pointer(path) != incoming.pointer(path) {
            return Err(Error("capture_conflict"));
        }
    }
    if prior.pointer("/meta/err").is_some_and(Value::is_null)
        != incoming.pointer("/meta/err").is_some_and(Value::is_null)
    {
        return Err(Error("capture_conflict"));
    }
    let a = prior
        .pointer("/transaction/message/instructions")
        .and_then(Value::as_array)
        .ok_or(Error("stored_record_invalid"))?;
    let b = incoming
        .pointer("/transaction/message/instructions")
        .and_then(Value::as_array)
        .ok_or(Error("invalid_capture_shape"))?;
    if instructions(a) != instructions(b) {
        return Err(Error("capture_conflict"));
    }
    // These fields are absent in some stream representations. Compare every known pair.
    for path in [
        "/blockTime",
        "/transaction/message/header",
        "/transaction/message/recentBlockhash",
        "/transaction/message/config",
        "/transaction/message/transactionConfig",
        "/meta/loadedAddresses",
    ] {
        if let (Some(a), Some(b)) = (
            prior.pointer(path).filter(|v| !v.is_null()),
            incoming.pointer(path).filter(|v| !v.is_null()),
        ) && a != b
        {
            return Err(Error("capture_conflict"));
        }
    }
    if let (Some(a), Some(b)) = (
        prior
            .pointer("/meta/innerInstructions")
            .and_then(Value::as_array),
        incoming
            .pointer("/meta/innerInstructions")
            .and_then(Value::as_array),
    ) && inner(a) != inner(b)
    {
        return Err(Error("capture_conflict"));
    }
    Ok(())
}

/// Keep known optional identity metadata when a later source omits it.
pub fn reference(prior: &Value, incoming: &Value) -> Value {
    match (prior, incoming) {
        (_, Value::Null) => prior.clone(),
        (Value::Object(a), Value::Object(b)) => {
            let mut result = b.clone();
            for (key, old) in a {
                result.insert(
                    key.clone(),
                    reference(old, b.get(key).unwrap_or(&Value::Null)),
                );
            }
            Value::Object(result)
        }
        _ => incoming.clone(),
    }
}
fn instructions(values: &[Value]) -> Vec<Value> {
    values.iter().map(|v|json!({"programIdIndex":v["programIdIndex"],"accounts":v["accounts"],"data":v["data"]})).collect()
}
fn inner(values: &[Value]) -> Vec<Value> {
    let mut groups:Vec<_>=values.iter().map(|v|json!({"index":v["index"],"instructions":instructions(v["instructions"].as_array().map_or(&[],Vec::as_slice))})).collect();
    groups.sort_by_key(|v| v["index"].as_u64());
    groups
}
fn enrich<T: Clone + PartialEq>(prior: &Option<T>, incoming: &Option<T>) -> Result<Option<T>> {
    match (prior, incoming) {
        (Some(a), Some(b)) if a != b => Err(Error("capture_conflict")),
        (Some(a), _) => Ok(Some(a.clone())),
        (_, b) => Ok(b.clone()),
    }
}
pub fn transfer(prior: &Transfer, incoming: &Transfer) -> Result<Transfer> {
    let mut merged = incoming.clone();
    merged.block_time = enrich(&prior.block_time, &incoming.block_time)?;
    merged.source_owner = enrich(&prior.source_owner, &incoming.source_owner)?;
    merged.destination_owner = enrich(&prior.destination_owner, &incoming.destination_owner)?;
    merged.fee_raw = enrich(&prior.fee_raw, &incoming.fee_raw)?;
    let mut comparable = prior.clone();
    comparable.block_time = merged.block_time;
    comparable.source_owner = merged.source_owner.clone();
    comparable.destination_owner = merged.destination_owner.clone();
    comparable.fee_raw = merged.fee_raw;
    if comparable != merged {
        return Err(Error("capture_conflict"));
    }
    Ok(merged)
}
