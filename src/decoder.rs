use crate::{Error, Result, model::*};
use serde_json::Value;
use std::collections::HashSet;

pub const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
pub const DECODER_VERSION: &str = "token-transfers-v7";
pub const MAX_CAPTURE_BYTES: usize = 16 * 1024 * 1024;

pub fn parse_capture(bytes: &[u8]) -> Result<Capture> {
    if bytes.len() > MAX_CAPTURE_BYTES {
        return Err(Error("capture_too_large"));
    }
    let value: Value = serde_json::from_slice(bytes).map_err(|_| Error("invalid_json"))?;
    // Missing execution status must never deserialize as a successful null value.
    if value.pointer("/result/meta/err").is_none() {
        return Err(Error("missing_execution_status"));
    }
    let capture: Capture =
        serde_json::from_value(value).map_err(|_| Error("invalid_capture_shape"))?;
    if capture.schema_version != 1
        || capture.cluster != "mainnet-beta"
        || capture.commitment != "finalized"
    {
        return Err(Error("unsupported_capture_contract"));
    }
    if !matches!(
        capture.source.as_str(),
        "solana-mainnet-rpc"
            | "fixture"
            | "helius-laserstream"
            | "old-faithful-jetstreamer"
            | "archive-rpc-corroborated"
    ) {
        return Err(Error("unsupported_capture_source"));
    }
    let tx = &capture.result;
    if tx.version != "legacy" && tx.version != 0 && tx.version != 1 {
        return Err(Error("unsupported_transaction_version"));
    }
    if tx.slot > i64::MAX as u64 {
        return Err(Error("unsupported_slot"));
    }
    if tx.transaction.signatures.is_empty() {
        return Err(Error("missing_signature"));
    }
    for signature in &tx.transaction.signatures {
        validate_base58(signature, 64)?;
    }
    Ok(capture)
}

fn validate_base58(text: &str, len: usize) -> Result<()> {
    if text.len() > 100 {
        return Err(Error("invalid_base58_identifier"));
    }
    let bytes = bs58::decode(text)
        .into_vec()
        .map_err(|_| Error("invalid_base58_identifier"))?;
    if bytes.len() != len {
        return Err(Error("invalid_base58_identifier"));
    }
    Ok(())
}

pub fn decode(capture: &Capture) -> Result<Vec<Transfer>> {
    let tx = &capture.result;
    if !tx.meta.err.is_null() {
        return Ok(Vec::new());
    }
    // Absent inner metadata is unknown, not proof of no inner transfers.
    let groups = tx
        .meta
        .inner_instructions
        .as_ref()
        .ok_or(Error("missing_inner_instructions"))?;
    let message = &tx.transaction.message;
    let mut keys = message.account_keys.clone();
    if let Some(loaded) = &tx.meta.loaded_addresses {
        keys.extend(loaded.writable.iter().cloned());
        keys.extend(loaded.readonly.iter().cloned());
    } else if !message.address_table_lookups.is_empty() {
        return Err(Error("missing_loaded_addresses"));
    }
    if keys.is_empty() || keys.len() > 256 {
        return Err(Error("invalid_account_keys"));
    }
    for key in &keys {
        validate_base58(key, 32)?;
    }
    let mut seen_groups = HashSet::new();
    for group in groups {
        if group.index >= message.instructions.len() || !seen_groups.insert(group.index) {
            return Err(Error("invalid_inner_instruction_group"));
        }
    }
    for balances in [&tx.meta.pre_token_balances, &tx.meta.post_token_balances]
        .into_iter()
        .flatten()
    {
        let mut seen = HashSet::new();
        for balance in balances {
            if balance.account_index >= keys.len() || !seen.insert(balance.account_index) {
                return Err(Error("invalid_token_balance_index"));
            }
            validate_base58(&balance.mint, 32)?;
            if let Some(owner) = &balance.owner {
                validate_base58(owner, 32)?;
            }
            if let Some(program) = &balance.program_id {
                validate_base58(program, 32)?;
            }
        }
    }
    let mut output = Vec::new();
    let mut changed_owners = HashSet::new();
    for token_program in [SPL_TOKEN, TOKEN_2022] {
        let has_inner_token = groups.iter().flat_map(|g| &g.instructions).any(|ix| {
            keys.get(ix.program_id_index)
                .is_some_and(|p| p == token_program)
        });
        if has_inner_token {
            let logs = tx
                .meta
                .log_messages
                .as_ref()
                .ok_or(Error("missing_execution_evidence"))?;
            let invoke_prefix = format!("Program {token_program} invoke [");
            let success = format!("Program {token_program} success");
            let expected = message
                .instructions
                .iter()
                .chain(groups.iter().flat_map(|g| &g.instructions))
                .filter(|ix| {
                    keys.get(ix.program_id_index)
                        .is_some_and(|p| p == token_program)
                })
                .count();
            if logs.iter().any(|line| runtime_failure(line)) {
                return Err(Error("unsupported_caught_token_failure"));
            }
            if logs
                .iter()
                .filter(|line| line.starts_with(&invoke_prefix))
                .count()
                != expected
                || logs.iter().filter(|line| *line == &success).count() != expected
            {
                return Err(Error("incomplete_execution_evidence"));
            }
        }
    }
    for (outer, instruction) in message.instructions.iter().enumerate() {
        decode_instruction(
            tx,
            &keys,
            instruction,
            outer,
            None,
            &mut output,
            &mut changed_owners,
        )?;
        if let Some(group) = groups.iter().find(|g| g.index == outer) {
            for (inner, instruction) in group.instructions.iter().enumerate() {
                decode_instruction(
                    tx,
                    &keys,
                    instruction,
                    outer,
                    Some(inner),
                    &mut output,
                    &mut changed_owners,
                )?;
            }
        }
    }
    for transfer in &mut output {
        if changed_owners.contains(&transfer.source_token_account) {
            transfer.source_owner = None;
        }
        if changed_owners.contains(&transfer.destination_token_account) {
            transfer.destination_owner = None;
        }
    }
    Ok(output)
}

pub(crate) fn runtime_failure(line: &str) -> bool {
    line.strip_prefix("Program ")
        .and_then(|rest| rest.split_once(" failed:"))
        .is_some_and(|(program, _)| validate_base58(program, 32).is_ok())
}

fn balance(meta: &Meta, index: usize, pre: bool) -> Option<&TokenBalance> {
    let balances = if pre {
        &meta.pre_token_balances
    } else {
        &meta.post_token_balances
    };
    balances.as_ref()?.iter().find(|b| b.account_index == index)
}

fn owner(meta: &Meta, index: usize) -> Option<String> {
    let pre = balance(meta, index, true).and_then(|b| b.owner.as_ref());
    let post = balance(meta, index, false).and_then(|b| b.owner.as_ref());
    // Transaction-end observations alone cannot disambiguate an ownership change.
    match (pre, post) {
        (Some(a), Some(b)) if a == b => Some(a.clone()),
        _ => None,
    }
}

fn decode_instruction(
    tx: &RpcTransaction,
    keys: &[String],
    ix: &Instruction,
    outer: usize,
    inner: Option<usize>,
    output: &mut Vec<Transfer>,
    changed_owners: &mut HashSet<String>,
) -> Result<()> {
    let program = keys
        .get(ix.program_id_index)
        .ok_or(Error("invalid_instruction_index"))?;
    if ix.accounts.iter().any(|&a| a >= keys.len()) {
        return Err(Error("invalid_instruction_index"));
    }
    if program != SPL_TOKEN && program != TOKEN_2022 {
        return Ok(());
    }
    if ix.data.len() > 2048 {
        return Err(Error("invalid_token_instruction"));
    }
    let data = bs58::decode(&ix.data)
        .into_vec()
        .map_err(|_| Error("invalid_token_instruction"))?;
    let mut data = data;
    let mut fee_raw = if program == SPL_TOKEN { Some(0) } else { None };
    if data.first() == Some(&6) {
        if data.len() < 3 || ix.accounts.len() < 2 {
            return Err(Error("invalid_authority_instruction"));
        }
        if data[1] > 3 {
            return Err(Error("unsupported_authority_change"));
        }
        if !matches!((data[2], data.len()), (0, 3) | (1, 35)) || (data[1] == 2 && data[2] == 0) {
            return Err(Error("invalid_authority_instruction"));
        }
        // Agreement at transaction boundaries cannot resolve change-and-restore.
        if data[1] == 2 {
            changed_owners.insert(keys[ix.accounts[0]].clone());
        }
        return Ok(());
    }
    // Fee collection moves withheld balances, not an ordinary instructed edge.
    if program == TOKEN_2022 && data.first() == Some(&26) {
        let minimum = match data.get(1) {
            Some(2) => Some((2, 3)),
            Some(3) if data.len() == 3 => Some((3, 3 + usize::from(data[2]))),
            Some(3) => return Err(Error("invalid_fee_maintenance_instruction")),
            Some(4) => Some((2, 1)),
            _ => None,
        };
        if let Some((length, accounts)) = minimum {
            if data.len() != length || ix.accounts.len() < accounts {
                return Err(Error("invalid_fee_maintenance_instruction"));
            }
            return Ok(());
        }
    }
    // Unwrapping moves native lamports, which are outside the token-transfer index.
    if data.first() == Some(&45)
        && ix.accounts.len() >= 3
        && (data.as_slice() == [45, 0] || (data.len() == 10 && data[1] == 1))
    {
        return Ok(());
    }
    // A successful checked-with-fee call validates this fee against execution-time mint state.
    if program == TOKEN_2022 && data.first() == Some(&26) {
        if data.len() != 19 || data[1] != 1 {
            return Err(Error("unsupported_transfer_fee_instruction"));
        }
        fee_raw = Some(u64::from_le_bytes(data[11..19].try_into().unwrap()));
        data = [&[12], &data[2..11]].concat();
    }
    let tag = *data.first().ok_or(Error("invalid_token_instruction"))?;
    let confidential = program == TOKEN_2022 && tag == 27;
    let (source, destination, explicit) = if confidential {
        let variant = *data
            .get(1)
            .ok_or(Error("invalid_confidential_instruction"))?;
        let (length, accounts) = match variant {
            // Two discriminators, 36-byte decryptable balance, two 64-byte
            // auditor ciphertexts, then three/five proof instruction offsets.
            7 => (169, 4),
            13 => (171, 4),
            // Configuration and movements within one account do not form edges.
            0 => (67, 1),
            1 => (35, 2),
            2 => (47, 4),
            3 => (2, 3),
            4 => (3, 3),
            5 => (11, 3),
            6 => (49, 4),
            8 => (46, 2),
            9..=12 => (2, 2),
            14 => (2, 3),
            _ => return Err(Error("unsupported_confidential_instruction")),
        };
        if data.len() != length || ix.accounts.len() < accounts {
            return Err(Error("invalid_confidential_instruction"));
        }
        if !matches!(variant, 7 | 13) {
            return Ok(());
        }
        // A proof offset of zero consumes a context account; any nonzero
        // offset requires one shared instructions sysvar, plus the authority.
        // Equality, validity and range proofs are always read; TransferWithFee's
        // middle fee offsets are read only for fee mints, unknown from the instruction.
        let offsets = [data[166], data[167], data[length - 1]];
        let proof_accounts = offsets.iter().filter(|&&offset| offset == 0).count()
            + usize::from(offsets.iter().any(|&offset| offset != 0));
        if ix.accounts.len() < 4 + proof_accounts {
            return Err(Error("invalid_confidential_instruction"));
        }
        // Token-2022 rejects plain confidential Transfer on transfer-fee mints.
        if variant == 7 {
            fee_raw = Some(0);
        }
        (
            ix.accounts[0],
            ix.accounts[2],
            Some((&keys[ix.accounts[1]], None)),
        )
    } else {
        match tag {
            3 if data.len() == 9 && ix.accounts.len() >= 3 => {
                (ix.accounts[0], ix.accounts[1], None)
            }
            12 if data.len() == 10 && ix.accounts.len() >= 4 => (
                ix.accounts[0],
                ix.accounts[2],
                Some((&keys[ix.accounts[1]], Some(data[9]))),
            ),
            3 | 12 => return Err(Error("invalid_transfer_instruction")),
            0..=24 => return Ok(()),
            // InitializeMintCloseAuthority and InitializePermanentDelegate configure a mint.
            25 | 35 if program == TOKEN_2022 => return Ok(()),
            _ => return Err(Error("unsupported_token_instruction")),
        }
    };
    let mut token = explicit.map(|(mint, decimals)| (mint.clone(), decimals));
    for index in [source, destination] {
        for pre in [true, false] {
            if let Some(b) = balance(&tx.meta, index, pre) {
                if b.program_id.as_deref().is_some_and(|p| p != program) {
                    return Err(Error("conflicting_token_program"));
                }
                let observed = (b.mint.clone(), Some(b.ui_token_amount.decimals));
                if token.as_ref().is_some_and(|t| {
                    t.0 != observed.0 || t.1.is_some_and(|d| Some(d) != observed.1)
                }) {
                    return Err(Error("conflicting_token_metadata"));
                }
                token = Some(observed);
            }
        }
    }
    let (mint, decimals) = token.ok_or(Error("missing_token_metadata"))?;
    let amount_raw = if confidential {
        None
    } else {
        Some(u64::from_le_bytes(data[1..9].try_into().unwrap()))
    };
    if program == TOKEN_2022 && !confidential {
        let amount = amount_raw.unwrap();
        if fee_raw.is_some_and(|fee| fee > amount) {
            return Err(Error("invalid_transfer_fee"));
        }
        if source == destination || amount == 0 || tag == 3 {
            fee_raw = Some(0);
        }
    }
    let signature = tx
        .transaction
        .signatures
        .first()
        .ok_or(Error("missing_signature"))?;
    let position = inner.map_or_else(|| "outer".to_owned(), |n| n.to_string());
    output.push(Transfer {
        transfer_id: format!("solana:mainnet-beta:{signature}:{outer}:{position}"),
        signature: signature.clone(),
        slot: tx.slot,
        block_time: tx.block_time,
        instruction_index: outer,
        inner_instruction_index: inner,
        mint,
        token_program: program.clone(),
        source_token_account: keys[source].clone(),
        destination_token_account: keys[destination].clone(),
        source_owner: owner(&tx.meta, source),
        destination_owner: owner(&tx.meta, destination),
        amount_raw,
        decimals,
        fee_raw,
        confidential,
        decoder_version: DECODER_VERSION.to_owned(),
    });
    Ok(())
}
