#!/usr/bin/env python3
"""Offline cross-check of classic SPL USDC edges against retained RPC blocks.

Usage: python3 scripts/compare-captured-usdc.py RAW_INPUT_DB INDEX_DB
Reads at most 16 retained blocks / 64 MiB and 100,000 stored USDC rows.
The independent parser excludes transactions with runtime failure evidence.
It does not certify Token-2022 transfers or full-chain/provider completeness.
"""
import json
from pathlib import Path
import sqlite3
import sys

ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"


def base58(text):
    number = 0
    for char in text:
        number = number * 58 + ALPHABET.index(char)
    return b"\0" * (len(text) - len(text.lstrip("1"))) + number.to_bytes(
        (number.bit_length() + 7) // 8, "big"
    )


def runtime_failure(line):
    if not line.startswith("Program ") or " failed:" not in line:
        return False
    try:
        return len(base58(line[8:].split(" failed:", 1)[0])) == 32
    except ValueError:
        return False


def readonly(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)


def compare(source, destination):
    blocks = {}
    size = 0
    for count, (slot, raw) in enumerate(source.execute(
        "SELECT slot,response FROM historical_inputs ORDER BY slot,attempt LIMIT 33"
    ), start=1):
        assert count <= 32, "too many retained block observations"
        size += len(raw)
        assert size <= 64 * 1024 * 1024, "retained input exceeds validation bound"
        blocks[slot] = json.loads(raw)["result"]
        assert len(blocks) <= 16, "too many retained blocks"
    assert blocks, "no retained blocks"
    expected = {}
    for slot, block in blocks.items():
        for tx in block["transactions"]:
            meta = tx["meta"]
            if meta["err"] is not None or any(
                runtime_failure(line) for line in meta.get("logMessages") or []
            ):
                continue
            message = tx["transaction"]["message"]
            loaded = meta.get("loadedAddresses") or {}
            keys = message["accountKeys"] + loaded.get("writable", []) + loaded.get("readonly", [])
            mints = {
                balance["accountIndex"]: balance["mint"]
                for balance in (meta.get("preTokenBalances") or [])
                + (meta.get("postTokenBalances") or [])
            }
            for outer, instruction in enumerate(message["instructions"]):
                instructions = [(None, instruction)]
                for group in meta.get("innerInstructions") or []:
                    if group["index"] == outer:
                        instructions.extend(enumerate(group["instructions"]))
                for inner, instruction in instructions:
                    if keys[instruction["programIdIndex"]] != PROGRAM:
                        continue
                    data, accounts = base58(instruction["data"]), instruction["accounts"]
                    if len(data) == 9 and data[0] == 3:
                        src, dst = accounts[:2]
                        mint = mints.get(src) or mints.get(dst)
                    elif len(data) == 10 and data[0] == 12:
                        src, mint_index, dst = accounts[:3]
                        mint = keys[mint_index]
                    else:
                        continue
                    if mint != MINT:
                        continue
                    signature = tx["transaction"]["signatures"][0]
                    position = "outer" if inner is None else inner
                    identity = f"solana:mainnet-beta:{signature}:{outer}:{position}"
                    assert identity not in expected, "duplicate instruction identity"
                    expected[identity] = (
                        str(int.from_bytes(data[1:9], "little")), keys[src], keys[dst]
                    )
    actual = {}
    for row, in destination.execute(
        "SELECT record_json FROM transfers WHERE mint=? AND slot BETWEEN ? AND ? LIMIT 100001",
        (MINT, min(blocks), max(blocks)),
    ):
        row = json.loads(row)
        assert row["slot"] in blocks, "stored slot lacks retained comparison block"
        actual[row["transfer_id"]] = (
            row["amount_raw"], row["source_token_account"], row["destination_token_account"]
        )
        assert len(actual) <= 100000, "too many stored comparison rows"
    missing = len(expected.keys() - actual.keys())
    extra = len(actual.keys() - expected.keys())
    wrong = sum(actual[key] != expected[key] for key in expected.keys() & actual.keys())
    print(json.dumps(dict(expected=len(expected), stored=len(actual), missing=missing,
                          extra=extra, wrong=wrong, blocks=len(blocks))))
    return bool(missing or extra or wrong)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    with readonly(sys.argv[1]) as source, readonly(sys.argv[2]) as destination:
        sys.exit(compare(source, destination))
