#!/usr/bin/env python3
"""
kvstore.py - Persistent key-value store using append-only log.

Commands:
    SET <key> <value>  - Store or overwrite a key-value pair.
    GET <key>          - Retrieve a value by key.
    EXIT               - Exit the program.

Persistence:
    All SET commands are appended to 'data.db'.
    On startup, the log is replayed to rebuild the in-memory index.
"""

import os
from typing import List, Tuple, Dict

DB_FILE = "data.db"

# ------------------------
# In-memory index
# ------------------------
index_list: List[Tuple[str, str]] = []
key_to_pos: Dict[str, int] = {}

# ------------------------
# Database operations
# ------------------------
def load_db() -> None:
    """
    Load the database from the append-only log.

    Raises:
        OSError: If the file cannot be opened or read.
    """
    if not os.path.exists(DB_FILE):
        return
    with open(DB_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(maxsplit=2)
            if len(parts) != 3:
                continue
            cmd, key, value = parts
            if cmd.upper() == "SET":
                update_index(key, value)

def update_index(key: str, value: str) -> None:
    """
    Update the in-memory index with last-write-wins semantics.

    Args:
        key: The key to store.
        value: The value associated with the key.
    """
    if key in key_to_pos:
        index_list[key_to_pos[key]] = (key, value)
    else:
        index_list.append((key, value))
        key_to_pos[key] = len(index_list) - 1

def append_log(key: str, value: str) -> None:
    """
    Append a SET command to the log file.

    Args:
        key: Key to store.
        value: Value to store.

    Raises:
        OSError: If the file cannot be written.
    """
    with open(DB_FILE, "a", encoding="utf-8") as f:
        f.write(f"SET {key} {value}\n")
        f.flush()

# ------------------------
# Command handlers
# ------------------------
def set_key(key: str, value: str) -> None:
    """
    Handle the SET command.

    Args:
        key: Key to store.
        value: Value to associate with key.
    """
    update_index(key, value)
    append_log(key, value)

def get_key(key: str) -> str:
    """
    Handle the GET command.

    Args:
        key: Key to retrieve.

    Returns:
        The value associated with the key, or empty string if not found.
    """
    if key in key_to_pos:
        return index_list[key_to_pos[key]][1]
    return ""

# ------------------------
# Main loop
# ------------------------
def main() -> None:
    """
    Main loop to read commands from stdin and execute them.

    Supports:
        SET <key> <value>
        GET <key>
        EXIT
    """
    try:
        load_db()
    except OSError as e:
        raise RuntimeError(f"Failed to load DB: {e}") from e

    import sys
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=2)
        cmd = parts[0].upper()
        try:
            if cmd == "SET" and len(parts) == 3:
                set_key(parts[1], parts[2])
                print("OK", flush=True)
            elif cmd == "GET" and len(parts) == 2:
                print(get_key(parts[1]), flush=True)
            elif cmd == "EXIT":
                break
        except OSError as e:
            raise RuntimeError(f"DB error: {e}") from e

if __name__ == "__main__":
    main()
