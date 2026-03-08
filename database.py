#!/usr/bin/env python3
"""
Simple Key-Value Store
======================
A persistent, append-only key-value store that supports SET and GET commands.
Data is written to a local file named `data.db` and survives program restarts.

Usage:
    python3 database.py

Commands (entered via standard input):
    SET <key> <value>   Store a value under the given key.
    GET <key>           Retrieve the value for the given key.
    EXIT                Shut down the database cleanly.
"""

import sys
import os

# Path to the persistent log file. Every SET command is appended here.
DB_FILE = "data.db"


# ---------------------------------------------------------------------------
# Custom In-Memory Index
# ---------------------------------------------------------------------------

class KeyValueIndex:
    """
    A simple in-memory index built on top of a plain Python list.

    Internally, each entry is stored as a two-element list [key, value].
    Lookups are done with a linear scan. When a key already exists, its
    value is updated in place so that "last write wins" semantics hold.

    No built-in dictionary or map types are used, as required.
    """

    def __init__(self):
        # Each element is a two-item list: [key, value]
        self._entries = []

    def set(self, key, value):
        """
        Insert a new key-value pair or overwrite the value of an existing key.

        Scans the list linearly. If the key is found, its value is updated
        in place. Otherwise, a new entry is appended to the end of the list.
        """
        for entry in self._entries:
            if entry[0] == key:
                entry[1] = value
                return
        # Key not yet in the index — add it
        self._entries.append([key, value])

    def get(self, key):
        """
        Return the value associated with a key, or None if it does not exist.

        Performs a linear scan from the beginning of the list.
        """
        for entry in self._entries:
            if entry[0] == key:
                return entry[1]
        return None


# ---------------------------------------------------------------------------
# Database — ties the index to the append-only log on disk
# ---------------------------------------------------------------------------

class Database:
    """
    Coordinates the in-memory index with a persistent append-only log file.

    On startup, the log is replayed line by line to reconstruct the full
    state of the index. Every new SET command is immediately flushed to
    disk before returning, so no data is lost if the process crashes.
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.index   = KeyValueIndex()
        self._file   = None          # opened lazily on the first write

        # Rebuild state from the log if it already exists
        self._replay_log()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _replay_log(self):
        """
        Read every line from the log file and feed each SET command back
        into the in-memory index. This restores the database state exactly
        as it was when the process last exited.
        """
        if not os.path.exists(self.db_path):
            return  # Fresh start — nothing to replay

        with open(self.db_path, "r") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                # Each log line has the format: SET <key> <value>
                parts = line.split(" ", 2)
                if len(parts) == 3 and parts[0] == "SET":
                    self.index.set(parts[1], parts[2])

    def _open_log(self):
        """Open the log file in append mode the first time a write occurs."""
        if self._file is None:
            self._file = open(self.db_path, "a")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def set(self, key, value):
        """
        Persist a key-value pair to disk and update the in-memory index.

        The log entry is written and flushed before this method returns,
        ensuring durability even if the process is killed immediately after.
        """
        self._open_log()
        self._file.write(f"SET {key} {value}\n")
        self._file.flush()          # write-through: don't buffer writes
        self.index.set(key, value)

    def get(self, key):
        """Look up a key and return its value, or None if it does not exist."""
        return self.index.get(key)

    def close(self):
        """Flush any remaining data and close the log file gracefully."""
        if self._file is not None:
            self._file.flush()
            self._file.close()
            self._file = None


# ---------------------------------------------------------------------------
# Command-Line Interface
# ---------------------------------------------------------------------------

def parse_command(raw_line):
    """
    Split a raw input line into a (command, args) tuple.

    Returns the upper-cased command name and a list of its arguments.
    Returns (None, []) for empty or whitespace-only lines.
    """
    line = raw_line.strip()
    if not line:
        return None, []
    parts = line.split(" ", 2)   # at most 3 tokens: command, key, value
    return parts[0].upper(), parts[1:]


def main():
    """
    Main read-eval-print loop.

    Reads one command per line from standard input, executes it against the
    database, and prints the result to standard output. All output is flushed
    immediately so that automated testers receive responses without delay.
    """
    db = Database(DB_FILE)

    try:
        for raw_line in sys.stdin:
            command, args = parse_command(raw_line)

            if command is None:
                continue

            # ---- SET -------------------------------------------------------
            if command == "SET":
                if len(args) < 2:
                    print("ERROR: SET requires both a key and a value")
                    sys.stdout.flush()
                    continue
                key, value = args[0], args[1]
                db.set(key, value)
                print("OK")
                sys.stdout.flush()

            # ---- GET -------------------------------------------------------
            elif command == "GET":
                if len(args) < 1:
                    print("ERROR: GET requires a key")
                    sys.stdout.flush()
                    continue
                key = args[0]
                value = db.get(key)
                if value is None:
                    print("NULL")
                else:
                    print(value)
                sys.stdout.flush()

            # ---- EXIT ------------------------------------------------------
            elif command == "EXIT":
                break

            # ---- Unknown ---------------------------------------------------
            else:
                print(f"ERROR: Unknown command '{command}'")
                sys.stdout.flush()

    finally:
        # Always close cleanly, even if the loop exits due to EOF or an error
        db.close()


if __name__ == "__main__":
    main()
