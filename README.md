# Simple Key-Value Store

## Project Overview
This project implements a **persistent key-value store** in Python that supports `SET` and `GET` commands through a command-line interface (CLI).  

All data is stored using an **append-only log** (`data.db`), allowing the program to recover the database state after a restart.

This implementation is compatible with automated **Gradebot testing**.

---

## Features
- **Persistent Storage**: All writes are immediately appended to `data.db`.  
- **Custom In-Memory Index**: Efficient key lookups without using built-in dictionary types.  
- **Overwrite Handling**: The latest `SET` for a key replaces any previous value.  
- **Nonexistent Key Handling**: Safely returns a message if a key does not exist.  
- **Interactive CLI**: Accepts commands from `STDIN` and outputs results to `STDOUT`.  
- **Blackbox Test Ready**: Works with Gradebot automated tests.

---

## Installation & Usage

1. **Clone the repository**:

```bash
git clone https://github.com/Allenpandey1/build-a-database.git
cd build-a-database
