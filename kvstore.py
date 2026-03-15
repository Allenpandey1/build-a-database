import sys
import os

DATA_FILE = "data.db"
store = []   # list used as index (not dictionary)

def load_data():
    if not os.path.exists(DATA_FILE):
        return

    with open(DATA_FILE, "r") as f:
        for line in f:
            parts = line.strip().split(" ", 2)
            if parts[0] == "SET":
                set_key(parts[1], parts[2], False)

def set_key(key, value, persist=True):
    global store

    for i in range(len(store)):
        if store[i][0] == key:
            store[i] = (key, value)
            break
    else:
        store.append((key, value))

    if persist:
        with open(DATA_FILE, "a") as f:
            f.write(f"SET {key} {value}\n")

def get_key(key):
    for k, v in store:
        if k == key:
            return v
    return "NULL"

def main():
    load_data()

    for line in sys.stdin:
        cmd = line.strip().split(" ", 2)

        if cmd[0] == "SET":
            set_key(cmd[1], cmd[2])
            print("OK")

        elif cmd[0] == "GET":
            print(get_key(cmd[1]))

        elif cmd[0] == "EXIT":
            break

if __name__ == "__main__":
    main()
