import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect("intelligence_platform.db")
cursor = conn.cursor()

# --- Create Tables ---
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS cyber_incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_type TEXT,
    severity TEXT,
    date TEXT,
    description TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS datasets_metadata (
    dataset_id TEXT PRIMARY KEY,
    name TEXT,
    source TEXT,
    last_updated TEXT,
    description TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS it_tickets (
    ticket_id INTEGER PRIMARY KEY,
    user TEXT,
    issue TEXT,
    status TEXT,
    opened_date TEXT,
    resolved_date TEXT
);
""")

# --- Load CSV Data ---
def load_csv(table_name, file_path):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Loaded {file_path} into {table_name}")

load_csv("cyber_incidents", "DATA/cyber_incidents.csv")
load_csv("datasets_metadata", "DATA/datasets_metadata.csv")
load_csv("it_tickets", "DATA/it_tickets.csv")

# --- Migrate users.txt ---
def migrate_users(file_path):
    with open(file_path, "r") as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) == 3:
                username, password_hash, role = parts
                cursor.execute("INSERT OR IGNORE INTO users VALUES (?, ?, ?)", (username, password_hash, role))
    print("Migrated users.txt into users table")

migrate_users("user.txt")

