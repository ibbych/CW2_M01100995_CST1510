import sqlite3
import csv
import json
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union, Generator

"""
data_layer.py

A lightweight database layer class for SQLite that:
- Connects to a database file (or in-memory)
- Executes arbitrary SQL (CRUD) passed in via the `sql` parameter
- Inserts rows from CSV files
- Inserts rows from JSON files

Usage:
    db = DatabaseLayer("my.db")
    db.connect()
    db.create("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    db.insert_from_csv("users", "users.csv")  # expects header row or provide columns
    rows = db.read("SELECT * FROM users")
    db.close()
"""



class DatabaseLayer:
    def __init__(self, db_path: str = ":memory:", timeout: float = 5.0):
        """
        Initialize the DatabaseLayer.

        :param db_path: Path to SQLite database file. Use ":memory:" for in-memory DB.
        :param timeout: SQLite connection timeout.
        """
        self.db_path = db_path
        self.timeout = timeout
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        """Open a SQLite connection and configure row factory to return dicts for selects."""
        if self.conn:
            return
        self.conn = sqlite3.connect(self.db_path, timeout=self.timeout)
        self.conn.row_factory = self._row_factory

    def close(self) -> None:
        """Close the connection if open."""
        if self.conn:
            try:
                self.conn.commit()
            finally:
                self.conn.close()
                self.conn = None

    def __enter__(self) -> "DatabaseLayer":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type:
            # On exception roll back changes
            if self.conn:
                self.conn.rollback()
        self.close()

    @staticmethod
    def _row_factory(cursor: sqlite3.Cursor, row: sqlite3.Row) -> Dict[str, Any]:
        """Transform sqlite3.Row into a dict keyed by column name."""
        return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}  # type: ignore

    def _ensure_conn(self) -> None:
        if not self.conn:
            self.connect()

    def run_sql(self, sql: str, params: Optional[Sequence[Any]] = None, fetch: bool = False) -> Union[int, List[Dict[str, Any]]]:
        """
        Execute arbitrary SQL passed in as `sql`.

        :param sql: SQL statement to execute (can be SELECT, INSERT, UPDATE, DELETE, DDL, etc.)
        :param params: Optional sequence of parameters for parameterized queries.
        :param fetch: If True and the statement is a SELECT, return fetched rows as list of dicts.
        :return: For SELECT with fetch=True -> list of dict rows. Otherwise returns cursor.rowcount.
        """
        self._ensure_conn()
        assert self.conn is not None
        cur = self.conn.cursor()
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            # Decide whether to fetch results
            is_select = sql.strip().lower().startswith("select")
            if is_select and fetch:
                rows = cur.fetchall()
                return rows  # list[dict]
            else:
                self.conn.commit()
                return cur.rowcount
        finally:
            cur.close()

    # CRUD convenience wrappers that accept `sql` parameter from outside
    def create(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        return self.run_sql(sql, params)

    def read(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        return self.run_sql(sql, params, fetch=True)  # type: ignore

    def update(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        return self.run_sql(sql, params)

    def delete(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        return self.run_sql(sql, params)

    # Helpers for bulk inserts from CSV / JSON
    def insert_from_csv(
        self,
        table: str,
        csv_path: str,
        columns: Optional[Sequence[str]] = None,
        has_header: bool = True,
        batch_size: int = 500,
    ) -> int:
        """
        Insert rows into `table` from a CSV file.

        :param table: Destination table name.
        :param csv_path: Path to CSV file.
        :param columns: Optional sequence of column names. If omitted and has_header=True, header will be used.
        :param has_header: If True, expects first CSV row to be header.
        :param batch_size: Number of rows to insert per executemany commit.
        :return: Number of rows inserted.
        """
        self._ensure_conn()
        assert self.conn is not None
        inserted = 0

        with open(csv_path, newline="", encoding="utf-8") as fh:
            if has_header and columns is None:
                reader = csv.DictReader(fh)
                columns_to_use = reader.fieldnames or []
                if not columns_to_use:
                    return 0
                row_iter = (self._row_values(row, columns_to_use) for row in reader)
            else:
                # No header: use csv.reader and require columns to be provided
                if columns is None:
                    raise ValueError("columns must be provided when CSV has no header")
                reader = csv.reader(fh)
                columns_to_use = list(columns)
                row_iter = (tuple(row) for row in reader)

            placeholders = ",".join(["?"] * len(columns_to_use))
            sql = f"INSERT INTO {table} ({', '.join(columns_to_use)}) VALUES ({placeholders})"

            cur = self.conn.cursor()
            batch: List[Tuple[Any, ...]] = []
            try:
                for vals in row_iter:
                    # ensure tuple
                    if isinstance(vals, dict):
                        vals_tuple = tuple(vals[col] for col in columns_to_use)
                    else:
                        vals_tuple = tuple(vals)
                    batch.append(vals_tuple)
                    if len(batch) >= batch_size:
                        cur.executemany(sql, batch)
                        inserted += cur.rowcount
                        batch.clear()
                if batch:
                    cur.executemany(sql, batch)
                    inserted += cur.rowcount
                self.conn.commit()
            finally:
                cur.close()
        return inserted

    def insert_from_json(
        self,
        table: str,
        json_path: str,
        columns: Optional[Sequence[str]] = None,
        batch_size: int = 500,
    ) -> int:
        """
        Insert rows into `table` from a JSON file.

        JSON should contain either:
            - a list of objects: [ {"col1": val1, ...}, ... ]
            - a single object: {"col1": val1, ...}

        :param table: Destination table name.
        :param json_path: Path to JSON file.
        :param columns: Optional sequence of column names. If omitted, will infer from first object.
        :param batch_size: Number of rows to insert per executemany commit.
        :return: Number of rows inserted.
        """
        self._ensure_conn()
        assert self.conn is not None
        inserted = 0

        with open(json_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        else:
            raise ValueError("JSON root must be an object or an array of objects")

        if not data_list:
            return 0

        # Infer columns if not provided: use keys from first object
        if columns is None:
            first = data_list[0]
            if not isinstance(first, dict):
                raise ValueError("JSON array elements must be objects when columns are not provided")
            columns_to_use = list(first.keys())
        else:
            columns_to_use = list(columns)

        placeholders = ",".join(["?"] * len(columns_to_use))
        sql = f"INSERT INTO {table} ({', '.join(columns_to_use)}) VALUES ({placeholders})"

        cur = self.conn.cursor()
        try:
            batch: List[Tuple[Any, ...]] = []
            for obj in data_list:
                if not isinstance(obj, dict):
                    raise ValueError("JSON array elements must be objects")
                vals = tuple(obj.get(col) for col in columns_to_use)
                batch.append(vals)
                if len(batch) >= batch_size:
                    cur.executemany(sql, batch)
                    inserted += cur.rowcount
                    batch.clear()
            if batch:
                cur.executemany(sql, batch)
                inserted += cur.rowcount
            self.conn.commit()
        finally:
            cur.close()

        return inserted

    @staticmethod
    def _row_values(row: Dict[str, Any], columns: Sequence[str]) -> Tuple[Any, ...]:
        """Return tuple of values for columns from a dict-like row (used by CSV dictreader)."""
        return tuple(row.get(col) for col in columns)


# Example (commented) usage:
# if __name__ == "__main__":
#     db = DatabaseLayer("example.db")
#     db.connect()
#     db.create("CREATE TABLE IF NOT EXISTS people(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
#     count = db.insert_from_csv("people", "people.csv")  # assumes header row matches columns
#     print("Inserted rows:", count)
#     rows = db.read("SELECT * FROM people")
#     print(rows)
#     db.close()