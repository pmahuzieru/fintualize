import sqlite3
from pathlib import Path
import pandas as pd
from typing import List, Tuple, Any, Optional

base_dir = Path(__file__).parent
DB_FILEPATH = base_dir / "fintualized.db"

class DatabaseManager:
    def __init__(self, db_name: str | Path = DB_FILEPATH):
        """Initializes the DatabaseManager with the given database name."""
        self.db_name = db_name
        self.connection: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        """Establishes a connection to the SQLite database."""
        try:
            self.connection = sqlite3.connect(self.db_name)
            print(f"Connected to database '{self.db_name}' successfully.")
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")

    def disconnect(self) -> None:
        """Closes the connection to the SQLite database."""
        if self.connection:
            self.connection.close()
            self.connection = None
            print("Database connection closed.")

    def execute_query(self, query: str, parameters: Optional[Tuple[Any, ...]] | Optional[List[Tuple[Any, ...]]] = (), executemany: bool = False) -> Any:
        """
        Executes a query against the SQLite database.

        Parameters:
            query (str): The SQL query to execute.
            parameters (Optional[Tuple[Any, ...]]): Parameters to use with the query.
            executemany (bool): If True, use executemany to execute the query for multiple parameter sets.

        Returns:
            Any: The results of the query (if SELECT, as a pandas DataFrame;
                for other queries, the number of rows affected or the last row ID).
        """
        if not self.connection:
            raise ConnectionError("No database connection established. Call connect() first.")

        try:
            cursor = self.connection.cursor()

            if executemany and isinstance(parameters, list):
                # Use executemany if multiple parameter sets are provided
                cursor.executemany(query, parameters)
            else:
                # Use execute for a single set of parameters
                cursor.execute(query, parameters) # type: ignore

            self.connection.commit()
            print("Query executed successfully.")

            if query.strip().lower().startswith("select"):
                # Return fetched data as a DataFrame
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                return pd.DataFrame(rows, columns=columns)
            elif query.strip().lower().startswith(("insert", "update", "delete")):
                # Return the number of rows affected
                return cursor.rowcount
            return None  # For queries like CREATE TABLE

        except sqlite3.Error as e:
            print(f"Error executing query: {e}")
            raise

    def create_table(self, table_name: str, schema: str) -> None:
        """
        Creates a table in the SQLite database.

        Parameters:
            table_name (str): The name of the table to create.
            schema (str): The schema of the table, e.g., "id INTEGER PRIMARY KEY, name TEXT".
        """
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});"
        self.execute_query(query)

    def list_tables(self):
        """List all tables in the current database."""
        if not self.connection:
            raise ConnectionError("No database connection established. Call connect() first.")
        
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("Tables in database:", [table[0] for table in tables])

if __name__ == "__main__":
    # Example usage
    db_manager = DatabaseManager()
    
    try:
        db_manager.connect()
        db_manager.create_table("users", "id INTEGER PRIMARY KEY, name TEXT, email TEXT")
        
        # Example of single insert
        db_manager.execute_query("INSERT INTO users (name, email) VALUES (?, ?)", ("Alice", "alice@example.com"))
        
        # Example of multiple inserts using executemany
        users = [
            ("Bob", "bob@example.com"),
            ("Charlie", "charlie@example.com"),
            ("David", "david@example.com")
        ]
        db_manager.execute_query("INSERT INTO users (name, email) VALUES (?, ?)", users, executemany=True)
        
        results = db_manager.execute_query("SELECT * FROM users")
        print("Users:", results)
    finally:
        db_manager.disconnect()
