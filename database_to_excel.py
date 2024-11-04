import sqlite3
import pandas as pd


class SQLiteToFile():
    def __init__(self) -> None:
        self.sqlite_db_path = "database.db"

    def get_table_names(self):
        connection = sqlite3.connect(self.sqlite_db_path)
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        connection.close()

        return [table[0] for table in tables]

    def get_table_data(self, table_name):
        connection = sqlite3.connect(self.sqlite_db_path)
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, connection)
        connection.close()
        return df
