import sqlite3
import pandas as pd


class SQLQueries():
    def __init__(self) -> None:
        self.conn = sqlite3.connect("database.db")

    def drop_table(self, table_name):

        cursor = self.conn.cursor()

        sql_statement = f"DROP TABLE IF EXISTS {table_name};"

        cursor.execute(sql_statement)
        self.conn.commit()
        self.conn.close()

    def check_for_duplicates(self, table_name):
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.conn)
        duplicate_rows = df[df.duplicated(keep=False)]

        if not duplicate_rows.empty:
            print(duplicate_rows)
        else:
            print("No duplicates")

        self.conn.close()

    def drop_duplicates(self, table_name):
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.conn)
        duplicate_rows = df[df.duplicated(keep=False)]

        if not duplicate_rows.empty:
            df.drop_duplicates(inplace=True)
            df.to_sql(name=table_name, con=self.conn,
                      if_exists="replace", index=False)

        self.conn.close()

    def update_values(self, table, column, old_value, new_value):
        cursor = self.conn.cursor()
        cursor.execute(
            f"UPDATE {table} SET {column} = {new_value} WHERE {column} = {old_value}")
        self.conn.commit()
        self.conn.close()


if __name__ == '__main__':
    sql = SQLQueries()
    # sql.update_values("anacamarge_synthese", "report week",
    #                   "2024-W10", "2024-W03")
    sql.drop_table("anacamarge_synthese")
    # sql.check_for_duplicates("extraction_parametrable")
    # sql.drop_duplicates("extraction_parametrable")
