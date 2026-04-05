import pandas as pd


class DBWriter:
    """Handles inserting DataFrame rows into an existing SQLite table."""

    def __init__(self, connection):
        self.connection = connection

    def insert_dataframe(self, dataframe, table_name, column_mapping):
        """
        Insert DataFrame rows into an existing SQLite table.

        column_mapping:
            {
                "original_csv_column": "sanitized_db_column",
                ...
            }
        """
        if dataframe.empty:
            return 0

        source_columns = list(column_mapping.keys())
        target_columns = list(column_mapping.values())

        placeholders = ", ".join(["?"] * len(target_columns))
        quoted_columns = ", ".join(f'"{column}"' for column in target_columns)

        insert_sql = f'''
        INSERT INTO "{table_name}" ({quoted_columns})
        VALUES ({placeholders})
        '''

        rows = []
        for row in dataframe[source_columns].itertuples(index=False, name=None):
            cleaned_row = tuple(None if pd.isna(value) else str(value) for value in row)
            rows.append(cleaned_row)

        self.connection.executemany(insert_sql, rows)
        self.connection.commit()

        return len(rows)