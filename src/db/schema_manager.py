import re
import os
import sqlite3

import pandas as pd


class SchemaManager:
    """Handles schema discovery, comparison, and table creation."""

    TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(self, connection):
        self.connection = connection
    
    def sanitize_identifier(self, name):
        """Sanitize a table name or column name for SQLite usage."""
        sanitized = str(name).strip().lower()
        sanitized = re.sub(r"\s+", "_", sanitized)
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", sanitized)
        sanitized = re.sub(r"_+", "_", sanitized).strip("_")

        if not sanitized:
            sanitized = "field"


        return sanitized
    
    def build_column_mapping(self, dataframe):
        """
        Return a mapping from original DataFrame column names
        to sanitized database column names.
        """
        mapping = {}

        for column_name in dataframe.columns:
            mapping[str(column_name)] = self.sanitize_identifier(column_name)

        return mapping

    def make_table_name_from_csv_path(self, source_path):

        file_name = os.path.basename(source_path)
        base_name = os.path.splitext(file_name)[0]
        return self.sanitize_identifier(base_name)

    def infer_dataframe_schema(self, dataframe) -> dict:
        """Infer schema from CSV headers only."""
        mapping = self.build_column_mapping(dataframe)
        columns = []

        for sanitized_name in mapping.values():
            columns.append({"name": sanitized_name})

        return {"columns": columns}

    def list_tables(self):
        """Return all non-internal SQLite tables."""
        cursor = self.connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def get_table_schema(self, table_name):
        """Return schema information for one table."""

        cursor = self.connection.execute(f'PRAGMA table_info("{table_name}")')
        rows = cursor.fetchall()

        if not rows:
            raise ValueError(f"Table '{table_name}' does not exist.")

        columns = []
        for row in rows:
            columns.append(
                {
                    "name": row[1],
                    "type": row[2] if row[2] else "TEXT",
                    "nullable": not bool(row[3]),
                }
            )

        return {
            "table_name": table_name,
            "columns": columns,
        }

    def get_all_table_schemas(self):
        """Return schemas for all tables in the database."""
        schemas = []
        for table_name in self.list_tables():
            schemas.append(self.get_table_schema(table_name))
        return schemas

    def schemas_match(self, left_schema, right_schema):
        """Check whether two schemas match based only on column names."""
        left_columns = [column["name"] for column in left_schema["columns"]]
        right_columns = [column["name"] for column in right_schema["columns"]]

        return left_columns == right_columns

    def find_matching_table(self, dataframe):
        """Return the name of a table whose schema matches the DataFrame."""
        incoming_schema = self.infer_dataframe_schema(dataframe)

        for table_name in self.list_tables():
            table_schema = self.get_table_schema(table_name)
            if self.schemas_match(table_schema, incoming_schema):
                return table_name

        return None

    def resolve_table(self, dataframe, new_table_name):
        """
        Find a matching table for the DataFrame.
        If none exists, create a new table and return its name.
        """
        matching_table = self.find_matching_table(dataframe)
        if matching_table is not None:
            print(f"Table '{matching_table}' already exists. Data will be inserted into this table.")
            return matching_table

        self.create_table_from_dataframe(dataframe, new_table_name)
        return new_table_name

    def create_table_from_dataframe(self, dataframe, table_name):
        """Create a new SQLite table using only CSV headers, with all columns as TEXT."""
        schema = self.infer_dataframe_schema(dataframe)

        column_definitions = []
        for column in schema["columns"]:
            column_definitions.append(f'"{column["name"]}" TEXT')

        create_sql = f'''
        CREATE TABLE "{table_name}" (
            {", ".join(column_definitions)}
        )
        '''

        self.connection.execute(create_sql)
        self.connection.commit()

    def _infer_sqlite_type(self, series):
        if pd.api.types.is_integer_dtype(series):
            return "INTEGER"
        if pd.api.types.is_float_dtype(series):
            return "REAL"
        if pd.api.types.is_bool_dtype(series):
            return "INTEGER"
        return "TEXT"
    
    def get_table_columns(self, table_name):
        """Return the already-sanitized column names of a table."""
        cursor = self.connection.execute(f'PRAGMA table_info("{table_name}")')
        rows = cursor.fetchall()

        if not rows:
            raise ValueError(f"Table '{table_name}' does not exist.")

        return [row[1] for row in rows]