import sqlite3


class DBReader:
    """Simple reader for mock verification queries."""

    def __init__(self, connection):
        self.connection = connection
    
    def execute_query(self, sql_query: str, params: tuple = ()):
        cursor = self.connection.execute(sql_query, params)
        return cursor.fetchall()

    def get_weapons_starting_with_a(self, table_name):
        query = f'''
        SELECT *
        FROM "{table_name}"
        WHERE LOWER(weapon) LIKE 'a%'
        '''

        cursor = self.connection.execute(query)
        return cursor.fetchall()