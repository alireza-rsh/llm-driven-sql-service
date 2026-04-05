
class QueryService:
    """Coordinates schema retrieval, prompt building, SQL generation, execution, and response formatting."""

    def __init__(self, schema_manager, llm_adapter, db_reader):
        self.schema_manager = schema_manager
        self.llm_adapter = llm_adapter
        self.db_reader = db_reader

    def build_schema_context(self):
        """Build a text description of the current database schema for the LLM."""
        table_schemas = self.schema_manager.get_all_table_schemas()

        if not table_schemas:
            return "The database currently has no tables."

        lines = []
        for table_schema in table_schemas:
            lines.append(f"Table: {table_schema['table_name']}")
            lines.append("Columns:")
            for column in table_schema["columns"]:
                lines.append(f"- {column['name']}")
            lines.append("")

        return "\n".join(lines).strip()

    def run_query(self, user_query):
        """
        Flow:
        1. Get DB schema
        2. Build schema context
        3. Send user query + schema to LLM
        4. Get SQL back
        5. Execute SQL using DBReader
        6. Format the response
        """
        if not isinstance(user_query, str) or not user_query.strip():
            raise ValueError("user_query must be a non-empty string.")

        cleaned_user_query = user_query.strip()
        schema_context = self.build_schema_context()

        llm_result = self.llm_adapter.generate_sql(
            user_query=cleaned_user_query,
            schema_context=schema_context,
        )

        sql_query = llm_result["sql"]
        explanation = llm_result["explanation"]
        rows = self.db_reader.execute_query(sql_query)

        return {
            "user_query": cleaned_user_query,
            "schema_context": schema_context,
            "sql_query": sql_query,
            "explanation": explanation,
            "rows": rows,
            "row_count": len(rows),
        }