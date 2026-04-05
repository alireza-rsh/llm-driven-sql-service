import json

from openai import OpenAI

#export OPENAI_API_KEY="""

class OpenAIAdapter:
    """Generate SQL from natural-language questions. Does not execute SQL."""

    def __init__(self, model="gpt-5.4-mini"):
        self.client = OpenAI()
        self.model = model

    def generate_sql(self, user_query, schema_context):
        if not isinstance(user_query, str) or not user_query.strip():
            raise ValueError("user_query must be a non-empty string.")

        if not isinstance(schema_context, str) or not schema_context.strip():
            raise ValueError("schema_context must be a non-empty string.")

        system_prompt = """
You are a SQL generation assistant.

Your job:
- Convert the user's request into a single SQLite-compatible SQL query.
- Use only the tables and columns provided in the schema context.
- Do not invent tables or columns.
- Prefer SELECT queries only.
- Do not execute anything.
- Return JSON with exactly these keys:
  - sql
  - explanation

Important:
- The SQL must be valid SQLite syntax.
- If the request cannot be answered from the schema, return:
  {"sql": "", "explanation": "The request cannot be answered from the available schema."}
"""

        user_prompt = f"""
Schema:
{schema_context}

User request:
{user_query}
"""

        response = self.client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            text={"format": {"type": "json_object"}},
        )

        raw_text = response.output_text
        parsed = json.loads(raw_text)

        return {
            "sql": parsed.get("sql", "").strip(),
            "explanation": parsed.get("explanation", "").strip(),
            "raw_output": raw_text,
        }