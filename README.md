# LLM-Driven SQL Service

A modular Python project for loading CSV files into SQLite and querying the database using natural language through an LLM.

This project is designed for **EC530 Course** and focuses on **separation of concerns** across loading, schema management, query generation, and query execution.

---

## Project Objective

The goal of this project is to build a simple but modular system that:

- loads CSV files into a SQLite database
- automatically detects whether a matching table already exists
- creates a new table when needed
- translates user natural-language queries into SQL using an LLM
- executes the generated SQL through a query service layer

---

## Main Design Idea

This project follows a modular architecture.

Each component has one clear responsibility:

- **CSVLoader**: reads CSV files into memory as a pandas DataFrame
- **SchemaManager**: understands the schema of the loaded data and the database
- **DBWriter**: inserts loaded data into the database
- **DBReader**: executes read queries against the database
- **LLM Adapter**: converts user requests into SQL
- **QueryService**: coordinates schema retrieval, LLM invocation, and query execution
- **CLI Interface**: acts as the controller for the user

---

## Project Structure

```text
llm-driven-sql-service/
├── main.py
├── README.md
├── requirements.txt
├── src/
│   ├── cli/
│   │   └── interface.py
│   ├── db/
│   │   ├── schema_manager.py
│   │   ├── db_writer.py
│   │   └── db_reader.py
│   ├── llm/
│   │   └── openai_adapter.py
│   ├── loaders/
│   │   ├── interface.py
│   │   └── csv_loader.py
│   └── services/
│       └── query_service.py
└── tests/
    ├── test_csv_loader.py
    └── ...
