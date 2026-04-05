import sqlite3

from src.loaders.csv_loader import CSVLoader
from src.db.schema_manager import SchemaManager
from src.db.db_writer import DBWriter
from src.db.db_reader import DBReader
from src.services.query_service import QueryService
from src.llm.openai_adapter import OpenAIAdapter


class CLIInterface:
    """Simple CLI controller for the project."""

    def __init__(self, db_path="app.db"):
        self.loader = CSVLoader()
        self.connection = sqlite3.connect(db_path)
        self.schema_manager = SchemaManager(self.connection)
        self.db_writer = DBWriter(self.connection)
        self.db_reader = DBReader(self.connection)
        self.openai_adapter = OpenAIAdapter()
        self.query_service = QueryService(self.schema_manager, self.openai_adapter, self.db_reader)

    def show_banner(self):
        print("=" * 60)
        print("         LLM-Driven SQL Service")
        print("              EC530 Course")
        print("=" * 60)
        print()

    def show_menu(self):
        print("Choose an option:")
        print("1. Load a CSV file")
        print("2. Ask a query")
        print("3. Exit")
        print()

    def handle_load_csv(self):
        source_path = input("Enter CSV path: ").strip()

        try:
            dataframe = self.loader.load(source_path)

            new_table_name = self.schema_manager.make_table_name_from_csv_path(source_path)
            table_name = self.schema_manager.resolve_table(dataframe, new_table_name)
            column_mapping = self.schema_manager.build_column_mapping(dataframe)
            rows = self.db_writer.insert_dataframe(dataframe, table_name, column_mapping)
            

            print("\nCSV loaded into database successfully.")
            print(f"Target table: {table_name}")
            print(f"Rows inserted: {len(dataframe)}")
            print(f"Columns: {list(dataframe.columns)}")
            print()

        except Exception as exc:
            print(f"\nError: {exc}\n")

    def handle_query(self):
        query = input("Enter your query: ").strip()

        if not query:
            print("\nError: query cannot be empty.\n")
            return

        result = self.query_service.run_query(query)
        print(result)
        print()

    def run(self):
        self.show_banner()

        while True:
            self.show_menu()
            choice = input("Enter your choice: ").strip()

            if choice == "1":
                self.handle_load_csv()
            elif choice == "2":
                self.handle_query()
            elif choice == "3":
                print("\nGoodbye.")
                self.connection.close()
                break
            else:
                print("\nInvalid choice. Please enter 1, 2, or 3.\n")