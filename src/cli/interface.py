from src.loaders.csv_loader import CSVLoader


class CLIInterface:

    def __init__(self):
        self.loader = CSVLoader()

    def show_banner(self) -> None:
        print("=" * 60)
        print("         LLM-Driven SQL Service")
        print("              EC530 Course")
        print("=" * 60)
        print()

    def show_menu(self) -> None:
        print("Choose an option:")
        print("1. Load a CSV file")
        print("2. Ask a query")
        print("3. Exit")
        print()

    def handle_load_csv(self) -> None:
        source_path = input("Enter CSV path: ").strip()

        try:
            dataframe = self.loader.load(source_path)
            print("\nCSV loaded successfully.")
            print(f"Rows: {len(dataframe)}")
            print(f"Columns: {list(dataframe.columns)}")
            print()
        except Exception as exc:
            print(f"\nError: {exc}\n")

    def handle_query(self) -> None:
        query = input("Enter your query: ").strip()

        if not query:
            print("\nError: query cannot be empty.\n")
            return

        print("\nReceived query:")
        print(query)
        print()

    def run(self) -> None:
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
                break
            else:
                print("\nInvalid choice. Please enter 1, 2, or 3.\n")