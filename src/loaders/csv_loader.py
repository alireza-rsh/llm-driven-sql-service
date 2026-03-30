import os
import pandas as pd

from src.loaders.interface import LoaderInterface


class CSVLoader(LoaderInterface):
    """Loader for CSV files."""

    def load(self, source_path: str) -> pd.DataFrame:
        """Read a CSV file and return its content as a DataFrame."""
        if not isinstance(source_path, str) or not source_path.strip():
            raise ValueError("source_path must be a non-empty string.")

        if not os.path.exists(source_path):
            raise FileNotFoundError(f"CSV file not found: {source_path}")

        if not os.path.isfile(source_path):
            raise ValueError(f"source_path is not a file: {source_path}")

        if not source_path.lower().endswith(".csv"):
            raise ValueError("The provided file must have a .csv extension.")

        try:
            return pd.read_csv(source_path)
        except Exception as exc:
            raise ValueError(f"Failed to read CSV file: {exc}") from exc