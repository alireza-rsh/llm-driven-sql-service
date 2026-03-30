from src.loaders.csv_loader import CSVLoader
import pandas as pd
import pytest

def test_load_valid_csv_with_relative_path(tmp_path, monkeypatch):
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)

    loader = CSVLoader()
    df = loader.load("./sample.csv")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["id", "name"]
    assert len(df) == 2
    assert df.iloc[0]["id"] == 1
    assert df.iloc[0]["name"] == "Alice"
    assert df.iloc[1]["id"] == 2
    assert df.iloc[1]["name"] == "Bob"


def test_load_raises_for_empty_path():
    loader = CSVLoader()

    with pytest.raises(ValueError, match="source_path must be a non-empty string"):
        loader.load("")


def test_load_raises_for_missing_file():
    loader = CSVLoader()

    with pytest.raises(FileNotFoundError, match="CSV file not found"):
        loader.load("./does_not_exist.csv")


def test_load_raises_for_non_csv_file(tmp_path):
    txt_file = tmp_path / "sample.txt"
    txt_file.write_text("hello", encoding="utf-8")

    loader = CSVLoader()

    with pytest.raises(ValueError, match="must have a .csv extension"):
        loader.load(str(txt_file))