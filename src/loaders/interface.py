from abc import ABC, abstractmethod
import pandas as pd


class LoaderInterface(ABC):
    """Base contract for file loaders."""

    @abstractmethod
    def load(self, source_path: str) -> pd.DataFrame:
        """
        Read a file from the given full path and return its data
        as a pandas DataFrame.
        """
        raise NotImplementedError