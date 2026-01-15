from abc import ABC, abstractmethod
from typing import Dict, List
import pandas as pd


# =========================
# Strategy (interface) - LEITURA
# =========================
class Source(ABC):
    @abstractmethod
    def read(self) -> pd.DataFrame:
        pass


class PandasCSVSource(Source):
    def __init__(
        self,
        filepath: str,
        *,
        encoding: str = "latin1",
        sep: str = ";",
        usecols: List[str] | None = None,
        dtype=str,
    ):
        self.filepath = filepath
        self.encoding = encoding
        self.sep = sep
        self.usecols = usecols
        self.dtype = dtype

    def read(self) -> pd.DataFrame:
        return pd.read_csv(
            self.filepath,
            encoding=self.encoding,
            sep=self.sep,
            usecols=self.usecols,
            dtype=self.dtype,
        )


# =========================
# Strategy (interface) - ESCRITA
# =========================
class Sink(ABC):
    @abstractmethod
    def write(self, data: Dict) -> None:
        pass


class ConsoleSink(Sink):
    def write(self, data: Dict) -> None:
        print("[ConsoleSink] Gravando dados:")
        print(data)


class FileSink(Sink):
    def __init__(self, filename: str):
        self.filename = filename

    def write(self, data: Dict) -> None:
        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(str(data) + "\n")


class MemorySink(Sink):
    def __init__(self):
        self.storage: List[Dict] = []

    def write(self, data: Dict) -> None:
        self.storage.append(data)


# =========================
# Dispatcher (orquestrador)
# =========================
class Dispatcher:
    def __init__(self, sinks: List[Sink]):
        self.sinks = sinks

    def dispatch(self, data: Dict) -> None:
        for sink in self.sinks:
            sink.write(data)


# =========================
# Exemplo de uso
# =========================
if __name__ == "__main__":
    source = PandasCSVSource(
        filepath="Lista_de_CEPs.csv",
        encoding="latin1",
        sep=";",
        usecols=["CEP Inicial"],
        dtype=str,
    )

    df = source.read()

    print("CSV file read successfully!")
    print(f"Shape: {df.shape}")
    print(df.head())
    print(df.columns)
