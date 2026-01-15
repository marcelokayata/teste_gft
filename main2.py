from abc import ABC, abstractmethod
from typing import Dict, List
import pandas as pd



# =========================
# Strategy (interface)
# =========================
class Sink(ABC):
    @abstractmethod
    def write(self, data: Dict) -> None:
        pass


# =========================
# Concrete Strategies
# =========================
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
    
    # Read the CSV file using pandas
    # df = pd.read_csv("Lista_de_CEPs.csv", sep=";")
    df = pd.read_csv(
        "Lista_de_CEPs.csv",
        encoding="latin1",
        sep=";",
        usecols=["CEP Inicial"],
        dtype=str
    )
    print("CSV file read successfully!")
    print(f"Shape: {df.shape}")
    print(df.head())
    print(df.columns)

    cep_data = {
        "cep": "58348-000",
        "logradouro": "",
        "complemento": "",
        "unidade": "",
        "bairro": "",
        "localidade": "Riachão do Poço",
        "uf": "PB",
        "estado": "Paraíba",
        "regiao": "Nordeste",
        "ibge": "2512762",
        "gia": "",
        "ddd": "83",
        "siafi": "0506"
    }

    # console_sink = ConsoleSink()
    # file_sink = FileSink("ceps.txt")
    # memory_sink = MemorySink()

    # dispatcher = Dispatcher(
    #     sinks=[console_sink, file_sink, memory_sink]
    # )

    # dispatcher.dispatch(cep_data)

    # print("\n[MemorySink] Conteúdo em memória:")
    # print(memory_sink.storage)
