from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Iterable
from concurrent.futures import ThreadPoolExecutor
import re
import threading
import pandas as pd
import requests


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
        usecols: Optional[List[str]] = None,
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
# Strategy (interface) - CONSULTA
# =========================
class CepProvider(ABC):
    @abstractmethod
    def fetch(self, cep8: str) -> Optional[Dict]:
        pass


class ViaCepProvider(CepProvider):
    """
    Session por thread (thread-safe) + timeout (connect, read).
    """
    def __init__(self, timeout_connect: float = 3.0, timeout_read: float = 7.0):
        self.timeout = (timeout_connect, timeout_read)
        self._local = threading.local()

    def _session(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    def fetch(self, cep8: str) -> Optional[Dict]:
        url = f"https://viacep.com.br/ws/{cep8}/json/"
        print(f"[ViaCepProvider] Consultando {url}")
        try:
            r = self._session().get(url, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("erro") is True:
                return None
            return data
        except (requests.RequestException, ValueError):
            return None


# =========================
# Strategy (interface) - ESCRITA
# =========================
class Sink(ABC):
    @abstractmethod
    def write(self, data: Dict) -> None:
        pass


class ConsoleSink(Sink):
    def write(self, data: Dict) -> None:
        print("[ConsoleSink]", data)


class FileSink(Sink):
    def __init__(self, filename: str):
        self.filename = filename

    def write(self, data: Dict) -> None:
        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(str(data) + "\n")


class Dispatcher:
    def __init__(self, sinks: List[Sink]):
        self.sinks = sinks

    def dispatch(self, data: Dict) -> None:
        for sink in self.sinks:
            sink.write(data)


# =========================
# Util: normalização
# =========================
CEP8_RE = re.compile(r"^\d{8}$")

def normalize_to_cep8(value: str) -> Optional[str]:
    digits = re.sub(r"\D", "", value or "")
    return digits if CEP8_RE.match(digits) else None


# =========================
# Pipeline (streaming + map)
# =========================
def iter_ceps(df: pd.DataFrame, col: str) -> Iterable[str]:
    # gera strings, sem criar lista gigante
    for v in df[col].fillna("").astype(str).values:
        yield v

def run_pipeline(
    df: pd.DataFrame,
    *,
    cep_column: str,
    provider: CepProvider,
    dispatcher: Dispatcher,
    max_workers: int = 15,
    log_every: int = 200,
) -> None:
    total = ok = bad = 0

    def task(raw_cep: str) -> Optional[Dict]:
        cep8 = normalize_to_cep8(raw_cep)
        if not cep8:
            return None
        data = provider.fetch(cep8)
        if not data:
            return None
        data["_cep_consultado"] = cep8
        return data

    # executor.map não cria 10k futures em memória; ele “vai puxando” aos poucos
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for result in ex.map(task, iter_ceps(df, cep_column), chunksize=50):
            total += 1

            if result is None:
                bad += 1
            else:
                ok += 1
                dispatcher.dispatch(result)

            if total % log_every == 0:
                print(f"[PROGRESSO] processados={total} ok={ok} invalidos/nao_encontrados={bad}")

    print(f"\n[FIM] processados={total} ok={ok} invalidos/nao_encontrados={bad}")


if __name__ == "__main__":
    source = PandasCSVSource(
        filepath="Lista_de_CEPs.csv",
        encoding="latin1",
        sep=";",
        usecols=["CEP Inicial"],
        dtype=str,
    )

    df = source.read()

    provider = ViaCepProvider(timeout_connect=3.0, timeout_read=7.0)
    dispatcher = Dispatcher([FileSink("viacep_resultados.txt")])

    # Comece conservador:
    run_pipeline(
        df,
        cep_column="CEP Inicial",
        provider=provider,
        dispatcher=dispatcher,
        max_workers=15,   # ajuste aqui
        log_every=200,    # mostra progresso
    )
