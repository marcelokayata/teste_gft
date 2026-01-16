from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Iterable, Tuple
from concurrent.futures import ThreadPoolExecutor
import re
import threading
import csv
import pandas as pd
import requests
import json
from xml.sax.saxutils import escape


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
    def fetch(self, cep8: str) -> Tuple[Optional[Dict], Optional[str]]:
        """Retorna (data, erro). Se sucesso: (data, None). Se falha: (None, 'motivo')."""
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

    def fetch(self, cep8: str) -> Tuple[Optional[Dict], Optional[str]]:
        url = f"https://viacep.com.br/ws/{cep8}/json/"
        try:
            r = self._session().get(url, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()

            # ViaCEP retorna {"erro": true} quando não encontra
            if isinstance(data, dict) and data.get("erro") is True:
                return None, "nao_encontrado"

            return data, None

        except requests.Timeout:
            return None, "timeout"
        except requests.HTTPError as e:
            return None, f"http_error:{getattr(e.response, 'status_code', 'unknown')}"
        except requests.RequestException:
            return None, "request_exception"
        except ValueError:
            return None, "json_decode_error"


# =========================
# Strategy (interface) - ESCRITA
# =========================
class Sink(ABC):
    @abstractmethod
    def write(self, data: Dict) -> None:
        pass


class FileSink(Sink):
    def __init__(self, filename: str):
        self.filename = filename

    def write(self, data: Dict) -> None:
        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(str(data) + "\n")


class ErrorCSVSink(Sink):
    """
    Escreve erros em CSV (append). Cria cabeçalho se o arquivo estiver vazio/não existir.
    """
    def __init__(self, filename: str):
        self.filename = filename
        self._fieldnames = ["cep_raw", "cep_normalizado", "url", "erro"]

        # cria arquivo com header se não existir/estiver vazio
        try:
            with open(self.filename, "r", encoding="utf-8") as _:
                pass
        except FileNotFoundError:
            with open(self.filename, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=self._fieldnames)
                w.writeheader()

    def write(self, data: Dict) -> None:
        with open(self.filename, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=self._fieldnames)
            w.writerow({k: data.get(k, "") for k in self._fieldnames})


class Dispatcher:
    def __init__(self, sinks: List[Sink]):
        self.sinks = sinks

    def dispatch(self, data: Dict) -> None:
        for sink in self.sinks:
            sink.write(data)

class JSONLinesSink(Sink):
    """
    Grava um JSON por linha (formato jsonl). É ideal para grandes volumes.
    """
    def __init__(self, filename: str):
        self.filename = filename

    def write(self, data: Dict) -> None:
        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")


class XMLSink(Sink):
    """
    Grava um XML único com vários endereços.
    - Chame begin() antes do pipeline e end() depois.
    """
    def __init__(self, filename: str, root_tag: str = "enderecos", item_tag: str = "endereco"):
        self.filename = filename
        self.root_tag = root_tag
        self.item_tag = item_tag
        self._started = False

    def begin(self) -> None:
        if self._started:
            return
        with open(self.filename, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write(f"<{self.root_tag}>\n")
        self._started = True

    def end(self) -> None:
        if not self._started:
            return
        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(f"</{self.root_tag}>\n")
        self._started = False

    def write(self, data: Dict) -> None:
        if not self._started:
            # segurança: garante root aberto
            self.begin()

        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(f"  <{self.item_tag}>\n")
            for k, v in data.items():
                # garante XML válido (escapa < > & " ')
                key = escape(str(k))
                val = escape("" if v is None else str(v))
                f.write(f"    <{key}>{val}</{key}>\n")
            f.write(f"  </{self.item_tag}>\n")



# =========================
# Util: normalização
# =========================
CEP8_RE = re.compile(r"^\d{8}$")

def normalize_to_cep8(value: str) -> Optional[str]:
    digits = re.sub(r"\D", "", value or "")
    return digits if CEP8_RE.match(digits) else None


# =========================
# Pipeline (streaming + map) + CSV de erros
# =========================
def iter_ceps(df: pd.DataFrame, col: str) -> Iterable[str]:
    for v in df[col].fillna("").astype(str).values:
        yield v


def run_pipeline(
    df: pd.DataFrame,
    *,
    cep_column: str,
    provider: CepProvider,
    success_dispatcher: Dispatcher,
    error_dispatcher: Dispatcher,
    max_workers: int = 15,
    log_every: int = 200,
) -> None:
    total = ok = bad = 0

    def task(raw_cep: str) -> Tuple[bool, Dict]:
        cep8 = normalize_to_cep8(raw_cep)
        if not cep8:
            return False, {
                "cep_raw": raw_cep,
                "cep_normalizado": "",
                "url": "",
                "erro": "cep_invalido",
            }

        data, err = provider.fetch(cep8)
        url = f"https://viacep.com.br/ws/{cep8}/json/"

        if err is not None or data is None:
            return False, {
                "cep_raw": raw_cep,
                "cep_normalizado": cep8,
                "url": url,
                "erro": err or "falha_desconhecida",
            }

        data["_cep_consultado"] = cep8
        return True, data

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for success, payload in ex.map(task, iter_ceps(df, cep_column), chunksize=50):
            total += 1

            if success:
                ok += 1
                success_dispatcher.dispatch(payload)
            else:
                bad += 1
                error_dispatcher.dispatch(payload)

            if total % log_every == 0:
                print(f"[PROGRESSO] processados={total} ok={ok} erros={bad}")

    print(f"\n[FIM] processados={total} ok={ok} erros={bad}")


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

    # Sucessos (mantém seu output atual)
    json_sink = JSONLinesSink("enderecos.json")   # (na prática é jsonl, mas nome .json ok)
    xml_sink = XMLSink("enderecos.xml")

    # abre o XML antes de começar
    xml_sink.begin()

    success_dispatcher = Dispatcher([json_sink, xml_sink])


    # Erros em CSV
    error_dispatcher = Dispatcher([ErrorCSVSink("erros_consultas.csv")])

    run_pipeline(
        df,
        cep_column="CEP Inicial",
        provider=provider,
        success_dispatcher=success_dispatcher,
        error_dispatcher=error_dispatcher,
        max_workers=15,
        log_every=200,
    )

    xml_sink.end()
