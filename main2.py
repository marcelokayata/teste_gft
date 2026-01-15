from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Iterable, Optional
import re

import pandas as pd


# =========================
# Strategy (interface)
# =========================
class Sink(ABC):
    @abstractmethod
    def write_batch(self, items: List[Dict]) -> None:
        pass


# =========================
# Concrete Strategies
# =========================
class ConsoleSink(Sink):
    def write_batch(self, items: List[Dict]) -> None:
        for data in items:
            print("[ConsoleSink]", data)


class FileSink(Sink):
    def __init__(self, filename: str):
        self.filename = filename

    def write_batch(self, items: List[Dict]) -> None:
        # Escreve em batch (mais rápido e evita abrir/fechar em loop)
        with open(self.filename, "a", encoding="utf-8") as f:
            for data in items:
                f.write(str(data) + "\n")


class MemorySink(Sink):
    def __init__(self):
        self.storage: List[Dict] = []

    def write_batch(self, items: List[Dict]) -> None:
        self.storage.extend(items)


# =========================
# Dispatcher (orquestrador)
# =========================
class Dispatcher:
    def __init__(self, sinks: List[Sink]):
        self.sinks = sinks

    def dispatch_batch(self, items: List[Dict]) -> None:
        for sink in self.sinks:
            sink.write_batch(items)


# =========================
# Transformer / "processamento"
# =========================
CEP_DIGITS_RE = re.compile(r"^\d{8}$")


def normalize_cep(raw_cep: str) -> str:
    return re.sub(r"\D", "", raw_cep or "")


def process_cep(raw_cep: str) -> Optional[Dict]:
    """
    Exemplo de processamento:
    - normaliza/valida
    - "enriquece" (aqui: mock simples)
    Retorna dict pronto para persistir, ou None se inválido.
    """
    cep_digits = normalize_cep(raw_cep)

    if not CEP_DIGITS_RE.match(cep_digits):
        return None

    # MOCK de "enriquecimento": aqui você chamaria ViaCEP/DB/etc.
    # Vou usar seu exemplo fixo apenas quando o CEP for 58348000.
    if cep_digits == "58348000":
        return {
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
            "siafi": "0506",
        }

    # Fallback mínimo
    return {"cep": f"{cep_digits[:5]}-{cep_digits[5:]}"}


# =========================
# Runner (pandas + paralelismo)
# =========================
def run_pipeline(
    csv_path: str,
    dispatcher: Dispatcher,
    *,
    cep_column: str = "cep",
    chunksize: int = 2000,
    max_workers: int = 20,
    batch_size_out: int = 500,
) -> None:
    total = 0
    ok = 0
    invalid = 0

    out_buffer: List[Dict] = []

    # Lê em chunks (streaming com pandas)
    for chunk in pd.read_csv(csv_path, dtype=str, usecols=[cep_column], chunksize=chunksize):
        ceps: List[str] = chunk[cep_column].fillna("").astype(str).tolist()
        total += len(ceps)

        # Paraleliza o processamento do chunk
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(process_cep, cep) for cep in ceps]

            for fut in as_completed(futures):
                result = fut.result()
                if result is None:
                    invalid += 1
                    continue

                ok += 1
                out_buffer.append(result)

                # Flush em batch pros sinks (escrita sequencial)
                if len(out_buffer) >= batch_size_out:
                    dispatcher.dispatch_batch(out_buffer)
                    out_buffer.clear()

    # Flush final
    if out_buffer:
        dispatcher.dispatch_batch(out_buffer)
        out_buffer.clear()

    print(f"\n[FIM] total={total} ok={ok} invalid={invalid}")


# =========================
# Exemplo de uso
# =========================
if __name__ == "__main__":
    console_sink = ConsoleSink()
    file_sink = FileSink("ceps.txt")
    memory_sink = MemorySink()

    dispatcher = Dispatcher(sinks=[console_sink, file_sink, memory_sink])

    # CSV esperado com coluna "cep"
    # cep
    # 58348-000
    # 01001-000
    # 999
    run_pipeline(
        csv_path="ceps.csv",
        dispatcher=dispatcher,
        cep_column="cep",
        chunksize=2000,
        max_workers=20,
        batch_size_out=500,
    )

    print("\n[MemorySink] Total em memória:", len(memory_sink.storage))
