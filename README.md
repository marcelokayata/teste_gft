# Consulta de CEPs (ViaCEP) com Pipeline + Strategy + Persistência (JSON, XML, MongoDB)

Este projeto lê uma lista de CEPs a partir de um CSV, consulta o endpoint do **ViaCEP** em paralelo e persiste os resultados:
- **Sucesso**: `enderecos.json` (JSON Lines), `enderecos.xml` (XML) e **MongoDB** (upsert por CEP)
- **Erro**: `erros_consultas.csv` com motivo e URL consultada

---

## Requisitos

- Python **3.8.10** (recomendado via `pyenv`)
- Docker + Docker Compose (para rodar com MongoDB local via compose)
- Dependências Python listadas em `requirements.txt`

---

## Setup local (venv + pyenv)

### Selecionar a versão correta do Python
```bash
pyenv local 3.8.10
```

### Criar e ativar virtualenv
```bash
python -m venv ./venv
source venv/bin/activate
```

### Instalar dependências
```bash
pip install -r requirements.txt
```

### Gerar/atualizar o requirements.txt
```bash
pip freeze > requirements.txt
```

---

## Como rodar com Docker Compose

1. Garanta que `Lista_de_CEPs.csv` está no **mesmo diretório** do `docker-compose.yml`.
2. Suba os serviços:
```bash
docker compose up --build
```

---

## Verificando os dados no MongoDB (opcional)

Em outro terminal:
```bash
docker exec -it mongo_local mongosh
```

Depois:
```javascript
use ceps
db.enderecos.countDocuments()
db.enderecos.findOne()
```

---

## Estrutura esperada do CSV

O código lê o arquivo `Lista_de_CEPs.csv` com:
- `encoding="latin1"`
- `sep=";"`
- coluna usada: **`CEP Inicial`**

Exemplo de cabeçalho:
```csv
CEP Inicial;...
01001000;...
```

Se a coluna tiver outro nome, ajuste o `usecols` e o parâmetro `cep_column` no `main`.

---

## Variáveis de ambiente (configuração)

O script usa variáveis de ambiente com valores padrão:

- `CSV_PATH` (default: `Lista_de_CEPs.csv`)
- `MONGO_URI` (default: `mongodb://localhost:27017`)
- `MONGO_DB` (default: `ceps`)
- `MONGO_COLLECTION` (default: `enderecos`)

Exemplo:
```bash
export CSV_PATH="Lista_de_CEPs.csv"
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="ceps"
export MONGO_COLLECTION="enderecos"
```

---

## O que o código faz (visão geral)

O projeto foi organizado com uma abordagem inspirada no padrão **Strategy**, separando responsabilidades:

1. **Leitura (Source)**: lê dados do CSV e devolve um `DataFrame`
2. **Consulta (CepProvider)**: consulta um serviço externo (ViaCEP)
3. **Escrita (Sink)**: persiste resultados em diferentes destinos (JSONL, XML, CSV de erros, MongoDB)
4. **Pipeline**: orquestra a execução em paralelo e roteia resultados para sucesso/erro

---

## Componentes principais

### 1) Leitura: `Source` e `PandasCSVSource`
- `Source` define a interface (`read() -> pd.DataFrame`)
- `PandasCSVSource` implementa a leitura via `pandas.read_csv(...)`

Responsabilidade: **ler o CSV** e devolver um `DataFrame` para o pipeline.

---

### 2) Consulta: `CepProvider` e `ViaCepProvider`
- `CepProvider` define a interface: `fetch(cep8) -> (data | None, erro | None)`
- `ViaCepProvider` chama `https://viacep.com.br/ws/{cep}/json/`

Detalhes importantes:
- Usa `requests.Session()` com **thread-local** (`threading.local()`), evitando recriar sessão a cada request
- Trata erros comuns:
  - `timeout`
  - `http_error:<status_code>`
  - `request_exception`
  - `json_decode_error`
  - `nao_encontrado` (quando o ViaCEP retorna `{"erro": true}`)

---

### 3) Escrita: `Sink` e implementações

#### `ErrorCSVSink`
Cria (se não existir) e escreve `erros_consultas.csv` com colunas:
- `cep_raw`
- `cep_normalizado`
- `url`
- `erro`

#### `JSONLinesSink`
Escreve um JSON por linha no arquivo `enderecos.json` (formato JSONL/NDJSON).
- Facilita processamento incremental (streaming).

#### `XMLSink`
Gera `enderecos.xml` como uma lista de elementos:
- Root: `<enderecos>`
- Item: `<endereco>...</endereco>`
- Faz `escape(...)` para evitar XML inválido.

Uso:
- `begin()` escreve cabeçalho/root de abertura
- `write(...)` adiciona itens
- `end()` fecha a root

#### `MongoSink`
Grava no MongoDB com **upsert** (atualiza se existir; insere se não existir).

Chave de deduplicação:
- tenta usar `data["cep"]`, se existir
- senão, usa `data["_cep_consultado"]` (CEP normalizado consultado)

Implementação:
- cria índices em `cep` e `_cep_consultado` (não necessariamente únicos)
- faz upsert com filtro `{ "_key": <cep> }`

Resultado: cada CEP tende a virar **um documento**, atualizado em execuções subsequentes.

---

### 4) Dispatcher
`Dispatcher` recebe uma lista de `Sink`s e envia (`dispatch`) o mesmo payload para todos.
- Ex.: no sucesso, o mesmo endereço vai para JSON, XML e Mongo ao mesmo tempo.

---

## Normalização de CEP

Função:
```python
normalize_to_cep8(value: str) -> Optional[str]
```

- Remove tudo que não é dígito
- Valida o formato final com regex `^\d{8}$`
- Retorna o CEP normalizado (8 dígitos) ou `None` (inválido)

Exemplos:
- `"01001-000"` → `"01001000"`
- `"ABC"` → inválido (`None`)

---

## Pipeline de execução (paralelo)

Função principal:
```python
run_pipeline(df, cep_column, provider, success_dispatcher, error_dispatcher, max_workers=15, log_every=200)
```

Como funciona:
1. Itera a coluna de CEPs do DataFrame (`iter_ceps`)
2. Para cada CEP cru (`raw_cep`), cria uma tarefa:
   - normaliza o CEP
   - consulta no provider (ViaCEP)
   - gera payload de sucesso ou payload de erro
3. Executa em paralelo com `ThreadPoolExecutor`
4. Despacha:
   - sucesso → `success_dispatcher`
   - erro → `error_dispatcher`
5. A cada `log_every` itens, imprime progresso

Parâmetros relevantes:
- `max_workers`: controla o paralelismo (padrão: 15)
- `chunksize=50`: reduz overhead no `executor.map`

---

## Arquivos gerados

- `enderecos.json` (JSON Lines) — 1 registro por linha
- `enderecos.xml` — lista XML de endereços
- `erros_consultas.csv` — erros e motivo
- MongoDB: banco `ceps`, coleção `enderecos` (padrões), com upsert por CEP

---

## Ponto de entrada (main)

No bloco:
```python
if __name__ == "__main__":
```

O script:
1. Lê variáveis de ambiente
2. Lê CSV com `PandasCSVSource(... usecols=["CEP Inicial"])`
3. Inicializa:
   - `ViaCepProvider`
   - `JSONLinesSink`, `XMLSink`, `MongoSink`
4. Inicia XML (`xml_sink.begin()`)
5. Executa `run_pipeline(...)`
6. Finaliza XML (`xml_sink.end()`)

---

## Observações e boas práticas

- **Rate limiting / estabilidade do ViaCEP**: paralelismo alto pode aumentar timeouts. Se ocorrerem muitos erros `timeout`, reduza `max_workers`.
- **Reexecução**: como há upsert no Mongo, rodar novamente tende a atualizar documentos existentes pelo mesmo CEP.
- **Consistência de dados**: o ViaCEP pode retornar campos diferentes dependendo do CEP; o `XMLSink` escreve dinamicamente as chaves existentes no payload.

---

## Troubleshooting rápido

- **Erro de encoding/coluna no CSV**: confirme separador `;`, encoding `latin1` e existência da coluna `CEP Inicial`.
- **Falha ao conectar no Mongo**: confirme `MONGO_URI` e se o container `mongo_local` está ativo (`docker ps`).
- **Muitos timeouts**: reduza `max_workers` e/ou aumente `timeout_read`.
