# GFT Teste - CEP Data Pipeline

Projeto Python que implementa um pipeline de processamento de dados de CEP (Código de Endereçamento Postal) brasileiro usando padrões de design como Strategy e Observer.

## Estrutura do Projeto

- **main.py**: Implementação do padrão Strategy com diferentes "sinks" (destinos) para armazenar dados de CEP
- **main2.py**: Pipeline completo de processamento de CEP incluindo leitura de CSV, validação, transformação e armazenamento

## Componentes

### main.py - Strategy Pattern

Implementa diferentes estratégias de gravação de dados:

- **ConsoleSink**: Exibe dados no console
- **FileSink**: Grava dados em arquivo de texto
- **MemorySink**: Armazena dados em memória
- **Dispatcher**: Orquestra o envio de dados para múltiplos sinks simultaneamente

### main2.py - CEP Data Pipeline

Implementa um pipeline completo com os seguintes componentes:

- **CSVCEPReader**: Lê CEPs de um arquivo CSV em streaming
- **CEPProvider**: Interface abstrata para buscar dados de CEP
- **MockCEPProvider**: Implementação mock para testes
- **CEPTransformer**: Normaliza, valida e transforma dados de CEP
- Suporte a banco de dados SQLite para armazenamento

## Como Usar

### Exemplo com main.py

```bash
python main.py
```

Exemplo de saída com dados de CEP do Riachão do Poço - PB.

### Exemplo com main2.py

O pipeline completo permite:
- Ler múltiplos CEPs de um arquivo CSV
- Validar e normalizar os dados
- Buscar informações complementares
- Armazenar em banco de dados SQLite

## Requisitos

- Python 3.7+

## Estrutura de Dados

Exemplo de registro de CEP:

```json
{
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
```

## Padrões de Design

- **Strategy Pattern**: Implementado em `main.py` para múltiplas estratégias de armazenamento
- **Reader Pattern**: Leitura eficiente de CSV em streaming
- **Provider Pattern**: Abstração para diferentes fontes de dados de CEP
- **Transformer Pattern**: Validação e transformação de dados

para criar a pasta venv: python -m venv ./venv
para rodar local: source venv/bin/activate
Para criar o requirements.txt: pip freeze > requirements.txt
Utilizar a versão correta: pyenv local 3.8.10