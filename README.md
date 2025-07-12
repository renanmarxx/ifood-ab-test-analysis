## Como rodar o projeto

### 1. Clone o repositório

```sh
git clone https://github.com/seu-usuario/seu-repo.git
cd seu-repo
```

### 2. Instale o Poetry (caso não tenha)

```sh
pip install poetry
```

### 3. Instale as dependências e crie o ambiente virtual

```sh
poetry install
```

### 4. Ative o ambiente virtual do Poetry

```sh
poetry shell
```

### 5. Configure as variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto com o seguinte conteúdo:

```
URL_S3_ORDERS="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz"
URL_S3_CONSUMERS="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz"
URL_S3_MERCHANTS="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz"
URL_S3_AB_TEST="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"
```

### 6. Execute o pipeline

```sh
python main.py
```

## Estrutura dos arquivos

- `main.py`: Script principal de execução.
- `funcoes_gerais.py`: Funções utilitárias para download, processamento e análise.
- `pyproject.toml`: Dependências do projeto.
- `poetry.lock`: Configurações e demais dependências do projeto.

## Observações

- O arquivo `.env` **não é versionado** por segurança.
- Os arquivos de dados baixados e processados são salvos na pasta `data/`.

---