# iFood Processo Seletivo

Este projeto realiza o download, processamento e análise de dados do iFood a partir de arquivos públicos.

## Como rodar o projeto

### 1. Clone o repositório

```sh
git clone https://github.com/seu-usuario/seu-repo.git
cd seu-repo
```

### 2. Crie e ative um ambiente virtual

```sh
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Instale as dependências

```sh
pip install -r requirements.txt
```

### 4. Configure as variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto com o seguinte conteúdo (ou copie de `.env.example`):

```
URL_S3_ORDERS="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz"
URL_S3_CONSUMERS="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz"
URL_S3_MERCHANTS="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz"
URL_S3_AB_TEST="https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"
```

### 5. Execute o pipeline

```sh
python main.py
```

## Estrutura dos arquivos

- `main.py`: Script principal de execução.
- `funcoes_gerais.py`: Funções utilitárias para download, processamento e análise.
- `.env.example`: Exemplo de variáveis de ambiente.
- `requirements.txt`: Dependências do projeto.

## Observações

- O arquivo `.env` **não é versionado** por segurança.
- Os arquivos de dados baixados e processados são salvos na pasta `data/`.

---