import requests
import gzip
import os
import shutil
import tarfile
import ijson
import pandas as pd
import json
import duckdb
from dotenv import load_dotenv

from ifood_processo_seletivo.funcoes_gerais import funcoesGerais

def main():
    
    # Instancia a classe de funcoes
    fg = funcoesGerais()

    # Carrega as variáveis de ambiente do arquivo .env
    load_dotenv()
    url_s3_orders = os.getenv("URL_S3_ORDERS")
    url_s3_consumers = os.getenv("URL_S3_CONSUMERS")
    url_s3_merchants = os.getenv("URL_S3_MERCHANTS")
    url_s3_ab_test = os.getenv("URL_S3_AB_TEST")

    # Cria dicionario com as URLs e tipos dos arquivos
    URLS = {
        "orders": {
            "url": url_s3_orders,
            "type": "json"
        },
        "consumers": {
            "url": url_s3_consumers,
            "type": "csv"
        },
        "restaurants": {
            "url": url_s3_merchants,
            "type": "csv"
        },
        "ab_test": {
            "url": url_s3_ab_test,
            "type": "tar"
        }
    }

    # Define variáveis dos diretórios e arquivos
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)

    

    # Baixa e processa todos os arquivos
    for name, info in URLS.items():
        url = info["url"]
        file_type = info["type"]
        if file_type == "tar":
            fg.download_and_extract_tar(url, output_dir)
        else:
            filename = os.path.basename(url)[:-3] if url.endswith('.gz') else os.path.basename(url)
            output_path = os.path.join(output_dir, filename)
            fg.download_and_decompress_gz(url, output_path)

    # Processa o arquivo JSON em streaming e converte em chunks para Parquet
    fg.split_jsonlines_to_parquet('./data/order.json', './data/orders', target_size_mb=250)

    # Concatena os arquivos Parquet em um único arquivo parquet para facilitar a leitura
    fg.concat_chunks_into_single_file('./data/order.json', './data/orders', target_size_mb=250)

    # Deleta os arquivos chunks para reduzir espaço em memória ocupado - opcional
    fg.delete_chunks('./data/order.json', './data/orders', target_size_mb=250)

    # Gera os dataframes a partir dos arquivos para realizar as análises
    con = duckdb.connect()

    alias_json = './data/orders/orders_final.parquet'
    alias_csv1 = './data/restaurant.csv'
    alias_csv2 = './data/consumer.csv'
    alias_tar = './data/ab_test_ref.csv'

    df_json = con.execute(f"SELECT * FROM '{alias_json}' ").df()
    df_csv1 = con.execute(f"""SELECT * FROM read_csv_auto('{alias_csv1}', delim=',', quote='"', escape='\\') """).df()
    df_csv2 = con.execute(f"""SELECT * FROM read_csv_auto('{alias_csv2}', delim=',', quote='"', escape='\\') """).df()
    df_tar = con.execute(f"SELECT * FROM '{alias_tar}' ").df()

if __name__ == "__main__":
    print("Iniciando execução do processo...")
    main()
    print("Processamento concluído com sucesso!")