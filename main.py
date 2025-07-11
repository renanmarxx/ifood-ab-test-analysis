import requests
import gzip
import os
import shutil
import tarfile
import ijson
import pandas as pd
import json
import duckdb

from ifood_processo_seletivo.funcoes_gerais import funcoesGerais

fg = funcoesGerais()

def main():
    # Cria o diretório de saída, se não existir
    fg._create_new_directory()

    # Exemplo de download e descompressão de um arquivo gz
    url = "https://example.com/path/to/file.gz"
    output_path = os.path.join(fg.output_dir, "file_decompressed.txt")
    fg.download_and_decompress_gz(url, output_path)

    # Exemplo de download e extração de um arquivo tar
    tar_url = "https://example.com/path/to/file.tar.gz"
    fg.download_and_extract_tar(tar_url, fg.output_dir)

    # Processamento de um arquivo JSON em streaming
    json_path = os.path.join(fg.output_dir, "file.json")
    fg.process_json_streaming(json_path, limit=10)

    # Exemplo de leitura de um arquivo CSV com pandas
    csv_path = os.path.join(fg.output_dir, "file.csv")
    df = pd.read_csv(csv_path)
    print(df.head())

    # Exemplo de consulta SQL com DuckDB
    con = duckdb.connect(database=':memory:')
    con.execute("CREATE TABLE my_table AS SELECT * FROM df")
    result = con.execute("SELECT * FROM my_table LIMIT 5").fetchdf()
    print(result)   

if __name__ == "__main__":
    main()
    print("Processamento concluído com sucesso!")




