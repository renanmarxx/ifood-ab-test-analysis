import os
from dotenv import load_dotenv
from funcoes_gerais import FuncoesGerais
import duckdb

def main():
    
    print("1. Carrega variáveis de ambiente")
    load_dotenv()
    output_dir = "../data"
    url_s3_orders = os.getenv("URL_S3_ORDERS")
    url_s3_consumers = os.getenv("URL_S3_CONSUMERS")
    url_s3_merchants = os.getenv("URL_S3_MERCHANTS")
    url_s3_ab_test = os.getenv("URL_S3_AB_TEST")

    print("2. Faz a instancia de classe e variaveis iniciais")
    fg = FuncoesGerais(
        output_dir = output_dir,
        url_s3_orders = url_s3_orders,
        url_s3_consumers = url_s3_consumers,
        url_s3_merchants = url_s3_merchants,
        url_s3_ab_test = url_s3_ab_test,
        final_file_name = "orders_final.parquet"
    )

    print("3. Cria dicionário com as URLs e tipos dos arquivos")
    URLS = {
        "orders": {"url": fg.url_s3_orders, "type": "json"},
        "consumers": {"url": fg.url_s3_consumers, "type": "csv"},
        "restaurants": {"url": fg.url_s3_merchants, "type": "csv"},
        "ab_test": {"url": fg.url_s3_ab_test, "type": "tar"}
    }

    print("4. Cria o diretório de saída dos arquivos se não existir")
    fg._create_new_directory()

    print("5. Baixa e processa todos os arquivos")
    for name, info in URLS.items():
        url = info["url"]
        file_type = info["type"]
        if file_type == "tar":
            fg.download_and_extract_tar(url, fg.output_dir)
        else:
            filename = os.path.basename(url)[:-3] if url.endswith('.gz') else os.path.basename(url)
            output_path = os.path.join(fg.output_dir, filename)
            fg.download_and_decompress_gz(url, output_path)

    print("6. Processa o arquivo JSON em streaming e converte em chunks para Parquet")
    fg.split_jsonlines_to_parquet(os.path.join(fg.output_dir, "order.json"))

    print("7. Concatena os arquivos Parquet em um único arquivo parquet para facilitar a leitura")
    fg.concat_chunks_into_single_file()

    print("8. Deleta os arquivos chunks para reduzir espaço em memória ocupado - opcional")
    fg.delete_chunks()

    print("9. Gera os dataframes a partir dos arquivos para realizar as análises")
    con = duckdb.connect()

    #alias_json = os.path.join(fg.output_dir, "orders", fg.final_file_name)
    #alias_csv1 = os.path.join(fg.output_dir, "restaurant.csv")
    #alias_csv2 = os.path.join(fg.output_dir, "consumer.csv")
    #alias_tar = os.path.join(fg.output_dir, "ab_test_ref.csv")

    #df_json = con.execute(f"SELECT * FROM '{alias_json}' ").df()
    #df_csv1 = con.execute(f"""SELECT * FROM read_csv_auto('{alias_csv1}', delim=',', quote='"', escape='\\') """).df()
    #df_csv2 = con.execute(f"""SELECT * FROM read_csv_auto('{alias_csv2}', delim=',', quote='"', escape='\\') """).df()
    #df_tar = con.execute(f"SELECT * FROM '{alias_tar}' ").df()

if __name__ == "__main__":
    
    print("Iniciando execução do processo...")
    
    main()
    
    print("Processamento concluído com sucesso!")