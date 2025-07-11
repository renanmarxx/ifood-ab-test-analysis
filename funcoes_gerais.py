import os
import requests
import gzip
import json
import ijson
import pandas as pd
import duckdb
import tarfile
import shutil

class FuncoesGerais:
    """
    Classe utilitária para manipulação de arquivos e dados do processo seletivo iFood.
    """

    def __init__(
        self,
        output_dir,
        url_s3_orders,
        url_s3_consumers,
        url_s3_merchants,
        url_s3_ab_test,
        n_chunks_in_s3=10,
        final_file_name="orders_final.parquet"
    ):
        self.output_dir = output_dir
        self.url_s3_orders = url_s3_orders
        self.url_s3_consumers = url_s3_consumers
        self.url_s3_merchants = url_s3_merchants
        self.url_s3_ab_test = url_s3_ab_test
        self.json_chunks_prefix = "orders/part-"
        self.n_chunks_in_s3 = n_chunks_in_s3
        self.final_file_name = final_file_name

    def _create_new_directory(self):
        """
        Cria o diretório de saída se não existir.
        """
        os.makedirs(self.output_dir, exist_ok=True)

    def download_and_decompress_gz(self, url, output_path, chunk_size=1024*1024):
        """
        Baixa e descompacta um arquivo .gz a partir de uma URL.
        """
        print(f"Baixando e descompactando arquivo: {url} ...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with gzip.GzipFile(fileobj=r.raw) as gz:
                with open(output_path, 'wb') as out_f:
                    while True:
                        chunk = gz.read(chunk_size)
                        if not chunk:
                            break
                        out_f.write(chunk)
        print(f"Download e descompactação finalizados em: {output_path}")

    def download_and_extract_tar(self, url, dest_dir):
        """
        Baixa e extrai um arquivo .tar.gz a partir de uma URL.
        """
        print(f"Baixando e extraindo arquivo tar: {url} ...")
        tar_gz_path = os.path.join(dest_dir, os.path.basename(url))
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(tar_gz_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)

        with tarfile.open(tar_gz_path, 'r:gz') as tar:
            members = [m for m in tar.getmembers() if not m.name.startswith('._')]
            tar.extractall(path=dest_dir, members=members)
        print(f"Arquivo {tar_gz_path} extraído em {dest_dir}")
        os.remove(tar_gz_path)

    def process_json_streaming(self, json_path, limit=10):
        """
        Lê um arquivo JSON em streaming e imprime os primeiros itens.
        """
        print(f"Lendo arquivo json: {json_path} em streaming com ijson...")
        count = 0
        with open(json_path, 'rb') as f:
            for item in ijson.items(f, 'item'):
                print(item)  # Aqui você faz o processamento desejado
                count += 1
                if count >= limit:
                    print(f'Limite de {limit} itens atingido (apenas para exemplo)')
                    break
        print(f"Total processado: {count}")

    def split_jsonlines_to_parquet(self, json_path, output_dir=None, target_size_mb=250):
        """
        Divide um arquivo JSON em vários arquivos Parquet menores.
        """
        if output_dir is None:
            output_dir = os.path.join(self.output_dir, "orders")
        os.makedirs(output_dir, exist_ok=True)
        chunk = []
        file_idx = 0
        bytes_so_far = 0
        max_size_bytes = target_size_mb * 1024 * 1024 * 0.95

        with open(json_path, 'r', encoding='utf-8') as f:
            for line in f:
                chunk.append(json.loads(line))
                bytes_so_far += len(line.encode('utf-8'))
                if bytes_so_far >= max_size_bytes:
                    df = pd.DataFrame(chunk)
                    parquet_path = os.path.join(output_dir, f'orders_part_{file_idx:03d}.parquet')
                    df.to_parquet(parquet_path, index=False)
                    print(f"Salvou: {parquet_path} (~{bytes_so_far/1024/1024:.2f} MB)")
                    chunk = []
                    bytes_so_far = 0
                    file_idx += 1
            # Salva o que restou
            if chunk:
                df = pd.DataFrame(chunk)
                parquet_path = os.path.join(output_dir, f'orders_part_{file_idx:03d}.parquet')
                df.to_parquet(parquet_path, index=False)
                print(f"Salvou: {parquet_path} (final)")

        print("Separação em chunks feita com sucesso!")

    def concat_chunks_into_single_file(self, input_dir=None, output_file=None):
        """
        Concatena todos os arquivos Parquet em um único arquivo Parquet.
        """
        if input_dir is None:
            input_dir = os.path.join(self.output_dir, "orders")
        if output_file is None:
            output_file = os.path.join(input_dir, self.final_file_name)

        input_path = os.path.join(input_dir, "*.parquet")

        print("Concatenando os arquivos em um único arquivo .parquet\n")
        duckdb.sql(f"""
            COPY(SELECT * FROM '{input_path}') TO '{output_file}' (FORMAT PARQUET)
        """)
        print(f"Arquivo: {output_file} gerado com sucesso!")

    def delete_chunks(self, input_dir=None, keep_file=None):
        """
        Deleta todos os arquivos Parquet do diretório, exceto o arquivo final.
        """
        if input_dir is None:
            input_dir = os.path.join(self.output_dir, "orders")
        if keep_file is None:
            keep_file = self.final_file_name

        for filename in os.listdir(input_dir):
            file_path = os.path.join(input_dir, filename)
            if os.path.isfile(file_path) and filename != keep_file:
                os.remove(file_path)
                print(f"Deletado: {file_path}")