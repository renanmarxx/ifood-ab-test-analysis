import os
import requests
import gzip
import ijson
import pandas as pd
import duckdb

class funcoesGerais():

    def __init__(self, output_dir, url_s3):
        self.extractor = SimpleS3Extractor()
        self.json_chunks_prefix = "orders/part-"
        self.output_dir = output_dir
        self.n_chunks_in_s3 = n_chunks_in_s3
        self.final_file_name = final_file_name
        self.url_s3 = url_s3
    
    def _create_new_directory(self):
        """
        Caso o diretório "data" não exista, ele será criado para armazenar os arquivos necessários para as análises
        """

        return os.makedirs(self.output_dir, exist_ok = True)

    def download_and_decompress_gz(self, url, output_path, chunk_size=1024*1024):
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
    
    def process_json_streaming(json_path, limit=10):
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
    
    def split_jsonlines_to_parquet(json_path, output_dir, target_size_mb=250):
        
        os.makedirs(output_dir, exist_ok=True)
        chunk = []
        file_idx = 0
        bytes_so_far = 0
        max_size_bytes = target_size_mb * 1024 * 1024 * 0.95

        with open(json_path, 'r', encoding='utf-8') as f:
            for line in f:
                chunk.append(json.loads(line))  # ou: json.loads(line)
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
        
        print("Separacao em chunks feita com sucesso!")

    
    def concat_chunks_into_single_file():
        input_path = f"./data/orders/*.parquet" ##CORRIGIR: ALTERAR O DIR

        output_path = f"./data/orders/orders_final.parquet" ##CORRIGIR: ALTERAR O DIR

        print("Concatenando os arquivos em um unico arquivo .parquet")
        print("\n")

        duckdb.sql(f"""
        COPY(SELECT * FROM '{input_path}') TO '{output_path}' (FORMAT PARQUET)
        """)

        print(f"Arquivo: {output_path} gerado com sucesso!")
    
    def delete_chunks():
        folder = f"./data/orders"
        keep_file = f"orders_final.parquet"

        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path) and filename != keep_file:
                os.remove(file_path)
                print(f"Deletado: {file_path}")