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
        final_file_name="orders_final.parquet"
    ):
        self.output_dir = output_dir
        self.url_s3_orders = url_s3_orders
        self.url_s3_consumers = url_s3_consumers
        self.url_s3_merchants = url_s3_merchants
        self.url_s3_ab_test = url_s3_ab_test
        self.json_chunks_prefix = "orders/part-"
        self.final_file_name = final_file_name

    def _create_new_directory(self):
        """
        Cria o diretório de saída dos arquivos. Se já existir, limpa todos os arquivos e subdiretórios dentro dele.
        """

        print("Valida se o diretório de saída existe e se existe, faz a limpeza. Cria o diretório se não existir")

        if os.path.exists(self.output_dir):
            
            print(f"Diretório {self.output_dir} já existe. Limpando arquivos antigos...", end = ' ')

            for filename in os.listdir(self.output_dir):
                file_path = os.path.join(self.output_dir, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)

            print(f"Diretório {self.output_dir} limpo.")


        else:
            print(f"Criando diretório: {self.output_dir}")
            os.makedirs(self.output_dir, exist_ok=True)

    def download_and_decompress_gz(self, url, output_path, chunk_size=1024*1024):
        """
        Baixa e descompacta um arquivo .gz a partir de uma URL.
        """

        print(f"Baixando e descompactando arquivo: {url} ...", end = ' ')

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with gzip.GzipFile(fileobj=r.raw) as gz:
                with open(output_path, 'wb') as out_f:
                    while True:
                        chunk = gz.read(chunk_size)
                        if not chunk:
                            break
                        out_f.write(chunk)
        
        print(f"Arquivo descompactado em: {output_path}")

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
                print(item)  
                count += 1
                if count >= limit:
                    print(f'Limite de {limit} itens atingido (apenas para exemplo)')
                    break
        print(f"Total processado: {count}")

    def _check_column_types(self, df, file_idx):
        """
        Verifica colunas com tipos mistos e exibe um alerta.
        """
        for col in df.columns:
            types = df[col].map(type).unique()
            if len(types) > 1:
                print(f"[ALERTA] Coluna '{col}' no chunk {file_idx} possui tipos mistos: {types}")
    
    def split_jsonlines_to_parquet(self, json_path, output_dir = None, target_size_mb = 125):
        """
        Divide um arquivo JSON em vários arquivos Parquet menores.
        """

        print(f"Dividindo arquivo JSON em chunks de {target_size_mb} MB e salvando como Parquet...")

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
                    self._check_column_types(df, file_idx)

                    # Tratamento de colunas 
                    #df['customer_id'] = df['customer_id'].astype(str)
                    #df['order_scheduled_date'] = df['order_scheduled_date'].astype(str)
                    #if 'origin_platform' in df.columns:
                    #    df['origin_platform'] = df['origin_platform'].astype(str)
                    #
                    #for col in ['customer_id', 'order_scheduled_date', 'origin_platform']:
                    #    if col in df.columns:
                    #        df[col] = df[col].fillna("").astype(str)
                    #    
                    #    elif col not in df.columns:
                    #        df[col] = ""
                    #    
                    #    else:
                    #        print("")
                    

                    parquet_path = os.path.join(output_dir, f'orders_part_{file_idx:03d}.parquet')
                    df.to_parquet(parquet_path, index=False)
                    print(f"Salvou: {parquet_path} (~{bytes_so_far/1024/1024:.2f} MB)")
                    chunk = []
                    bytes_so_far = 0
                    file_idx += 1

            # Salva o que restou
            if chunk:
                df = pd.DataFrame(chunk)
                self._check_column_types(df, file_idx)

                # Tratamento de colunas 
                #df['customer_id'] = df['customer_id'].astype(str)
                #df['order_scheduled_date'] = df['order_scheduled_date'].astype(str)
                #if 'origin_platform' in df.columns:
                #    df['origin_platform'] = df['origin_platform'].astype(str)

                #for col in ['customer_id', 'order_scheduled_date', 'origin_platform']:
                #    if col in df.columns:
                #        df[col] = df[col].fillna("").astype(str)
                #    
                #    elif col not in df.columns:
                #        df[col] = ""
                #    
                #    else:
                #        print("")

                parquet_path = os.path.join(output_dir, f'orders_part_{file_idx:03d}.parquet')
                df.to_parquet(parquet_path, index=False)
                print(f"Salvou: {parquet_path} (final)")

        print("Separação em chunks feita com sucesso!")


    def concat_chunks_into_single_file(self, input_dir=None, output_file=None):
        """
        Concatena todos os arquivos Parquet em um único arquivo Parquet de forma eficiente.
        """
        import glob

        if input_dir is None:
            input_dir = os.path.join(self.output_dir, "orders")
        if output_file is None:
            output_file = os.path.join(input_dir, self.final_file_name)

        input_paths = sorted(glob.glob(os.path.join(input_dir, "*.parquet")))

        con = duckdb.connect()
        print("Criando tabela temporária para concatenação dos chunks...")
        con.execute(f"""CREATE TABLE temp_table AS 
                    SELECT
                        cast(cpf AS VARCHAR) AS cpf
                        ,cast(customer_id AS VARCHAR) AS customer_id
                        ,cast(customer_name AS VARCHAR) AS customer_name
                        ,cast(delivery_address_city as VARCHAR) AS delivery_address_city
                        ,cast(delivery_address_country as VARCHAR) AS delivery_address_country
                        ,cast(delivery_address_district as VARCHAR) AS delivery_address_district
                        ,cast(delivery_address_external_id as VARCHAR) AS delivery_address_external_id
                        ,cast(delivery_address_latitude as FLOAT) AS delivery_address_latitude
                        ,cast(delivery_address_longitude as FLOAT) AS delivery_address_longitude
                        ,cast(delivery_address_state as VARCHAR) AS delivery_address_state
                        ,cast(delivery_address_zip_code as VARCHAR) AS delivery_address_zip_code
                        ,cast(items as VARCHAR) AS items
                        ,cast(merchant_id as VARCHAR) AS merchant_id
                        ,cast(merchant_latitude as FLOAT) AS merchant_latitude
                        ,cast(merchant_longitude as FLOAT) AS merchant_longitude
                        ,cast(merchant_timezone as VARCHAR) AS merchant_timezone
                        ,cast(order_created_at as TIMESTAMP) AS order_created_at
                        ,cast(order_id as VARCHAR) AS order_id
                        ,cast(order_scheduled as BOOL) AS order_scheduled
                        ,cast(order_total_amount as FLOAT) AS order_total_amount
                        ,cast(origin_platform as VARCHAR) AS origin_platform
                        ,cast(order_scheduled_date as TIMESTAMP) AS order_scheduled_date
                    
                    FROM 
                        read_parquet('{input_paths[0]}')
                    """)

        print(f"Adicionando {len(input_paths) - 1} chunks à tabela temporária...")
        for parquet_file in input_paths[1:]:
            print(f"Adicionando chunk: {parquet_file}")
            con.execute(f"""INSERT INTO temp_table 
                        SELECT 
                            cast(cpf AS VARCHAR) AS cpf
                            ,cast(customer_id AS VARCHAR) AS customer_id
                            ,cast(customer_name AS VARCHAR) AS customer_name
                            ,cast(delivery_address_city as VARCHAR) AS delivery_address_city
                            ,cast(delivery_address_country as VARCHAR) AS delivery_address_country
                            ,cast(delivery_address_district as VARCHAR) AS delivery_address_district
                            ,cast(delivery_address_external_id as VARCHAR) AS delivery_address_external_id
                            ,cast(delivery_address_latitude as FLOAT) AS delivery_address_latitude
                            ,cast(delivery_address_longitude as FLOAT) AS delivery_address_longitude
                            ,cast(delivery_address_state as VARCHAR) AS delivery_address_state
                            ,cast(delivery_address_zip_code as VARCHAR) AS delivery_address_zip_code
                            ,cast(items as VARCHAR) AS items
                            ,cast(merchant_id as VARCHAR) AS merchant_id
                            ,cast(merchant_latitude as FLOAT) AS merchant_latitude
                            ,cast(merchant_longitude as FLOAT) AS merchant_longitude
                            ,cast(merchant_timezone as VARCHAR) AS merchant_timezone
                            ,cast(order_created_at as TIMESTAMP) AS order_created_at
                            ,cast(order_id as VARCHAR) AS order_id
                            ,cast(order_scheduled as BOOL) AS order_scheduled
                            ,cast(order_total_amount as FLOAT) AS order_total_amount
                            ,cast(origin_platform as VARCHAR) AS origin_platform
                            ,cast(order_scheduled_date as TIMESTAMP) AS order_scheduled_date

                        FROM 
                            read_parquet('{parquet_file}')
                        """)
        con.execute(f"COPY temp_table TO '{output_file}' (FORMAT PARQUET)")
        con.close()
        print(f"Arquivo: {output_file} gerado com sucesso!")

    def delete_chunks(self, input_dir=None, keep_file=None):
        """
        Deleta todos os arquivos Parquet do diretório, exceto o arquivo final.
        """

        print("Deletando arquivos de chunks, mantendo apenas o arquivo final...")

        if input_dir is None:
            input_dir = os.path.join(self.output_dir, "orders")
        if keep_file is None:
            keep_file = self.final_file_name

        for filename in os.listdir(input_dir):
            file_path = os.path.join(input_dir, filename)
            if os.path.isfile(file_path) and filename != keep_file:
                os.remove(file_path)
                print(f"Deletado: {file_path}")