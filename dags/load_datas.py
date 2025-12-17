import json
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore



def run_load(table, minio, postgres, bucket_name, s3_staged):
    """Lê os dados staged do MinIO e carrega no PostgreSQL."""
    try:
        
        for i, tab in enumerate(table):
            print(f"ATENÇÃO! Iniciando carga para a tabela: {tab}")
            
            s3_hook = S3Hook(aws_conn_id=minio)
            pg_hook = PostgresHook(postgres_conn_id=postgres)
            

            # Fallback para o arquivo mais recente se XCom falhar
            keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=s3_staged)
            latest_key = max(keys, key=lambda k: s3_hook.get_key(key=k, bucket_name=bucket_name).last_modified) if keys else None
            if not keys:
                print("Nenhum arquivo STAGED encontrado para carregar. Finalizando carga.")
                return

            
            print(f"Carregando o arquivo STAGED: {latest_key}")

            # Baixa o staged, lê e converte em DataFrame
            file_content = s3_hook.read_key(key=latest_key, bucket_name=bucket_name)
            data = json.loads(file_content)
            

            if isinstance(data, list):
                articles_list = data
            else:
                articles_list = data.get('results', []) 
            # ------------------------------------

            df = pd.DataFrame(articles_list)
            
            if df.empty:
                print("DataFrame vazio. Nenhuma carga realizada.")
                return
            

            # Renomeia a coluna de data para corresponder ao SQL
            df = df.rename(columns={'data_publicacao_bruta': 'data_publicacao'})

            columns_to_load = ['DS_TITLE', 'DT_PUBLISHED', 'TP_COIN', 'DS_AUTHOR', 'DS_PUB_LINK', 'ID_FONT', 'DT_INSERT']
            
            if not all(col in df.columns for col in columns_to_load):
                print(f"Erro: Colunas do DataFrame ({df.columns.tolist()}) não correspondem ao SQL ({columns_to_load}).")
                return
            
            df = df[columns_to_load] 
        
            # Cria a tabela se não existir
            create_table_sql = f"""

            CREATE TABLE IF NOT EXISTS {tab} (
                ID SERIAL PRIMARY KEY,
                DS_TITLE VARCHAR(500),
                DT_PUBLISHED TIMESTAMP WITHOUT TIME ZONE,
                TP_COIN VARCHAR(50),
                DS_AUTHOR VARCHAR(100),
                DS_PUB_LINK VARCHAR(500),
                ID_FONT VARCHAR(50),
                DT_INSERT TIMESTAMP
            );
            """
            pg_hook.run(create_table_sql)

            # Insere os dados na tabela
            pg_hook.insert_rows(
                table=tab, 
                rows=df.values.tolist(), 
                target_fields=df.columns.tolist()
            )
            print(f"Carregamento de {len(df)} linhas em {tab} concluído com sucesso!")
                        

    except Exception as e:
        print(f"Erro durante a carga de dados: {e}")
        raise
