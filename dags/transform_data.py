import json
import datetime
import pandas as pd # Para o tratamento de dados, embora aqui estejamos usando dicionários/listas
from requests.exceptions import RequestException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore



def run_transformation(ti, minio, bucket, s3_raw, s3_staged):
    """Lê os dados brutos (RAW), aplica imputação de nulos e salva na área STAGED."""
    

    s3_hook = S3Hook(aws_conn_id=minio)
    
    # Recupera o arquivo mais recente extraido usando o xcom do Airflow
    latest_key = ti.xcom_pull(task_ids='extract_data_to_minio')
    
    if not latest_key:
        print("Caminho do arquivo RAW não encontrado. Finalizando transformação.")
        raise

    print(f"Processando o arquivo RAW: {latest_key}")
    
    # Baixa o arquivo e lê o conteudo
    file_content = s3_hook.read_key(key=latest_key, bucket_name=bucket)
    data = json.loads(file_content)
    
    transformed_articles = []
    current_date_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    part = ', '

    # Loop através dos artigos para tratamento
    for article in data.get('results', []):
        
        
        # Se não houver título o artigo é descartado
        if not article.get('title'):
            continue
            
        # Imputação de Data de Publicação Faltante
        pub_date = article.get('pubDate')
        if not pub_date:
            pub_date = current_date_str 

        # Imputação de Autor
        creator = article.get('creator')
        
        if creator:
            creator = part.join(creator).strip()
        else:
            creator = 'Unknown Author'

             
        # Imputação de ID da Fonte
        source_id = article.get('source_id')
        source_id_str = source_id if source_id else 'Sem_ID'
        
        title = article['title'].strip().lower()

        
        # Definindo padrão de moeda com base no titulo e aplicando tratamento da lista
        moeda = article['coin']
        
        if moeda:
                       
            moeda = part.join(moeda).upper()
        else:
            if 'bitcoin' in title:
                moeda = 'BTC'
            elif 'ethereum' in title:
                moeda = 'ETH'
            elif 'ripple' in title or 'xrp' in title:
                moeda = 'XRP'
            else:
                moeda = ''
        

        # 2. Selecionando dados e renomeando as colunas
        transformed_articles.append({
            'DS_TITLE': article['title'].strip(),
            'DT_PUBLISHED': pub_date,
            'TP_COIN': moeda,
            'DS_AUTHOR': creator,
            'DS_PUB_LINK': article.get('link'),
            'ID_FONT': source_id_str.upper(),
            'TP_LANGUAGE': article.get('language'),
            'DT_INSERT': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
    
    
        
    print(f"Total de {len(transformed_articles)} artigos válidos após imputação.")

    if not transformed_articles:
        print("Nenhum artigo válido encontrado após a transformação.")
        return

    # Cria um arquivo no staged do MinIO com os dados transformados
    staged_file_name = latest_key.replace(s3_raw, s3_staged)
    staged_data_json = json.dumps({"results": transformed_articles}, indent=4, ensure_ascii=False)
    
    s3_hook.load_string(
        string_data=staged_data_json,
        key=staged_file_name,
        bucket_name=bucket,
        replace=True,
    )
    
    print(f"Dados transformados carregados para: s3://{bucket}/{staged_file_name}")
