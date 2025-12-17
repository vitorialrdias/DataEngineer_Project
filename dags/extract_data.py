
import json
import datetime
import time
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.models import Variable # type: ignore



def run_extraction_and_upload(max_pages, s3, minio, bucket):
    """Extrai dados paginados do NewsData.io e carrega na área RAW do MinIO."""
    
    try:
        API_KEY = Variable.get("NEWS_API_KEY", default_var=None) 
        API_URL = "https://newsdata.io/api/1/crypto"
        
        print("Iniciando extração de dados da API NewsData.io...")
        

        # Levanta um erro se a chave de AOU não estiver definida no Airflow
        if not API_KEY:
            raise ValueError("A chave de API 'NEWS_API_KEY' não foi definida nas Variáveis do Airflow.")
        

        all_articles = []
        next_page_token = None
        page_counter = 0
        
        # Percorre o numero de páginas definido no configs para trazer mais dados
        while page_counter < max_pages:
            query_params = {
                "apikey": API_KEY,
                "language": "pt",
                "page": next_page_token
            }
            

            response = requests.get(API_URL, params=query_params)
            response.raise_for_status() 
            data = response.json()
            
            articles = data.get("results", [])
            if not articles:
                break
                
            all_articles.extend(articles)
            next_page_token = data.get("nextPage")
            page_counter += 1
            
            if not next_page_token:
                break
            time.sleep(0.3)
            

                
        if not all_articles:
            return
        
        upload_data = {"totalArticlesCollected": len(all_articles), "results": all_articles}
        
        print(f"Total de artigos coletados: {len(all_articles)}")
        
        
        now = datetime.datetime.now()
        date_path = now.strftime("%Y%m%d")
        file_name = f"CRYPTO_news_paged_{date_path}.json" 
        object_path = (f"{s3}{date_path}/{file_name}")
        json_data = json.dumps(upload_data, indent=4, ensure_ascii=False)
    
        print(f"Carregando dados extraídos para o MinIO em {object_path}...")
        
        s3_hook = S3Hook(aws_conn_id=minio)
        s3_hook.load_string(
            string_data=json_data,
            key=object_path,
            bucket_name=bucket,
            replace=True,
        )
        
        return object_path

    except Exception as e:
        print(f"Erro durante a extração e upload: {e}")
        raise

