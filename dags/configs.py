from airflow.models import Variable # Para ler variáveis de configuração

# --- VARIÁVEIS DE CONFIGURAÇÃO 

MINIO_CONN_ID = "minio_conn"
BUCKET_NAME = "data-container" 

# PREFIXOS DE OBJETOS NO MINIO
S3_RAW_PREFIX = "raw/"
S3_STAGED_PREFIX = "staged/"


# IDs das conexões e buckets (Ajuste se necessário)
POSTGRES_CONN_ID = "postgres_dw"
TABLE_NAME = ["noticias_crypto"]


# --- CONFIGURAÇÕES DE EXTRAÇÃO (E) ---
MAX_PAGES = 1000
DELAY_SECONDS = 2.0
