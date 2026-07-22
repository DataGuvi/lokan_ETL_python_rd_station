import os
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()

# Identifica este projeto na tabela de logs de execução (compartilhada entre ETLs)
PROJECT_NAME = os.getenv("PROJECT_NAME", "LOKAN ETL PYTHON - RD STATION")

# RD Station CRM API V2
RD_API_BASE_URL = os.getenv("RD_API_BASE_URL", "https://api.rd.services/crm/v2")
RD_OAUTH_URL = os.getenv("RD_OAUTH_URL", "https://api.rd.services/oauth2/token")
RD_CLIENT_ID = os.getenv("RD_CLIENT_ID", "")
RD_CLIENT_SECRET = os.getenv("RD_CLIENT_SECRET", "")
RD_ACCESS_TOKEN = os.getenv("RD_ACCESS_TOKEN", "")
RD_REFRESH_TOKEN = os.getenv("RD_REFRESH_TOKEN", "")

# PostgreSQL (Azure)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "dguvi.postgres.database.azure.com")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "dllokan")
POSTGRES_USER = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "dw_rdstation")

DATABASE_URL = (
    f"postgresql://{quote_plus(POSTGRES_USER)}:{quote_plus(POSTGRES_PASSWORD)}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    f"?sslmode=require"
)
