import os
from dotenv import load_dotenv
from sqlalchemy.engine import URL

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = "aws-0-eu-west-1.pooler.supabase.com"
DB_PORT = "5432"
DB_NAME = "postgres"

DATABASE_URL = URL.create(
    "postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=int(DB_PORT),
    database=DB_NAME,
)
DATA_SOURCE = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTIBPRSnUp10oHj5fhnUTlUJn-W7DtjSRtXIhySqW_1nZak5u4b7bPc9gTcWPHM22JL0aZJJa5QakM1/pub?gid=0&single=true&output=csv" # noqa E501
DATA_PATH = "/opt/airflow/dags/data"
