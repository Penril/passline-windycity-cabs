import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def mysql_engine():
    host = os.environ["MYSQL_HOST"]
    port = os.environ.get("MYSQL_PORT", "3306")
    db = os.environ["MYSQL_DB"]
    user = os.environ["MYSQL_USER"]
    pwd = os.environ["MYSQL_PASSWORD"]

    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4"

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
