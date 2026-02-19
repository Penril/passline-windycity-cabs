from sqlalchemy import text
from src.config import mysql_engine

def main():
    engine = mysql_engine()
    with engine.connect() as conn:
        v = conn.execute(text("SELECT 1")).scalar()
        now = conn.execute(text("SELECT NOW()")).scalar()
    print("DB OK:", v, "NOW:", now)

if __name__ == "__main__":
    main()
