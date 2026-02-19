from sqlalchemy import text
from src.config import mysql_engine

def main():
    e = mysql_engine()
    with e.connect() as c:
        fact = c.execute(text("SELECT COUNT(*) FROM fact_trips")).scalar()
        daily = c.execute(text("SELECT COUNT(*) FROM daily_kpis")).scalar()
        print("fact_trips:", fact, "| daily_kpis:", daily)

if __name__ == "__main__":
    main()