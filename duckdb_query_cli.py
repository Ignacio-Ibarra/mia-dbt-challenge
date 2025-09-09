import duckdb
import pandas as pd

pd.set_option('display.max_colwidth', None)

while True:
    try:
        q = input("duckdb SQL> ")
        if q.strip().lower() in ["exit", "quit"]:
            break
        
        with duckdb.connect("mia_dbt/mia_dbt.duckdb") as con:
            df = con.sql(q).df()
        
        
        print(df.head(40))
    except Exception as e:
        print("Error:", e)