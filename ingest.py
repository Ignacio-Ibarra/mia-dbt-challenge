from dotenv import load_dotenv
from googledrive import get_folder_files, parse_filenames
from duckdb import DuckDBPyConnection
import duckdb
import yaml
import os



def get_profile_config(project:str) -> dict:
    with open('profiles.yml', "r") as f:
        config = yaml.safe_load(f)
    return config[project]


class Ingest:
    
    files: dict
    conn : DuckDBPyConnection
    
    def __init__(self, files:dict, conn:DuckDBPyConnection):
       
        self.files = files
        self.conn = conn
    
    def process(self, table_name:str, url:str, file_date:str):
        try:
            existing_tables = [t[0] for t in self.conn.execute("SHOW TABLES").fetchall()]
            if table_name not in existing_tables:
                self.conn.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT *, '{file_date}' AS file_date
                    FROM read_csv_auto('{url}')
                    """)
            else:
                self.conn.execute(f"""
                    INSERT INTO {table_name}
                    SELECT *, '{file_date}' AS file_date
                    FROM read_csv_auto('{url}')
                """)
        except Exception as e:
            print(f"Error procesando {url} -> {e}")
    
    def process_all(self):
        
        for table, file_infos in self.files.items():
            for file_info in file_infos:
                file_id = file_info['id']
                date = file_info['date']
                url = f"https://drive.google.com/uc?id={file_id}&export=download"
                self.process(table, url, date)
    


def main():

    load_dotenv()

    GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    GOOGLE_DRIVE_API_KEY = os.getenv("GOOGLE_DRIVE_API_KEY")

    PROJECT_NAME = os.getenv("PROJECT_NAME")

    folder_files = get_folder_files(folder_id = GOOGLE_DRIVE_FOLDER_ID, api_key=GOOGLE_DRIVE_API_KEY)

    files = parse_filenames(folder_files)

    config = get_profile_config(project=PROJECT_NAME)

    duckdb_path = config.get('outputs', {}) \
                   .get('dev', {}) \
                   .get('path')
    path = f"{PROJECT_NAME}/{duckdb_path}"
    conn = duckdb.connect(database=path)
    print(f"Connected to local DuckDB database in: {path}")

    ingestion = Ingest(files, conn)

    ingestion.process_all()

    print("Ingestion completed")
    conn.close()


# ==========================
# ENTRYPOINT
# ==========================
if __name__ == "__main__":
    main()






    

