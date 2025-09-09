from dotenv import load_dotenv
from googledrive import get_folder_files, parse_filenames
from duckdb import DuckDBPyConnection
import duckdb
import yaml
import os
import glob

PROJECT_NAME = "mia_dbt"

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
    
def get_local_files():
    """
    Obtiene archivos locales de la carpeta raw_data
    """
    raw_data_path = "./raw_data"
    if not os.path.exists(raw_data_path):
        print(f"La carpeta {raw_data_path} no existe")
        return {}
    
    # Buscar archivos CSV que coincidan con el patrón
    pattern = os.path.join(raw_data_path, "mercado_*.csv")
    csv_files = glob.glob(pattern)
    if len(csv_files) == 0:
        raise Exception("No se encontraron archivos en la carpeta raw_data, por favor descargue los archivos y guardelos en la carpeta '~/raw_data'")
    
    files = {}
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        # Extraer país y fecha del nombre del archivo
        # Formato esperado: mercado_brasil_20240831.csv o mercado_mexico_20240830.csv
        try:
            parts = filename.replace('.csv', '').split('_')
            if len(parts) >= 3:
                country = parts[1]  # brasil o mexico
                date = parts[2]     # 20240831
                
                if country not in files:
                    files[country] = []
                
                files[country].append({
                    'id': file_path,  # Usamos la ruta local como ID
                    'date': date,
                    'path': file_path
                })
        except Exception as e:
            print(f"Error procesando archivo {filename}: {e}")
    
    return files

def main():
    
    env_exists = os.path.exists('.env')
    
    if env_exists:
        print("Archivo .env encontrado, usando Google Drive...")
        load_dotenv()
        
        GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
        GOOGLE_DRIVE_API_KEY = os.getenv("GOOGLE_DRIVE_API_KEY")
        
        if not all([GOOGLE_DRIVE_FOLDER_ID, GOOGLE_DRIVE_API_KEY, PROJECT_NAME]):
            print("Variables de entorno faltantes, cambiando a archivos locales...")
            files = get_local_files()
        else:
            folder_files = get_folder_files(folder_id=GOOGLE_DRIVE_FOLDER_ID, api_key=GOOGLE_DRIVE_API_KEY)
            files = parse_filenames(folder_files)
    else:
        print("Archivo .env no encontrado, usando archivos locales...")
        files = get_local_files()
    
    if not files:
        print("No se encontraron archivos para procesar")
        raise Exception("No se encontraron archivos para procesar")
    
    config = get_profile_config(project=PROJECT_NAME)
    
    duckdb_path = config.get('outputs', {}).get('dev', {}).get('path')
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






    

