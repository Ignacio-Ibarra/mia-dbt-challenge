import requests
import re   



def get_folder_files(folder_id: str, api_key: str) -> list:
    url = f"https://www.googleapis.com/drive/v3/files"
    params = {
        "q": f"'{folder_id}' in parents",
        "fields": "files(id,name,mimeType)",
        "key": api_key
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    files = resp.json().get("files", [])
    return files

def parse_filenames(files:list) -> dict:
    
    patron = r"mercado_(.+)_(\d{8})\.csv" # suponemos que siempre va a venir en este formato
    results = {}
    for file in files:
        match = re.match(patron, file['name'])
        if match:
            table = match.group(1)
            date = match.group(2)
            if table not in results:
                results[table] = []
            results[table].append({'id': file['id'], 'date': date})
        
    return results
