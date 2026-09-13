import os
import requests
from dotenv import load_dotenv

load_dotenv()
url_base = os.getenv('VITE_SUPABASE_URL') or os.getenv('SUPABASE_URL')
key = os.getenv('VITE_SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_SERVICE_ROLE_KEY')

if not url_base:
    print("Erro: SUPABASE_URL não encontrada no .env")
    exit(1)

url = f"{url_base.rstrip('/')}/rest/v1/charts_procedural?limit=1"
headers = {
    'apikey': key,
    'Authorization': f"Bearer {key}"
}
r = requests.get(url, headers=headers)
if r.ok:
    data = r.json()
    if data:
        print("COLUNAS_ENCONTRADAS:", list(data[0].keys()))
    else:
        print("Tabela está vazia.")
else:
    print(f"Erro na API: {r.status_code} - {r.text}")
