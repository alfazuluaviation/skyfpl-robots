import os
import sys
import time
import json
import sqlite3
import requests
import boto3
from urllib.parse import urlencode

# Configurações do R2 Cloudflare
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_BUCKET = "skyfpl-charts"

# Proxy Supabase
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://gongoqjjpwphhttumdjm.supabase.co')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdvbmdvcWpqcHdwaGh0dHVtZGptIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MTUyMDcsImV4cCI6MjA4Mjk5MTIwN30.XhdrWma90JeoQdGfeqCoXgGVnyiTZ5GXHszEHw3Ce2o')
PROXY_URL = f"{SUPABASE_URL}/functions/v1/proxy-geoserver"

WFS_URL = 'https://geoaisweb.decea.mil.br/geoserver/ICA/wfs'

LAYERS = [
    {'id': 'ICA:vw_aerovia_baixa_v2', 'name': 'LOW_AIRWAY'},
    {'id': 'ICA:vw_aerovia_alta_v2', 'name': 'HIGH_AIRWAY'}
]

def init_s3():
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        print("Aviso: Credenciais R2 não configuradas.")
        return None
    return boto3.client('s3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )

def download_layer(layer_id):
    PAGE_SIZE = 1000
    all_features = []
    start_index = 0
    
    print(f"🚀 Iniciando download da camada: {layer_id}", flush=True)
    print(f"🔗 Usando Proxy: {PROXY_URL}", flush=True)
    
    while True:
        params = {
            'typeName': layer_id,
            'maxFeatures': str(PAGE_SIZE),
            'startIndex': str(start_index)
        }
        url = f"{PROXY_URL}?{urlencode(params)}"
        
        # Priorizar chave do ambiente (GitHub Secrets)
        env_key = os.environ.get('SUPABASE_ANON_KEY')
        active_key = env_key if env_key else SUPABASE_ANON_KEY
        
        headers = {
            'Authorization': f'Bearer {active_key}',
            'apikey': active_key,
            'Accept': 'application/json'
        }
        
        try:
            print(f"  🛰️ Solicitando página {start_index // PAGE_SIZE + 1} (Índice: {start_index})...", flush=True)
            response = requests.get(url, headers=headers, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                features = data.get('features', [])
                all_features.extend(features)
                print(f"  ✅ Recebidos {len(features)} itens. Total acumulado: {len(all_features)}", flush=True)
                
                if len(features) < PAGE_SIZE:
                    print(f"  🏁 Fim da camada {layer_id} alcançado.", flush=True)
                    break
                start_index += len(features)
            else:
                print(f"  ❌ Erro HTTP {response.status_code}: {response.text[:200]}", flush=True)
                print(f"  ⏳ Tentando novamente em 5 segundos...", flush=True)
                time.sleep(5)
        except Exception as e:
            print(f"  💥 Erro de conexão: {e}", flush=True)
            time.sleep(10)
            
    return all_features

def build_db(all_airways):
    db_path = "airways_v1.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE airway_segments (
            id TEXT PRIMARY KEY,
            airway_name TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            from_fix TEXT NOT NULL,
            to_fix TEXT NOT NULL,
            level TEXT,
            min_alt INTEGER,
            max_alt INTEGER,
            geom_json TEXT
        )
    ''')
    
    for layer_name, features in all_airways.items():
        print(f"Processando {layer_name} ({len(features)} feições)...")
        for f in features:
            props = f.get('properties', {})
            geom = f.get('geometry')
            
            # Mapeamento de campos DECEA
            airway_name = props.get('text_designator', 'UNKN')
            sequence = props.get('sequence', 0)
            from_fix = props.get('from_fix_ident', '')
            to_fix = props.get('to_fix_ident', '')
            
            seg_id = f.get('id', f"{airway_name}_{from_fix}_{to_fix}")
            
            cursor.execute('''
                INSERT INTO airway_segments 
                (id, airway_name, sequence, from_fix, to_fix, level, min_alt, max_alt, geom_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                seg_id,
                airway_name,
                sequence,
                from_fix,
                to_fix,
                'LOW' if 'baixa' in layer_name.lower() else 'HIGH',
                props.get('altitude_minima', 0),
                props.get('altitude_maxima', 0),
                json.dumps(geom)
            ))
            
    cursor.execute('CREATE INDEX idx_airway_name ON airway_segments(airway_name)')
    cursor.execute('CREATE INDEX idx_airway_fix ON airway_segments(from_fix, to_fix)')
    
    conn.commit()
    conn.close()
    print(f"Banco de dados {db_path} gerado com sucesso.")
    return db_path

def main():
    print("🚦 Iniciando Robô Processador de Aerovias SkyFPL...", flush=True)
    s3 = init_s3()
    all_airways = {}
    
    for layer in LAYERS:
        all_airways[layer['name']] = download_layer(layer['id'])
        
    print("🏗️ Iniciando construção do banco de dados SQLite...", flush=True)
    db_file = build_db(all_airways)
    
    if s3:
        print(f"📦 Fazendo upload de {db_file} para R2...", flush=True)
        try:
            s3.upload_file(db_file, R2_BUCKET, f"navdata/{db_file}")
            print("🚀 ✅ Sincronização com Cloudflare R2 concluída com sucesso!", flush=True)
        except Exception as e:
            print(f"❌ Erro fatal no upload: {e}", flush=True)
            sys.exit(1)
    else:
        print("⚠️ Upload ignorado (credenciais R2 ausentes).", flush=True)

if __name__ == "__main__":
    main()
