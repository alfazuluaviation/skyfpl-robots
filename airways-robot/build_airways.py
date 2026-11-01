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

def point_to_segment_dist_and_proj(px, py, x1, y1, x2, y2):
    dx = x2 - x1
    dy = y2 - y1
    l2 = dx * dx + dy * dy
    if l2 == 0:
        return (px - x1)**2 + (py - y1)**2, 0.0
    t = ((px - x1) * dx + (py - y1) * dy) / l2
    t_clamped = max(0.0, min(1.0, t))
    proj_x = x1 + t_clamped * dx
    proj_y = y1 + t_clamped * dy
    dist_sq = (px - proj_x)**2 + (py - proj_y)**2
    return dist_sq, t

def find_waypoints_along_linestring(coords, waypoints_list, max_dist_deg=0.015):
    """
    Identifica todos os waypoints publicados que cortam o LineString da aerovia.
    Retorna lista ordenada pelo avanço (fraction: 0.0 a 1.0).
    """
    if len(coords) < 2:
        return []
    
    cum_dists = [0.0]
    total_len = 0.0
    for i in range(len(coords) - 1):
        d = ((coords[i+1][0] - coords[i][0])**2 + (coords[i+1][1] - coords[i][1])**2)**0.5
        total_len += d
        cum_dists.append(total_len)
    
    if total_len == 0:
        return []
    
    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    min_x, max_x = min(xs) - 0.05, max(xs) + 0.05
    min_y, max_y = min(ys) - 0.05, max(ys) + 0.05
    
    matched = []
    max_d2 = max_dist_deg ** 2
    
    for wp in waypoints_list:
        w_ident, w_lng, w_lat = wp
        if not (min_x <= w_lng <= max_x and min_y <= w_lat <= max_y):
            continue
        
        best_d2 = 999.0
        best_cum_t = 0.0
        
        for i in range(len(coords) - 1):
            d2, t = point_to_segment_dist_and_proj(w_lng, w_lat, coords[i][0], coords[i][1], coords[i+1][0], coords[i+1][1])
            if d2 < best_d2:
                best_d2 = d2
                seg_d = cum_dists[i+1] - cum_dists[i]
                best_cum_t = cum_dists[i] + max(0.0, min(1.0, t)) * seg_d
        
        if best_d2 <= max_d2:
            fraction = best_cum_t / total_len
            # Ignora os próprios extremos (< 3% ou > 97%)
            if 0.03 < fraction < 0.97:
                matched.append({'ident': w_ident, 'fraction': fraction, 'lng': w_lng, 'lat': w_lat})
    
    matched.sort(key=lambda m: m['fraction'])
    return matched

def slice_linestring(coords, fraction_start, fraction_end):
    """Fatia o LineString proporcionalmente para o sub-segmento."""
    if len(coords) < 2:
        return coords
    cum_dists = [0.0]
    total_len = 0.0
    for i in range(len(coords) - 1):
        d = ((coords[i+1][0] - coords[i][0])**2 + (coords[i+1][1] - coords[i][1])**2)**0.5
        total_len += d
        cum_dists.append(total_len)
    
    if total_len == 0:
        return coords
    
    target_d_start = fraction_start * total_len
    target_d_end = fraction_end * total_len
    
    sliced = []
    # Ponto de início
    for i in range(len(coords) - 1):
        if cum_dists[i] <= target_d_start <= cum_dists[i+1]:
            seg_d = cum_dists[i+1] - cum_dists[i]
            t = (target_d_start - cum_dists[i]) / seg_d if seg_d > 0 else 0
            sliced.append([
                coords[i][0] + t * (coords[i+1][0] - coords[i][0]),
                coords[i][1] + t * (coords[i+1][1] - coords[i][1])
            ])
            break
            
    # Pontos intermediários
    for i in range(len(coords)):
        if target_d_start < cum_dists[i] < target_d_end:
            sliced.append(coords[i])
            
    # Ponto de fim
    for i in range(len(coords) - 1):
        if cum_dists[i] <= target_d_end <= cum_dists[i+1]:
            seg_d = cum_dists[i+1] - cum_dists[i]
            t = (target_d_end - cum_dists[i]) / seg_d if seg_d > 0 else 0
            sliced.append([
                coords[i][0] + t * (coords[i+1][0] - coords[i][0]),
                coords[i][1] + t * (coords[i+1][1] - coords[i][1])
            ])
            break
            
    if len(sliced) < 2:
        return coords
    return sliced

def build_db(all_airways, waypoints_list):
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
    
    inserted_count = 0
    split_count = 0
    
    for layer_name, features in all_airways.items():
        print(f"Processando {layer_name} ({len(features)} feições)...")
        level_str = 'LOW' if 'baixa' in layer_name.lower() else 'HIGH'
        
        for f in features:
            props = f.get('properties', {})
            geom = f.get('geometry')
            
            airway_name = props.get('text_designator', 'UNKN')
            base_sequence = props.get('sequence', 0)
            from_fix = props.get('from_fix_ident', '').strip().upper()
            to_fix = props.get('to_fix_ident', '').strip().upper()
            
            coords = geom.get('coordinates', []) if geom else []
            
            # 🌐 VERIFICAÇÃO GEOMÉTRICA DE FIXOS INTERMEDIÁRIOS (Particionamento Universal)
            intermediates = []
            if coords and len(coords) >= 2 and waypoints_list:
                intermediates = find_waypoints_along_linestring(coords, waypoints_list)
            
            if intermediates:
                # Particiona o segmento em múltiplos sub-segmentos:
                # from_fix -> int1 -> int2 -> ... -> to_fix
                nodes = [from_fix] + [m['ident'] for m in intermediates] + [to_fix]
                fractions = [0.0] + [m['fraction'] for m in intermediates] + [1.0]
                
                for idx in range(len(nodes) - 1):
                    n_from = nodes[idx]
                    n_to = nodes[idx + 1]
                    sub_seq = base_sequence * 100 + idx
                    sub_id = f"{airway_name}_{base_sequence}_{n_from}_{n_to}"
                    sub_coords = slice_linestring(coords, fractions[idx], fractions[idx + 1])
                    sub_geom = {'type': 'LineString', 'coordinates': sub_coords}
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO airway_segments 
                        (id, airway_name, sequence, from_fix, to_fix, level, min_alt, max_alt, geom_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        sub_id,
                        airway_name,
                        sub_seq,
                        n_from,
                        n_to,
                        level_str,
                        props.get('altitude_minima', 0),
                        props.get('altitude_maxima', 0),
                        json.dumps(sub_geom)
                    ))
                    inserted_count += 1
                split_count += len(intermediates)
            else:
                # Segmento sem fixos intermediários — insere integralmente
                seg_id = f.get('id', f"{airway_name}_{base_sequence}_{from_fix}_{to_fix}")
                cursor.execute('''
                    INSERT OR REPLACE INTO airway_segments 
                    (id, airway_name, sequence, from_fix, to_fix, level, min_alt, max_alt, geom_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    seg_id,
                    airway_name,
                    base_sequence * 100,
                    from_fix,
                    to_fix,
                    level_str,
                    props.get('altitude_minima', 0),
                    props.get('altitude_maxima', 0),
                    json.dumps(geom)
                ))
                inserted_count += 1
                
    cursor.execute('CREATE INDEX idx_airway_name ON airway_segments(airway_name)')
    cursor.execute('CREATE INDEX idx_airway_fix ON airway_segments(from_fix, to_fix)')
    cursor.execute('CREATE INDEX idx_airway_from ON airway_segments(from_fix)')
    cursor.execute('CREATE INDEX idx_airway_to ON airway_segments(to_fix)')
    
    conn.commit()
    conn.close()
    print(f"✅ Banco de dados {db_path} gerado com sucesso! Total segmentos: {inserted_count} (Particionamentos: {split_count}).")
    return db_path

def main():
    print("🚦 Iniciando Robô Processador de Aerovias SkyFPL (Modo Topológico Universal)...", flush=True)
    s3 = init_s3()
    
    # 1. Download dos waypoints de referência para o particionamento geométrico
    print("🌐 Baixando base de waypoints DECEA para particionamento topológico...")
    waypoint_features = download_layer('ICA:waypoint')
    waypoints_list = []
    for wf in waypoint_features:
        p = wf.get('properties', {})
        ident = p.get('ident')
        lat = p.get('latitude')
        lng = p.get('longitude')
        if ident and lat is not None and lng is not None:
            waypoints_list.append((ident.strip().upper(), float(lng), float(lat)))
    print(f"📍 {len(waypoints_list)} waypoints carregados para análise espacial.")
    
    # 2. Download das aerovias
    all_airways = {}
    for layer in LAYERS:
        all_airways[layer['name']] = download_layer(layer['id'])
        
    print("🏗️ Iniciando construção e particionamento topológico do banco de dados SQLite...", flush=True)
    db_file = build_db(all_airways, waypoints_list)
    
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
