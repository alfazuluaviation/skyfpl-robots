#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_airways.py
=============================================================================
ROBÔ UNIFICADO DE MALHA AÉREA E ROTAS PREFERENCIAIS (DECEA/CGNA)
Projeto: SkyFPL - Topologia Aeronáutica Soberana
Licença: Proprietário SkyFPL / Padrão Ouro de Engenharia

Propósito:
- Ingestão determinística de Aerovias de Alta (HIGH) e Baixa (LOW) do GeoAISWEB/DECEA.
- Particionamento topológico universal por interseção geométrica com waypoints oficiais.
- Ingestão das Rotas Preferenciais Oficiais (Playbook DECEA/CGNA - DASA).
- Compilação do banco SQLite de alta performance: airways_v1.db (Multi-Tabela).
- Exportação de artefatos cognitivos para o Segundo Cérebro (skyfpl-brain-vault).
- Sincronização atômica com Cloudflare R2 (skyfpl-charts / skyfpl-brain-vault).
=============================================================================
"""

import os
import sys
import time
import json
import sqlite3
import io
import re
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
import boto3
import openpyxl

# Configurações do R2 Cloudflare
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_BUCKET = os.environ.get('R2_BUCKET', 'skyfpl-charts')
R2_BUCKET_BRAIN = os.environ.get('R2_BUCKET_BRAIN', 'skyfpl-brain-vault')

# Proxy Supabase (GeoServer DECEA)
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://gongoqjjpwphhttumdjm.supabase.co')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdvbmdvcWpqcHdwaGh0dHVtZGptIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MTUyMDcsImV4cCI6MjA4Mjk5MTIwN30.XhdrWma90JeoQdGfeqCoXgGVnyiTZ5GXHszEHw3Ce2o')
PROXY_URL = f"{SUPABASE_URL}/functions/v1/proxy-geoserver"

WFS_URL = 'https://geoaisweb.decea.mil.br/geoserver/ICA/wfs'

LAYERS = [
    {'id': 'ICA:vw_aerovia_baixa_v2', 'name': 'LOW_AIRWAY'},
    {'id': 'ICA:vw_aerovia_alta_v2', 'name': 'HIGH_AIRWAY'}
]

# Endpoints DECEA Playbook (CGNA)
AISWEB_PLAYBOOK_URL = 'https://aisweb.decea.mil.br/?i=espaco-aereo&p=playbook'
DASA_PLAYBOOK_XLS_URL = 'https://dasa.decea.mil.br/v1/playbook/xls/download'
DASA_PLAYBOOK_API_URL = 'https://dasa.decea.mil.br/playbook/busca/filtros'


def init_s3():
    """Inicializa cliente S3/Cloudflare R2."""
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        print("⚠️ Aviso: Credenciais R2 não configuradas no ambiente local.")
        return None
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )


def download_layer(layer_id):
    """Baixa camadas WFS completas paginadas via Proxy Supabase GeoServer."""
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
                print("  ⏳ Tentando novamente em 5 segundos...", flush=True)
                time.sleep(5)
        except Exception as e:
            print(f"  💥 Erro de conexão: {e}", flush=True)
            time.sleep(10)

    return all_features


def parse_coords_to_geojson_linestring(coords_str):
    """Converte string de coordenadas DECEA ('lon lat,lon lat') em GeoJSON LineString."""
    if not coords_str or not isinstance(coords_str, str):
        return None
    coords = []
    pairs = coords_str.strip().split(',')
    for p in pairs:
        parts = p.strip().split()
        if len(parts) >= 2:
            try:
                lon = round(float(parts[0]), 6)
                lat = round(float(parts[1]), 6)
                coords.append([lon, lat])
            except ValueError:
                continue
    if len(coords) >= 2:
        return {'type': 'LineString', 'coordinates': coords}
    return None


def download_playbook_preferred_routes():
    """
    Coleta todas as Rotas Preferenciais Oficiais em vigor no DECEA (Playbook CGNA).
    Realiza handshake de sessão/CSRF no AISWEB e baixa o inventário oficial de rotas.
    """
    print("🧭 Iniciando coleta de Rotas Preferenciais Oficiais (DECEA Playbook / CGNA)...", flush=True)
    session = requests.Session()
    headers_base = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkyFPL-Airways-Robot/2.0',
        'Referer': AISWEB_PLAYBOOK_URL
    }

    token = None
    try:
        print("  🔑 Obtendo token CSRF e sessão do portal AISWEB...", flush=True)
        r_init = session.get(AISWEB_PLAYBOOK_URL, headers=headers_base, timeout=20)
        m = re.search(r'name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']', r_init.text)
        if not m:
            m = re.search(r'content=["\']([^"\']+)["\']\s+name=["\']csrf-token["\']', r_init.text)
        if m:
            token = m.group(1)
            print(f"  ✅ Token CSRF autenticado com sucesso: {token[:8]}...", flush=True)
        else:
            print("  ⚠️ Aviso: Token CSRF não encontrado diretamente no HTML.", flush=True)
    except Exception as e:
        print(f"  ⚠️ Aviso na conexão inicial AISWEB: {e}", flush=True)

    headers = dict(headers_base)
    if token:
        headers['X-CSRF-TOKEN'] = token
        headers['X-Requested-With'] = 'XMLHttpRequest'

    routes = []

    # 1. Tentativa Principal: Download Direto em Lote via Excel Oficial do DECEA
    try:
        print(f"  📥 Solicitando inventário de rotas via endpoint oficial: {DASA_PLAYBOOK_XLS_URL}...", flush=True)
        r_xls = session.get(DASA_PLAYBOOK_XLS_URL, headers=headers, timeout=30)
        if r_xls.status_code == 200 and len(r_xls.content) > 1000:
            wb = openpyxl.load_workbook(io.BytesIO(r_xls.content), read_only=True)
            ws = wb.active
            print(f"  📊 Planilha DECEA carregada com sucesso. Linhas brutas: {ws.max_row}", flush=True)

            for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
                if not row or len(row) < 7:
                    continue

                r_id = row[0]
                ident = row[1]
                espaco = row[2]
                tipo_rota = row[3]
                adep = row[4]
                ades = row[5]
                rota_txt = row[6]

                # Se a linha não tiver dados básicos válidos, ignora
                if not ident or not adep or not ades or not rota_txt:
                    continue

                adep_str = str(adep).strip().upper()
                ades_str = str(ades).strip().upper()
                ident_str = str(ident).strip().upper()
                rota_clean = str(rota_txt).strip().replace('\r', '').replace('\n', ' ')

                # Coordenadas geográficas DECEA (coluna 11 - índice 10)
                coords_str = row[10] if len(row) > 10 else None
                geom = parse_coords_to_geojson_linestring(coords_str)

                adep_iata = str(row[20]).strip().upper() if len(row) > 20 and row[20] else None
                ades_iata = str(row[21]).strip().upper() if len(row) > 21 and row[21] else None
                em_vigor = bool(row[22]) if len(row) > 22 and row[22] is not None else True
                status_str = str(row[23]).strip() if len(row) > 23 and row[23] else 'Ativa'
                updated_at_str = str(row[9]).strip() if len(row) > 9 and row[9] else datetime.now(timezone.utc).isoformat()

                route_obj = {
                    'id': f"decea_playbook_{r_id}" if r_id else f"decea_{ident_str}_{adep_str}_{ades_str}",
                    'ident': ident_str,
                    'airspace_level': str(espaco).strip().upper() if espaco else 'SUPERIOR',
                    'route_type': str(tipo_rota).strip().upper() if tipo_rota else 'PREF',
                    'adep': adep_str,
                    'ades': ades_str,
                    'adep_iata': adep_iata,
                    'ades_iata': ades_iata,
                    'route_text': rota_clean,
                    'geom_json': json.dumps(geom) if geom else None,
                    'em_vigor': 1 if em_vigor else 0,
                    'status': status_str,
                    'updated_at': updated_at_str
                }
                routes.append(route_obj)

            print(f"  ✅ Extraídas com sucesso {len(routes)} rotas preferenciais ativas do DECEA Playbook.", flush=True)
            return routes
        else:
            print(f"  ❌ Endpoint XLS retornou status {r_xls.status_code}. Tentando fallback via API...", flush=True)
    except Exception as e:
        print(f"  ⚠️ Falha no download XLS: {e}. Executando fallback via API...", flush=True)

    # 2. Fallback: API paginada /playbook/busca/filtros
    try:
        page = 1
        while True:
            r_api = session.post(f"{DASA_PLAYBOOK_API_URL}?page={page}", json={}, headers=headers, timeout=20)
            if r_api.status_code != 200:
                break
            dados = r_api.json().get('dados', {})
            items = dados.get('data', [])
            if not items:
                break
            for item in items:
                if not item.get('em_vigor'):
                    continue
                adep_str = str(item.get('adep', '')).strip().upper()
                ades_str = str(item.get('ades', '')).strip().upper()
                ident_str = str(item.get('ident', '')).strip().upper()
                rota_txt = str(item.get('rota', '')).strip().replace('\r', '').replace('\n', ' ')
                r_id = item.get('id')

                route_obj = {
                    'id': f"decea_playbook_{r_id}" if r_id else f"decea_{ident_str}_{adep_str}_{ades_str}",
                    'ident': ident_str,
                    'airspace_level': str(item.get('tipo_espaco_aereo', 'SUPERIOR')).strip().upper(),
                    'route_type': str(item.get('tipo_rota', 'PREF')).strip().upper(),
                    'adep': adep_str,
                    'ades': ades_str,
                    'adep_iata': item.get('adepiata'),
                    'ades_iata': item.get('adesiata'),
                    'route_text': rota_txt,
                    'geom_json': None,
                    'em_vigor': 1,
                    'status': item.get('status', 'Ativa'),
                    'updated_at': item.get('updated_at', datetime.now(timezone.utc).isoformat())
                }
                routes.append(route_obj)

            if page >= dados.get('last_page', 1):
                break
            page += 1
        print(f"  ✅ Fallback API concluído: {len(routes)} rotas coletadas.", flush=True)
    except Exception as e_api:
        print(f"  💥 Falha total na coleta de rotas preferenciais: {e_api}", flush=True)

    return routes


def point_to_segment_dist_and_proj(px, py, x1, y1, x2, y2):
    """Calcula a menor distância ao quadrado de um ponto a um segmento de reta e sua projeção normalizada."""
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
    Retorna lista ordenada pelo avanço longitudinal (fraction: 0.0 a 1.0).
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
    for i in range(len(coords) - 1):
        if cum_dists[i] <= target_d_start <= cum_dists[i+1]:
            seg_d = cum_dists[i+1] - cum_dists[i]
            t = (target_d_start - cum_dists[i]) / seg_d if seg_d > 0 else 0
            sliced.append([
                coords[i][0] + t * (coords[i+1][0] - coords[i][0]),
                coords[i][1] + t * (coords[i+1][1] - coords[i][1])
            ])
            break

    for i in range(len(coords)):
        if target_d_start < cum_dists[i] < target_d_end:
            sliced.append(coords[i])

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


def build_db(all_airways, waypoints_list, preferred_routes):
    """
    Compila o banco SQLite Multi-Tabela airways_v1.db:
    1. airway_segments: Aerovias de Alta e Baixa particionadas topologicamente.
    2. preferred_routes: Rotas Preferenciais Oficiais (DECEA Playbook / CGNA).
    """
    db_path = "airways_v1.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Tabela 1: Segmentos de Aerovias
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

    # Tabela 2: Rotas Preferenciais Oficiais (DECEA Playbook / CGNA)
    cursor.execute('''
        CREATE TABLE preferred_routes (
            id TEXT PRIMARY KEY,
            ident TEXT NOT NULL,
            airspace_level TEXT,
            route_type TEXT,
            adep TEXT NOT NULL,
            ades TEXT NOT NULL,
            adep_iata TEXT,
            ades_iata TEXT,
            route_text TEXT NOT NULL,
            geom_json TEXT,
            em_vigor INTEGER DEFAULT 1,
            status TEXT,
            updated_at TEXT
        )
    ''')

    inserted_airways = 0
    split_count = 0

    # 1. Processamento e Particionamento das Aerovias
    for layer_name, features in all_airways.items():
        print(f"⚙️ Processando {layer_name} ({len(features)} feições)...", flush=True)
        level_str = 'LOW' if 'baixa' in layer_name.lower() else 'HIGH'

        for f in features:
            props = f.get('properties', {})
            geom = f.get('geometry')

            airway_name = props.get('text_designator', 'UNKN')
            base_sequence = props.get('sequence', 0)
            from_fix = props.get('from_fix_ident', '').strip().upper()
            to_fix = props.get('to_fix_ident', '').strip().upper()

            coords = geom.get('coordinates', []) if geom else []

            intermediates = []
            if coords and len(coords) >= 2 and waypoints_list:
                intermediates = find_waypoints_along_linestring(coords, waypoints_list)

            if intermediates:
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
                    inserted_airways += 1
                split_count += len(intermediates)
            else:
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
                inserted_airways += 1

    # Criação dos índices da malha de aerovias
    cursor.execute('CREATE INDEX idx_airway_name ON airway_segments(airway_name)')
    cursor.execute('CREATE INDEX idx_airway_fix ON airway_segments(from_fix, to_fix)')
    cursor.execute('CREATE INDEX idx_airway_from ON airway_segments(from_fix)')
    cursor.execute('CREATE INDEX idx_airway_to ON airway_segments(to_fix)')

    # 2. Inserção das Rotas Preferenciais Oficiais
    inserted_routes = 0
    print(f"🧭 Inserindo {len(preferred_routes)} Rotas Preferenciais no SQLite...", flush=True)
    for r in preferred_routes:
        cursor.execute('''
            INSERT OR REPLACE INTO preferred_routes
            (id, ident, airspace_level, route_type, adep, ades, adep_iata, ades_iata, route_text, geom_json, em_vigor, status, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            r['id'],
            r['ident'],
            r['airspace_level'],
            r['route_type'],
            r['adep'],
            r['ades'],
            r['adep_iata'],
            r['ades_iata'],
            r['route_text'],
            r['geom_json'],
            r['em_vigor'],
            r['status'],
            r['updated_at']
        ))
        inserted_routes += 1

    # Criação dos índices das rotas preferenciais para consulta ultrarrápida (O(1)) por par ADEP -> ADES
    cursor.execute('CREATE INDEX idx_pref_adep_ades ON preferred_routes(adep, ades)')
    cursor.execute('CREATE INDEX idx_pref_adep ON preferred_routes(adep)')
    cursor.execute('CREATE INDEX idx_pref_ades ON preferred_routes(ades)')
    cursor.execute('CREATE INDEX idx_pref_ident ON preferred_routes(ident)')

    conn.commit()
    conn.close()

    # 3. Exportação de Artefato Cognitivo em JSON para o Segundo Cérebro
    json_pref_path = "preferred_routes.json"
    with open(json_pref_path, 'w', encoding='utf-8') as f_json:
        json.dump({
            'source': 'DECEA/CGNA Playbook de Rotas',
            'extracted_at': datetime.now(timezone.utc).isoformat(),
            'total_routes': len(preferred_routes),
            'routes': preferred_routes
        }, f_json, ensure_ascii=False, indent=2)

    db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
    print(f"✅ Banco SQLite {db_path} ({db_size_mb:.2f} MB) gerado com sucesso!")
    print(f"   ✈️ Segmentos de aerovias: {inserted_airways} (Particionamentos: {split_count})")
    print(f"   🧭 Rotas preferenciais DECEA: {inserted_routes}")
    print(f"   🧠 Artefato cognitivo: {json_pref_path} gerado para o Segundo Cérebro.")

    return db_path, json_pref_path


def main():
    print("=" * 75, flush=True)
    print("🚦 INICIANDO ROBÔ UNIFICADO DE MALHA AÉREA & ROTAS PREFERENCIAIS SKYFPL", flush=True)
    print("=" * 75, flush=True)
    s3 = init_s3()

    # 1. Download dos waypoints de referência para o particionamento geométrico
    print("\n[PASSO 1/4] 🌐 Baixando base de waypoints DECEA para particionamento topológico...")
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
    print("\n[PASSO 2/4] 🛰️ Baixando malha oficial de aerovias (Baixa e Alta)...")
    all_airways = {}
    for layer in LAYERS:
        all_airways[layer['name']] = download_layer(layer['id'])

    # 3. Coleta das rotas preferenciais oficiais do Playbook DECEA
    print("\n[PASSO 3/4] 🧭 Baixando Rotas Preferenciais Oficiais do Playbook DECEA (CGNA)...")
    preferred_routes = download_playbook_preferred_routes()

    # 4. Construção do banco de dados SQLite unificado e exportação para o Cérebro
    print("\n[PASSO 4/4] 🏗️ Construindo banco SQLite multi-tabela e artefatos cognitivos...")
    db_file, json_file = build_db(all_airways, waypoints_list, preferred_routes)

    # 5. Sincronização Cloudflare R2
    if s3:
        print(f"\n📦 Sincronizando artefatos com Cloudflare R2...")
        try:
            # R2 App Bucket (skyfpl-charts)
            print(f"  ☁️ Enviando {db_file} para bucket [{R2_BUCKET}] em navdata/{db_file}...")
            s3.upload_file(db_file, R2_BUCKET, f"navdata/{db_file}")
            print("  ✅ Upload do SQLite concluído com sucesso!")

            # R2 Brain Vault Bucket (skyfpl-brain-vault)
            if R2_BUCKET_BRAIN:
                try:
                    print(f"  🧠 Enviando {json_file} para bucket [{R2_BUCKET_BRAIN}] em 03_AIRSPACE_TOPOLOGY/{json_file}...")
                    s3.upload_file(json_file, R2_BUCKET_BRAIN, f"03_AIRSPACE_TOPOLOGY/{json_file}")
                    print("  ✅ Upload para o Segundo Cérebro concluído!")
                except Exception as e_vault:
                    print(f"  ⚠️ Aviso: Não foi possível subir para {R2_BUCKET_BRAIN}: {e_vault}")

            print("🚀 ✅ Sincronização com Cloudflare R2 finalizada!")
        except Exception as e:
            print(f"❌ Erro fatal no upload: {e}", flush=True)
            sys.exit(1)
    else:
        print("\n⚠️ Upload ignorado (credenciais R2 ausentes neste ambiente).")

    print("\n" + "=" * 75)
    print("🎉 EXECUÇÃO DO ROBÔ UNIFICADO CONCLUÍDA COM SUCESSO!")
    print("=" * 75, flush=True)


if __name__ == "__main__":
    main()
