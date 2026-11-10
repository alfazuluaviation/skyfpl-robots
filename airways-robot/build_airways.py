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
import hashlib
from datetime import datetime, timedelta, timezone
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
R2_BRAIN_ACCESS_KEY_ID = os.environ.get('R2_BRAIN_ACCESS_KEY_ID')
R2_BRAIN_SECRET_ACCESS_KEY = os.environ.get('R2_BRAIN_SECRET_ACCESS_KEY')
R2_PUBLIC_BASE_URL = os.environ.get(
    'R2_PUBLIC_BASE_URL',
    'https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev'
).rstrip('/')

# Limiares da Trava Atômica na Nuvem (Cloud Guardrail)
MIN_AIRWAY_SEGMENTS = int(os.environ.get('MIN_AIRWAY_SEGMENTS', '3500'))
MIN_PREFERRED_ROUTES = int(os.environ.get('MIN_PREFERRED_ROUTES', '300'))
MIN_DB_SIZE_BYTES = int(os.environ.get('MIN_DB_SIZE_BYTES', '500000'))
DRY_RUN = os.environ.get('DRY_RUN', 'false').strip().lower() == 'true'

# Proxy Supabase (GeoServer DECEA) e Cérebro Soberano (Master Supervisor / Sala de Situação)
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://gongoqjjpwphhttumdjm.supabase.co')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdvbmdvcWpqcHdwaGh0dHVtZGptIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MTUyMDcsImV4cCI6MjA4Mjk5MTIwN30.XhdrWma90JeoQdGfeqCoXgGVnyiTZ5GXHszEHw3Ce2o')
PROXY_URL = f"{SUPABASE_URL}/functions/v1/proxy-geoserver"
MASTER_BRAIN_URL = os.environ.get(
    'MASTER_BRAIN_URL',
    'https://uzpkjteanhmkgnefftut.supabase.co/functions/v1/dov-master-agent'
).strip()
ROBOT_ID = 'robot-airways-playbook'
ROBOT_NAME = 'Robô Malha de Aerovias & Playbook DECEA'
ROBOT_DESC = 'Compilação unificada de aerovias (LOW/HIGH) e rotas preferenciais CGNA/DECEA em airways_v1.db + manifesto R2 com trava atômica'

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
    """Inicializa cliente S3/Cloudflare R2 para o bucket principal do app."""
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


def init_s3_brain(primary_s3):
    """Inicializa cliente R2 dedicado para o vault do Segundo Cérebro, quando configurado."""
    if all([R2_BRAIN_ACCESS_KEY_ID, R2_BRAIN_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        return boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_BRAIN_ACCESS_KEY_ID,
            aws_secret_access_key=R2_BRAIN_SECRET_ACCESS_KEY,
            region_name='auto'
        )
    return primary_s3


def sha256_file(file_path):
    """Calcula hash SHA-256 hexadecimal do arquivo em chunks de 64KB."""
    digest = hashlib.sha256()
    with open(file_path, 'rb') as f_bin:
        for chunk in iter(lambda: f_bin.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_airac_cycle(target_dt=None):
    """
    Determina o ciclo AIRAC alvo respeitando AIRAC_CYCLE manual ou a janela
    oficial ICAO D-14 com base no calendário compartilhado de navdata-robot.
    """
    manual_cycle = os.environ.get('AIRAC_CYCLE', '').strip()
    if manual_cycle:
        return manual_cycle

    now = target_dt if target_dt is not None else datetime.now(timezone.utc)
    calendar_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'navdata-robot', 'calendar.json')
    )
    all_cycles = []
    if os.path.exists(calendar_path):
        try:
            with open(calendar_path, 'r', encoding='utf-8') as f_cal:
                master_cal = json.load(f_cal)
            for _year, cycles in master_cal.items():
                for cycle_id, date_str in cycles.items():
                    parts = [int(p) for p in str(date_str).split('/')]
                    dt = datetime(parts[2], parts[1], parts[0], tzinfo=timezone.utc)
                    all_cycles.append({'cycle': str(cycle_id), 'effective_dt': dt})
        except Exception as e_cal:
            print(f"⚠️ Aviso ao ler calendar.json ({e_cal}). Usando cálculo algorítmico AIRAC.")

    all_cycles.sort(key=lambda item: item['effective_dt'])
    if all_cycles:
        current_c = all_cycles[0]
        next_c = None
        for idx, entry in enumerate(all_cycles):
            if entry['effective_dt'] <= now:
                current_c = entry
                next_c = all_cycles[idx + 1] if idx + 1 < len(all_cycles) else None
        if next_c:
            days_until_next = (next_c['effective_dt'] - now).days
            if 0 <= days_until_next <= 14:
                return next_c['cycle']
        return current_c['cycle']

    # Fallback algorítmico 28 dias a partir do ciclo 2601 (22/01/2026)
    epoch = datetime(2026, 1, 22, tzinfo=timezone.utc)
    lookahead_dt = now + timedelta(days=14)
    diff_days = (lookahead_dt - epoch).days
    step_dt = epoch + timedelta(days=(diff_days // 28) * 28)
    first_of_year = epoch
    while first_of_year >= datetime(step_dt.year, 1, 1, tzinfo=timezone.utc):
        first_of_year -= timedelta(days=28)
    first_of_year += timedelta(days=28)
    ordinal = ((step_dt - first_of_year).days // 28) + 1
    return f"{step_dt.year % 100:02d}{ordinal:02d}"


def fetch_previous_telemetry(s3):
    """Consulta o manifesto airways_telemetry.json vigente no R2 para incrementar versão."""
    if s3:
        try:
            obj = s3.get_object(Bucket=R2_BUCKET, Key='navdata/airways_telemetry.json')
            return json.loads(obj['Body'].read().decode('utf-8'))
        except Exception:
            pass
    try:
        resp = requests.get(
            f"{R2_PUBLIC_BASE_URL}/navdata/airways_telemetry.json?t={int(time.time())}",
            timeout=10
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def resolve_package_version(airac_cycle, prev_telemetry):
    """Resolve versão semântica <AIRAC>.<patch>, incrementando quando já existe publicação no ciclo."""
    manual_ver = os.environ.get('PACKAGE_VERSION', '').strip()
    if manual_ver:
        return manual_ver

    if isinstance(prev_telemetry, dict) and str(prev_telemetry.get('airac_cycle', '')) == str(airac_cycle):
        prev_ver = str(prev_telemetry.get('version', ''))
        m = re.match(rf'^{re.escape(str(airac_cycle))}\.(\d+)$', prev_ver)
        if m:
            return f"{airac_cycle}.{int(m.group(1)) + 1}"

    return f"{airac_cycle}.1"


def parse_em_vigor_flag(raw_val):
    """Normaliza coluna booleana/textual de vigência do Playbook DECEA para 1 ou 0."""
    if raw_val is None:
        return 1
    if isinstance(raw_val, bool):
        return 1 if raw_val else 0
    if isinstance(raw_val, (int, float)):
        return 1 if int(raw_val) != 0 else 0
    txt = str(raw_val).strip().lower()
    if txt in ('0', 'false', 'não', 'nao', 'no', 'n', 'inativa', 'inativo', 'cancelada', 'suspensa'):
        return 0
    return 1


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
                if adep_str in ('ADEP', 'ORIGEM') or ades_str in ('ADES', 'DESTINO') or ident_str in ('IDENT', 'ROTA'):
                    continue

                rota_clean = re.sub(r'\s+', ' ', str(rota_txt).strip().replace('\r', '').replace('\n', ' '))

                # Coordenadas geográficas DECEA (coluna 11 - índice 10)
                coords_str = row[10] if len(row) > 10 else None
                geom = parse_coords_to_geojson_linestring(coords_str)

                adep_iata = str(row[20]).strip().upper() if len(row) > 20 and row[20] else None
                ades_iata = str(row[21]).strip().upper() if len(row) > 21 and row[21] else None
                em_vigor_flag = parse_em_vigor_flag(row[22] if len(row) > 22 else 1)
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
                    'em_vigor': em_vigor_flag,
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


def build_db(all_airways, waypoints_list, preferred_routes, airac_cycle, package_version):
    """
    Compila o banco SQLite Multi-Tabela airways_v1.db + manifesto airways_telemetry.json:
    1. airway_segments: Aerovias de Alta e Baixa particionadas topologicamente.
    2. preferred_routes: Rotas Preferenciais Oficiais (DECEA Playbook / CGNA).
    3. app_metadata: Metadados canônicos (versão, ciclo AIRAC, contagens e carimbo UTC).
    """
    db_path = "airways_v1.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('PRAGMA journal_mode = OFF')
    cursor.execute('PRAGMA synchronous = OFF')

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

    # Tabela 3: Metadados Canônicos do Pacote R2
    cursor.execute('''
        CREATE TABLE app_metadata (
            key TEXT PRIMARY KEY,
            value TEXT
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

    # 3. Gravação de app_metadata antes da auditoria e compactação
    generated_iso = datetime.now(timezone.utc).isoformat()
    active_routes_count = sum(1 for r in preferred_routes if int(r.get('em_vigor', 1)) == 1)
    metadata_rows = [
        ('airways_version', str(package_version)),
        ('airac_cycle', str(airac_cycle)),
        ('airway_segments_count', str(inserted_airways)),
        ('preferred_routes_count', str(inserted_routes)),
        ('preferred_routes_active_count', str(active_routes_count)),
        ('updated_at', generated_iso),
    ]
    cursor.executemany(
        'INSERT OR REPLACE INTO app_metadata (key, value) VALUES (?, ?)',
        metadata_rows
    )

    conn.commit()
    cursor.execute('VACUUM')

    # Auditoria obrigatória pós-construção dentro do SQLite compilado
    integrity_row = cursor.execute('PRAGMA integrity_check').fetchone()
    integrity_status = integrity_row[0] if integrity_row else 'unknown'
    final_seg_count = cursor.execute('SELECT COUNT(*) FROM airway_segments').fetchone()[0]
    final_pref_count = cursor.execute('SELECT COUNT(*) FROM preferred_routes').fetchone()[0]
    final_active_pref_count = cursor.execute(
        'SELECT COUNT(*) FROM preferred_routes WHERE em_vigor = 1'
    ).fetchone()[0]

    conn.close()

    db_size_bytes = os.path.getsize(db_path)
    db_sha256 = sha256_file(db_path)

    # 4. Manifesto Leve airways_telemetry.json (Contrato App / Sentinel R2)
    telemetry_manifest_path = "airways_telemetry.json"
    manifest_payload = {
        'version': str(package_version),
        'airac_cycle': str(airac_cycle),
        'updated_at': generated_iso,
        'status': 'active',
        'airway_segments_count': final_seg_count,
        'preferred_routes_count': final_pref_count,
        'preferred_routes_active_count': final_active_pref_count,
        'db_url': f"{R2_PUBLIC_BASE_URL}/navdata/airways_v1.db",
        'db_size_bytes': db_size_bytes,
        'sha256': db_sha256,
    }
    with open(telemetry_manifest_path, 'w', encoding='utf-8') as f_tel:
        json.dump(manifest_payload, f_tel, ensure_ascii=False, indent=2)
        f_tel.write('\n')

    # 5. Exportação de Artefato Cognitivo em JSON para o Segundo Cérebro
    json_pref_path = "preferred_routes.json"
    with open(json_pref_path, 'w', encoding='utf-8') as f_json:
        json.dump({
            'source': 'DECEA/CGNA Playbook de Rotas',
            'airac_cycle': str(airac_cycle),
            'version': str(package_version),
            'extracted_at': generated_iso,
            'total_routes': len(preferred_routes),
            'routes': preferred_routes
        }, f_json, ensure_ascii=False, indent=2)

    build_report = {
        'db_path': db_path,
        'telemetry_path': telemetry_manifest_path,
        'json_pref_path': json_pref_path,
        'integrity_check': integrity_status,
        'db_size_bytes': db_size_bytes,
        'sha256': db_sha256,
        'airway_segments_count': final_seg_count,
        'preferred_routes_count': final_pref_count,
        'preferred_routes_active_count': final_active_pref_count,
        'split_count': split_count,
        'manifest': manifest_payload,
    }

    db_size_mb = db_size_bytes / (1024 * 1024)
    print(f"✅ Banco SQLite {db_path} ({db_size_mb:.2f} MB | {db_size_bytes} bytes) compilado!")
    print(f"   🏷️ Versão: {package_version} (AIRAC {airac_cycle})")
    print(f"   🔐 SHA-256: {db_sha256}")
    print(f"   🛡️ PRAGMA integrity_check: {integrity_status}")
    print(f"   ✈️ Segmentos de aerovias: {final_seg_count} (Particionamentos: {split_count})")
    print(f"   🧭 Rotas preferenciais DECEA: {final_pref_count} (em_vigor=1: {final_active_pref_count})")
    print(f"   📄 Manifesto gerado: {telemetry_manifest_path}")
    print(f"   🧠 Artefato cognitivo: {json_pref_path}")

    return build_report


def validate_cloud_guardrail(build_report, prev_telemetry):
    """
    Trava Atômica na Nuvem (Cloud Guardrail):
    Impede sobrescrita do bucket de produção no R2 caso integridade SQLite falhe ou
    qualquer tabela fique vazia / abaixo dos limiares mínimos operacionais.
    """
    reasons = []
    if build_report.get('integrity_check') != 'ok':
        reasons.append(f"PRAGMA integrity_check falhou ({build_report.get('integrity_check')})")
    if build_report.get('db_size_bytes', 0) < MIN_DB_SIZE_BYTES:
        reasons.append(
            f"Tamanho do SQLite abaixo do mínimo ({build_report.get('db_size_bytes', 0)} < {MIN_DB_SIZE_BYTES} bytes)"
        )
    if build_report.get('airway_segments_count', 0) < MIN_AIRWAY_SEGMENTS:
        reasons.append(
            f"airway_segments abaixo do mínimo ({build_report.get('airway_segments_count', 0)} < {MIN_AIRWAY_SEGMENTS})"
        )
    if build_report.get('preferred_routes_active_count', 0) < MIN_PREFERRED_ROUTES:
        reasons.append(
            f"preferred_routes (em_vigor=1) abaixo do mínimo ({build_report.get('preferred_routes_active_count', 0)} < {MIN_PREFERRED_ROUTES})"
        )

    passed = len(reasons) == 0
    guardrail_report = {
        'passed': passed,
        'checked_at': datetime.now(timezone.utc).isoformat(),
        'airac_cycle': build_report['manifest']['airac_cycle'],
        'package_version': build_report['manifest']['version'],
        'thresholds': {
            'min_airway_segments': MIN_AIRWAY_SEGMENTS,
            'min_preferred_routes': MIN_PREFERRED_ROUTES,
            'min_db_size_bytes': MIN_DB_SIZE_BYTES,
        },
        'metrics': {
            'integrity_check': build_report.get('integrity_check'),
            'db_size_bytes': build_report.get('db_size_bytes'),
            'airway_segments_count': build_report.get('airway_segments_count'),
            'preferred_routes_count': build_report.get('preferred_routes_count'),
            'preferred_routes_active_count': build_report.get('preferred_routes_active_count'),
            'sha256': build_report.get('sha256'),
        },
        'reasons': reasons,
        'previous_manifest_preserved': not passed,
        'previous_version': prev_telemetry.get('version') if isinstance(prev_telemetry, dict) else None,
    }

    with open('guardrail_report.json', 'w', encoding='utf-8') as f_guard:
        json.dump(guardrail_report, f_guard, ensure_ascii=False, indent=2)
        f_guard.write('\n')

    return passed, guardrail_report


def call_master_brain(payload):
    """Despacha telemetria/mensagens ao Cérebro Soberano (dov-master-agent) de forma resiliente."""
    if not MASTER_BRAIN_URL:
        return None
    try:
        resp = requests.post(
            MASTER_BRAIN_URL,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=12
        )
        if resp.status_code >= 400:
            print(f"⚠️ Master Brain HTTP {resp.status_code}: {resp.text[:180]}", flush=True)
            return None
        return resp.json()
    except Exception as e_brain:
        print(f"⚠️ Telemetria Master Brain indisponível no momento: {e_brain}", flush=True)
        return None


def report_robot_heartbeat(status, airac_cycle, records_processed=None, error_message="", metadata=None):
    """Atualiza o cartão vivo deste robô na Sala de Situação (robot_telemetry)."""
    payload = {
        'action': 'robot_heartbeat',
        'robot_id': ROBOT_ID,
        'name': ROBOT_NAME,
        'description': ROBOT_DESC,
        'icon': 'git-branch',
        'status': status,
        'current_cycle': f"AIRAC {airac_cycle}",
        'error_message': error_message,
        'metadata': metadata or {},
    }
    if records_processed is not None:
        payload['records_processed'] = int(records_processed)
    return call_master_brain(payload)


def post_war_room_message(
    channel_id,
    message_text,
    recipient_id='@MasterSupervisor',
    metadata=None,
    requires_approval=False,
    approval_status='NONE'
):
    """Registra ocorrência/relatório no chat persistente inter-agentes (agent_messages)."""
    return call_master_brain({
        'action': 'post_agent_message',
        'message': {
            'channel_id': channel_id,
            'sender_id': ROBOT_ID,
            'sender_name': ROBOT_NAME,
            'sender_role': 'ROBOT',
            'sender_avatar': '🛣️',
            'recipient_id': recipient_id,
            'message': message_text,
            'metadata': metadata or {},
            'requires_approval': requires_approval,
            'approval_status': approval_status,
        }
    })


def create_war_room_task(title, description, assigned_to='AiracSentinel', priority='NORMAL', status='DONE', progress=100, result=None):
    """Abre tarefa auditável na esteira da empresa (agent_tasks) visível no MasterWarRoomModal."""
    return call_master_brain({
        'action': 'create_agent_task',
        'task': {
            'title': title,
            'description': description,
            'assigned_to': assigned_to,
            'created_by': ROBOT_ID,
            'priority': priority,
            'status': status,
            'progress_percent': progress,
            'result': result or {},
        }
    })


def main():
    print("=" * 75, flush=True)
    print("🚦 INICIANDO ROBÔ UNIFICADO DE MALHA AÉREA & ROTAS PREFERENCIAIS SKYFPL", flush=True)
    print("=" * 75, flush=True)
    s3 = init_s3()
    s3_brain = init_s3_brain(s3)

    airac_cycle = resolve_airac_cycle()
    prev_telemetry = fetch_previous_telemetry(s3)
    package_version = resolve_package_version(airac_cycle, prev_telemetry)
    trigger_src = os.environ.get('GITHUB_EVENT_NAME', 'manual').strip() or 'manual'
    github_run_id = os.environ.get('GITHUB_RUN_ID', '').strip()
    print(f"📅 Ciclo AIRAC Alvo: {airac_cycle} | 🏷️ Versão Pacote: {package_version} | DRY_RUN={DRY_RUN}", flush=True)

    # Batimento inicial SYNCING para o Agente Gestor (Sala de Situação)
    report_robot_heartbeat(
        status='SYNCING',
        airac_cycle=airac_cycle,
        error_message='',
        metadata={
            'phase': 'STARTING',
            'target_version': package_version,
            'dry_run': DRY_RUN,
            'trigger': trigger_src,
            'github_run_id': github_run_id,
        }
    )

    try:
        # 1. Download dos waypoints de referência para o particionamento geométrico
        print("\n[PASSO 1/5] 🌐 Baixando base de waypoints DECEA para particionamento topológico...")
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
        print("\n[PASSO 2/5] 🛰️ Baixando malha oficial de aerovias (Baixa e Alta)...")
        all_airways = {}
        for layer in LAYERS:
            all_airways[layer['name']] = download_layer(layer['id'])

        # 3. Coleta das rotas preferenciais oficiais do Playbook DECEA
        print("\n[PASSO 3/5] 🧭 Baixando Rotas Preferenciais Oficiais do Playbook DECEA (CGNA)...")
        preferred_routes = download_playbook_preferred_routes()

        # 4. Construção do banco de dados SQLite unificado e manifesto JSON
        print("\n[PASSO 4/5] 🏗️ Construindo banco SQLite multi-tabela, manifesto e artefatos cognitivos...")
        build_report = build_db(all_airways, waypoints_list, preferred_routes, airac_cycle, package_version)

        # 5. Trava Atômica na Nuvem (Cloud Guardrail) ANTES de tocar no R2
        print("\n[PASSO 5/5] 🛡️ Executando Auditoria Atômica (Cloud Guardrail)...")
        passed, guardrail_report = validate_cloud_guardrail(build_report, prev_telemetry)
        if not passed:
            print("=" * 75, flush=True)
            print("🛑 ABORTADO PELO CLOUD GUARDRAIL — UPLOAD R2 BLOQUEADO PARA PROTEGER PRODUÇÃO!", flush=True)
            for reason in guardrail_report['reasons']:
                print(f"   ❌ Motivo: {reason}", flush=True)
            prev_ver = guardrail_report.get('previous_version') or 'desconhecido'
            print(
                f"   🔒 Pacote anterior no R2 ({prev_ver}) foi 100% preservado.",
                flush=True
            )
            print("=" * 75, flush=True)

            reasons_txt = "; ".join(guardrail_report['reasons'])
            report_robot_heartbeat(
                status='ERROR',
                airac_cycle=airac_cycle,
                records_processed=build_report.get('preferred_routes_active_count', 0),
                error_message=reasons_txt,
                metadata={**guardrail_report, 'trigger': trigger_src, 'github_run_id': github_run_id}
            )
            post_war_room_message(
                channel_id='data-integrity',
                recipient_id='@MasterSupervisor',
                requires_approval=True,
                approval_status='PENDING',
                message_text=(
                    f"🛑 CLOUD GUARDRAIL ABORTOU UPLOAD ({package_version} / AIRAC {airac_cycle}): "
                    f"{reasons_txt}. Pacote anterior saudável no R2 ({prev_ver}) mantido 100% íntegro. "
                    f"Aprovar para disparar nova tentativa remota."
                ),
                metadata={
                    **guardrail_report,
                    'remote_action': 'dispatch_airways_robot',
                    'trigger': trigger_src,
                    'github_run_id': github_run_id,
                }
            )
            create_war_room_task(
                title=f"Guardrail Abortou Upload de Aerovias ({package_version})",
                description=f"Motivos: {reasons_txt}. Versão anterior ({prev_ver}) preservada no R2.",
                assigned_to='AiracSentinel',
                priority='CRITICAL',
                status='WAITING_APPROVAL',
                progress=0,
                result=guardrail_report
            )
            sys.exit(2)

        print("✅ Cloud Guardrail APROVADO! Todas as tabelas e integridade SQLite validadas.", flush=True)

        published_to_r2 = False
        if DRY_RUN:
            print("\n🧪 Modo DRY_RUN ativo: artefatos validados localmente sem sobrescrever o Cloudflare R2.", flush=True)
        elif s3:
            db_file = build_report['db_path']
            telemetry_file = build_report['telemetry_path']
            json_file = build_report['json_pref_path']
            print(f"\n📦 Sincronizando artefatos atômicos com Cloudflare R2...")
            # 1) Sobe primeiro o SQLite completo; 2) Só depois publica o manifesto apontando para ele
            print(f"  ☁️ Enviando {db_file} para bucket [{R2_BUCKET}] em navdata/{db_file}...")
            s3.upload_file(
                db_file,
                R2_BUCKET,
                f"navdata/{db_file}",
                ExtraArgs={
                    'ContentType': 'application/x-sqlite3',
                    'CacheControl': 'no-cache, must-revalidate',
                }
            )
            print("  ✅ Upload do SQLite concluído!")

            print(f"  📄 Publicando manifesto {telemetry_file} em [{R2_BUCKET}] navdata/{telemetry_file}...")
            s3.upload_file(
                telemetry_file,
                R2_BUCKET,
                f"navdata/{telemetry_file}",
                ExtraArgs={
                    'ContentType': 'application/json',
                    'CacheControl': 'no-cache, no-store, must-revalidate',
                }
            )
            print("  ✅ Manifesto airways_telemetry.json publicado!")
            published_to_r2 = True

            # R2 Brain Vault Bucket (skyfpl-brain-vault) — não bloqueante
            if R2_BUCKET_BRAIN and s3_brain:
                try:
                    print(f"  🧠 Enviando {json_file} para bucket [{R2_BUCKET_BRAIN}] em 03_AIRSPACE_TOPOLOGY/{json_file}...")
                    s3_brain.upload_file(
                        json_file,
                        R2_BUCKET_BRAIN,
                        f"03_AIRSPACE_TOPOLOGY/{json_file}",
                        ExtraArgs={'ContentType': 'application/json'}
                    )
                    print("  ✅ Upload para o Segundo Cérebro concluído!")
                except Exception as e_vault:
                    print(f"  ⚠️ Aviso: Não foi possível subir para {R2_BUCKET_BRAIN}: {e_vault}")

            print("🚀 ✅ Sincronização atômica com Cloudflare R2 finalizada!")
        else:
            print("\n⚠️ Upload ignorado (credenciais R2 ausentes neste ambiente).")

        # Telemetria Final ONLINE + Relatório no Chat Inter-Agentes + Tarefa Auditável
        seg_count = build_report['airway_segments_count']
        pref_count = build_report['preferred_routes_count']
        pref_active = build_report['preferred_routes_active_count']
        manifest_meta = {
            **build_report['manifest'],
            'split_count': build_report.get('split_count', 0),
            'dry_run': DRY_RUN,
            'published_to_r2': published_to_r2,
            'trigger': trigger_src,
            'github_run_id': github_run_id,
        }
        report_robot_heartbeat(
            status='ONLINE',
            airac_cycle=airac_cycle,
            records_processed=seg_count,
            error_message='',
            metadata=manifest_meta
        )
        mode_label = "SIMULAÇÃO (DRY_RUN)" if DRY_RUN else ("PUBLICADO EM PRODUÇÃO (R2)" if published_to_r2 else "COMPILADO LOCALMENTE")
        post_war_room_message(
            channel_id='airac-audits',
            recipient_id='@MasterSupervisor',
            message_text=(
                f"✅ [{mode_label}] Malha de Aerovias & Playbook DECEA v{package_version} (AIRAC {airac_cycle}) validada: "
                f"{seg_count} segmentos de aerovia + {pref_active} rotas preferenciais ativas | "
                f"PRAGMA integrity_check=ok | SHA-256 {build_report['sha256'][:12]}..."
            ),
            metadata=manifest_meta
        )
        if published_to_r2:
            create_war_room_task(
                title=f"Publicação R2 Aerovias & Playbook v{package_version} (AIRAC {airac_cycle})",
                description=f"{seg_count} segmentos de aerovia e {pref_active} rotas preferenciais publicados e verificados no R2.",
                assigned_to='AiracSentinel',
                priority='NORMAL',
                status='DONE',
                progress=100,
                result=manifest_meta
            )

        print("\n" + "=" * 75)
        print("🎉 EXECUÇÃO DO ROBÔ UNIFICADO CONCLUÍDA COM SUCESSO!")
        print("=" * 75, flush=True)

    except SystemExit:
        raise
    except Exception as e_fatal:
        print(f"❌ Erro fatal inesperado na execução do robô: {e_fatal}", flush=True)
        report_robot_heartbeat(
            status='ERROR',
            airac_cycle=airac_cycle,
            error_message=str(e_fatal)[:300],
            metadata={'trigger': trigger_src, 'github_run_id': github_run_id, 'dry_run': DRY_RUN}
        )
        post_war_room_message(
            channel_id='data-integrity',
            recipient_id='@MasterSupervisor',
            message_text=f"❌ Falha inesperada no Robô de Aerovias (AIRAC {airac_cycle}): {str(e_fatal)[:300]}",
            metadata={'trigger': trigger_src, 'github_run_id': github_run_id}
        )
        create_war_room_task(
            title=f"Erro na Execução do Robô de Aerovias (AIRAC {airac_cycle})",
            description=str(e_fatal)[:400],
            assigned_to='AiracSentinel',
            priority='HIGH',
            status='FAILED',
            progress=0,
            result={'error': str(e_fatal)}
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
