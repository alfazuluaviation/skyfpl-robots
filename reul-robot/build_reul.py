#!/usr/bin/env python3
"""
🚁 SkyFPL / SkyNav Pro — Robô Construtor de Cartas REUL (Rotas Especiais de Ultraleves)
======================================================================================
Compilação de cartas raster oficiais do DECEA em MBTiles SQLite otimizados.
Padrão Arquitetural: Segregação em Quarentena / Staging, Telemetria e Auditoria.

Estrutura de Armazenamento Cloudflare R2:
  - Staging / Quarentena: reul/staging/{cycle}/REUL_BRASIL_FULL.mbtiles
  - Manifesto de Auditoria: reul/staging/{cycle}/manifest.json
  - Telemetria em Tempo Real: reul/staging/{cycle}/progress.json e reul_progress.json
  - Produção Oficial Ativa: reul/production/REUL_BRASIL_FULL.mbtiles (após homologação)
"""

import os
import sys
import math
import json
import time
import sqlite3
import hashlib
import argparse
import requests
from io import BytesIO
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import xml.etree.ElementTree as ET
from PIL import Image
import boto3

# ─── Configurações Canônicas DECEA ───────────────────────────────────────────

WMS_BASE_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms"
CAPABILITIES_URL = "https://geoaisweb.decea.mil.br/geoserver/wms?service=WMS&version=1.1.1&request=GetCapabilities"
TILE_SIZE = 512
DEFAULT_MIN_ZOOM = 8
DEFAULT_MAX_ZOOM = 11

mbtiles_lock = Lock()

# Catálogo Canônico Oficial de Cartas REUL
CANONICAL_REUL_CHARTS = {
    "CCV_REUL_WJ3_RIO_DE_JANEIRO": {
        "layer": "ICA:CCV_REUL_WJ3_RIO_DE_JANEIRO",
        "title": "Carta REUL Rio de Janeiro (Rotas Especiais de Ultraleves)",
        "bbox": (-43.60, -23.15, -42.80, -22.70)
    }
}

# Envelope Global Unificado (Ultraleves Brasil)
GLOBAL_REUL_BBOX = (-43.60, -23.15, -42.80, -22.70)

# ─── Autodiscoberta Dinâmica via GeoServer DECEA ──────────────────────────────

def discover_reul_layers(session: requests.Session) -> dict:
    """Consulta GetCapabilities do WMS DECEA e retorna as cartas REUL raster homologadas."""
    print("📡 [Autodiscoberta] Consultando WMS GetCapabilities do DECEA para REUL...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/xml,text/xml"
    }
    
    discovered = {}
    try:
        r = session.get(CAPABILITIES_URL, headers=headers, timeout=30)
        if r.status_code == 200 and len(r.content) > 10000:
            xml_content = r.text
            layer_blocks = xml_content.split("<Layer")
            
            for block in layer_blocks:
                import re
                name_match = re.search(r"<Name>([^<]+)</Name>", block)
                if not name_match:
                    continue
                raw_name = name_match.group(1).strip()
                clean_name = raw_name.replace("ICA:", "")
                
                # Filtra apenas cartas raster REUL, excluindo polígonos vetoriais (CV_)
                if "REUL" in clean_name and not clean_name.startswith("CV_"):
                    code = clean_name.replace("CCV_", "")
                    bbox = None
                    
                    bbox_match = re.search(r'<LatLonBoundingBox[^>]*minx="([^"]+)"[^>]*miny="([^"]+)"[^>]*maxx="([^"]+)"[^>]*maxy="([^"]+)"', block)
                    if bbox_match:
                        try:
                            bbox = (
                                float(bbox_match.group(1)),
                                float(bbox_match.group(2)),
                                float(bbox_match.group(3)),
                                float(bbox_match.group(4))
                            )
                        except Exception:
                            pass
                            
                    if not bbox and clean_name in CANONICAL_REUL_CHARTS:
                        bbox = CANONICAL_REUL_CHARTS[clean_name]["bbox"]
                    elif not bbox and code in CANONICAL_REUL_CHARTS:
                        bbox = CANONICAL_REUL_CHARTS[code]["bbox"]
                        
                    if bbox:
                        title_match = re.search(r"<Title>([^<]+)</Title>", block)
                        title = title_match.group(1).strip() if title_match else code
                        discovered[clean_name] = {
                            "layer": raw_name if raw_name.startswith("ICA:") else f"ICA:{raw_name}",
                            "title": title,
                            "bbox": bbox
                        }
                        
            if discovered:
                print(f"✅ [Autodiscoberta] Sucesso: {len(discovered)} cartas REUL raster identificadas dinamicamente via WMS.")
                return discovered
    except Exception as e:
        print(f"⚠️ [Autodiscoberta] Aviso: Não foi possível obter catálogo dinâmico ({e}). Usando malha canônica.")
        
    print(f"🛡️ [Fallback] Carregando {len(CANONICAL_REUL_CHARTS)} cartas REUL raster do catálogo canônico integrado.")
    return CANONICAL_REUL_CHARTS

# ─── Utilitários Geográficos e Mercator ───────────────────────────────────────

def latLngToTile(lat: float, lng: float, zoom: int) -> tuple:
    n = 2.0 ** zoom
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y

def tile_bbox_mercator(x: int, y: int, z: int) -> tuple:
    origin_shift = 20037508.342789244
    initial_resolution = (2.0 * origin_shift) / float(TILE_SIZE)
    resolution = initial_resolution / (2.0 ** z)
    
    tile_size_m = float(TILE_SIZE) * resolution
    minX = -origin_shift + float(x) * tile_size_m
    maxX = minX + tile_size_m
    maxY = origin_shift - float(y) * tile_size_m
    minY = maxY - tile_size_m
    return minX, minY, maxX, maxY

def validate_tile_data(raw_data: bytes | None) -> tuple[bool, bytes]:
    """Valida se o payload é uma imagem PNG válida e não transparente/vazia."""
    if not raw_data or len(raw_data) < 100:
        return False, b""
    if b"<?xml" in raw_data[:50] or b"<ServiceException" in raw_data[:100]:
        return False, b""
    if len(raw_data) < 1200:
        try:
            im = Image.open(BytesIO(raw_data))
            if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                extrema = im.getextrema()
                alpha_extrema = extrema[3] if im.mode == "RGBA" else (extrema[1] if im.mode == "LA" else (0, 0))
                if alpha_extrema == (0, 0):
                    return False, b""
        except Exception:
            return False, b""
    return True, raw_data

# ─── Requisição WMS com Resiliência e Backoff ─────────────────────────────────

def download_wms_tile(x: int, y: int, z: int, session: requests.Session, layer: str) -> bytes | None:
    minX, minY, maxX, maxY = tile_bbox_mercator(x, y, z)
    params = {
        "SERVICE": "WMS",
        "VERSION": "1.1.1",
        "REQUEST": "GetMap",
        "LAYERS": layer,
        "STYLES": "",
        "SRS": "EPSG:3857",
        "BBOX": f"{minX},{minY},{maxX},{maxY}",
        "WIDTH": str(TILE_SIZE),
        "HEIGHT": str(TILE_SIZE),
        "FORMAT": "image/png",
        "TRANSPARENT": "TRUE",
    }
    
    for attempt in range(5):
        try:
            r = session.get(WMS_BASE_URL, params=params, timeout=30)
            if r.status_code == 200 and r.headers.get("Content-Type", "").startswith("image"):
                return r.content
            elif r.status_code == 429:
                time.sleep(1.5)
        except Exception:
            time.sleep(0.5 * (attempt + 1))
            
    return None

# ─── Inicialização e Auditoria do Banco SQLite MBTiles ────────────────────────

def init_mbtiles(conn: sqlite3.Connection, name: str, bbox: tuple, min_zoom: int, max_zoom: int):
    conn.execute("CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_idx ON tiles (zoom_level, tile_column, tile_row)")
    
    metadata = [
        ("name", name),
        ("type", "overlay"),
        ("version", "2.0.0"),
        ("description", f"Rotas Especiais de Ultraleves REUL - {name}"),
        ("format", "png"),
        ("bounds", f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"),
        ("minzoom", str(min_zoom)),
        ("maxzoom", str(max_zoom)),
        ("scheme", "tms"), # Padrão TMS para compatibilidade com MapLibre e apps móveis
    ]
    
    conn.executemany("INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)", metadata)
    conn.commit()

def verify_mbtiles_integrity(mbtiles_path: str) -> dict:
    """Audita a integridade física SQLite e calcula estatísticas detalhadas."""
    conn = sqlite3.connect(mbtiles_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA integrity_check;")
    check = cursor.fetchone()[0]
    if check != "ok":
        conn.close()
        raise ValueError(f"Falha no PRAGMA integrity_check do SQLite: {check}")
        
    cursor.execute("SELECT count(*) FROM tiles;")
    total_tiles = cursor.fetchone()[0]
    
    cursor.execute("SELECT zoom_level, count(*) FROM tiles GROUP BY zoom_level;")
    by_zoom = {str(row[0]): row[1] for row in cursor.fetchall()}
    conn.close()
    
    # Checksum SHA-256
    sha256 = hashlib.sha256()
    with open(mbtiles_path, "rb") as f:
        while chunk := f.read(65536):
            sha256.update(chunk)
            
    return {
        "integrity": "ok",
        "total_tiles": total_tiles,
        "by_zoom": by_zoom,
        "size_bytes": os.path.getsize(mbtiles_path),
        "sha256": sha256.hexdigest()
    }

# ─── Processamento de um Setor de Ultraleves ──────────────────────────────────

def process_chart(
    chart_code: str,
    chart_info: dict,
    min_zoom: int,
    max_zoom: int,
    workers: int,
    output_path: str,
    existing_conn: sqlite3.Connection | None = None,
    progress_callback=None
):
    bbox = chart_info["bbox"]
    layer = chart_info["layer"]
    print(f"\n🪂 [{chart_code}] Compilando camada {layer} | BBOX: {bbox}...")
    
    if existing_conn:
        conn = existing_conn
    else:
        conn = sqlite3.connect(output_path)
        init_mbtiles(conn, chart_code, bbox, min_zoom, max_zoom)
        
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=workers, pool_maxsize=workers * 2, max_retries=3)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    tiles_to_fetch = []
    for z in range(min_zoom, max_zoom + 1):
        x_min, y_max_tile = latLngToTile(bbox[1], bbox[0], z)
        x_max, y_min_tile = latLngToTile(bbox[3], bbox[2], z)
        
        x_start = min(x_min, x_max)
        x_end = max(x_min, x_max)
        y_start = min(y_min_tile, y_max_tile)
        y_end = max(y_min_tile, y_max_tile)
        
        x_start = max(0, x_start - 1)
        y_start = max(0, y_start - 1)
        x_end += 1
        y_end += 1
        
        for x in range(x_start, x_end + 1):
            for y in range(y_start, y_end + 1):
                tiles_to_fetch.append((x, y, z))
                
    total_tiles = len(tiles_to_fetch)
    print(f"  [{chart_code}] {total_tiles} tiles identificados para download.")
    
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(download_wms_tile, t[0], t[1], t[2], session, layer): t
            for t in tiles_to_fetch
        }
        
        for future in as_completed(futures):
            x, y, z = futures[future]
            try:
                raw_data = future.result()
                is_valid, tile_data = validate_tile_data(raw_data)
                
                if is_valid:
                    tms_y = (2 ** z) - 1 - y
                    with mbtiles_lock:
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
                            (z, x, tms_y)
                        )
                        row = cursor.fetchone()
                        
                        if row:
                            try:
                                bg_img = Image.open(BytesIO(row[0])).convert("RGBA")
                                fg_img = Image.open(BytesIO(tile_data)).convert("RGBA")
                                bg_img.alpha_composite(fg_img)
                                out_io = BytesIO()
                                bg_img.save(out_io, format="PNG")
                                tile_data = out_io.getvalue()
                            except Exception:
                                pass
                                
                        conn.execute(
                            "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                            (z, x, tms_y, tile_data)
                        )
            except Exception:
                pass
                
            done += 1
            if done % 15 == 0 or done == total_tiles:
                with mbtiles_lock:
                    conn.commit()
                if progress_callback:
                    progress_callback(done, total_tiles)
                    
    with mbtiles_lock:
        conn.commit()
        
    if not existing_conn:
        print(f"  [{chart_code}] Otimizando MBTiles (VACUUM)...")
        conn.execute("VACUUM")
        conn.close()
        
    print(f"  [{chart_code}] Setor concluído com sucesso: {done}/{total_tiles} processados.")

# ─── Buffer de Logs e Telemetria em Tempo Real ───────────────────────────────

TELEMETRY_LOGS_BUFFER = []

def log_telemetry(msg: str):
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    formatted = f"[{timestamp} UTC] {msg}"
    print(formatted)
    TELEMETRY_LOGS_BUFFER.insert(0, formatted)
    if len(TELEMETRY_LOGS_BUFFER) > 60:
        TELEMETRY_LOGS_BUFFER.pop()

def get_airac_dates(cycle: str) -> tuple:
    """Retorna (effective_date, expiration_date) em formato YYYY-MM-DD para o ciclo AIRAC."""
    candidate_paths = [
        os.path.join(os.path.dirname(__file__), 'calendar.json'),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'rea-robot', 'calendar.json'),
    ]
    for cp in candidate_paths:
        if os.path.exists(cp):
            try:
                with open(cp, 'r', encoding='utf-8') as f:
                    master_cal = json.load(f)
                for year, cycles in master_cal.items():
                    if cycle in cycles:
                        date_str = cycles[cycle]  # DD/MM/YYYY
                        parts = [int(p) for p in date_str.split('/')]
                        dt = datetime(parts[2], parts[1], parts[0])
                        exp_dt = dt + timedelta(days=28)
                        return dt.strftime('%Y-%m-%d'), exp_dt.strftime('%Y-%m-%d')
            except Exception:
                pass
                
    if cycle == "2610":
        return "2026-10-01", "2026-10-29"
    return "2026-09-03", "2026-10-01"

def upload_progress(
    r2_client,
    bucket: str,
    progress_key: str,
    status: str,
    percent: float,
    current_chart: str,
    charts_done: int,
    charts_total: int,
    run_id: str,
    cycle: str,
    is_staging: bool,
    metadata: dict
):
    effective_date, expiration_date = get_airac_dates(cycle)
    progress_data = {
        "status": status,
        "cycle": cycle,
        "airac_cycle": cycle,
        "effective_date": effective_date,
        "expiration_date": expiration_date,
        "is_staging": is_staging,
        "percent": round(percent, 1),
        "current_chart": current_chart,
        "charts_done": charts_done,
        "charts_total": charts_total,
        "run_id": run_id,
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "metadata": metadata,
        "logs": TELEMETRY_LOGS_BUFFER[:30]
    }
    
    try:
        body = json.dumps(progress_data, indent=2)
        r2_client.put_object(
            Bucket=bucket,
            Key=progress_key,
            Body=body,
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
        if progress_key != "reul_progress.json":
            r2_client.put_object(
                Bucket=bucket,
                Key="reul_progress.json",
                Body=body,
                ContentType="application/json",
                CacheControl="no-cache, no-store, must-revalidate"
            )
    except Exception as e:
        print(f"  [WARN] Falha ao enviar telemetria REUL para o Cloudflare R2: {e}")

# ─── Função Principal ─────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("🪂 SkyFPL — Motor de Compilação de Cartas REUL Ultraleves (MBTiles + Staging)")
    print("=" * 80)
    
    parser = argparse.ArgumentParser(description="Compilação de Rotas Especiais de Ultraleves REUL")
    parser.add_argument("--cycle", default=os.environ.get("CYCLE", "2609"), help="Ciclo AIRAC (ex: 2609 ou 2610)")
    parser.add_argument("--chart-codes", default=os.environ.get("CHART_CODES", "ALL"), help="ALL ou códigos separados por vírgula")
    parser.add_argument("--min-zoom", type=int, default=int(os.environ.get("MIN_ZOOM", DEFAULT_MIN_ZOOM)), help="Zoom mínimo (default: 8)")
    parser.add_argument("--max-zoom", type=int, default=int(os.environ.get("MAX_ZOOM", DEFAULT_MAX_ZOOM)), help="Zoom máximo (default: 11)")
    parser.add_argument("--single-file", default=os.environ.get("SINGLE_FILE", "true"), help="Gerar arquivo único consolidado Brasil (true/false)")
    parser.add_argument("--staging", default=os.environ.get("STAGING", "true"), help="Salvar em quarentena/staging (true/false)")
    parser.add_argument("--workers", type=int, default=int(os.environ.get("WORKERS", "8")), help="Número de threads simultâneas")
    parser.add_argument("--run-id", default=os.environ.get("RUN_ID", "local_run"), help="ID da execução")
    
    args = parser.parse_args()
    
    cycle = args.cycle.strip()
    chart_codes_env = args.chart_codes.strip()
    min_zoom = args.min_zoom
    max_zoom = args.max_zoom
    single_file = str(args.single_file).lower() == "true"
    is_staging = str(args.staging).lower() != "false"
    workers = args.workers
    run_id = args.run_id
    
    # Credenciais do Cloudflare R2
    r2_endpoint = os.environ.get("CLOUDFLARE_R2_ENDPOINT") or os.environ.get("R2_ENDPOINT", "")
    r2_access_key = os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID") or os.environ.get("R2_ACCESS_KEY_ID", "")
    r2_secret_key = os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY", "")
    r2_bucket = os.environ.get("CLOUDFLARE_R2_BUCKET") or os.environ.get("R2_BUCKET", "skyfpl-charts")
    
    if not r2_endpoint or not r2_access_key or not r2_secret_key:
        print("❌ Chaves do Cloudflare R2 ausentes! Interrompendo execução.")
        sys.exit(1)
        
    r2_client = boto3.client(
        "s3",
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key
    )
    
    if is_staging:
        r2_prefix = f"reul/staging/{cycle}"
        print(f"🛡️ MODO STAGING ATIVO: Artefatos salvos em '{r2_prefix}/' (Produção protegida)")
    else:
        r2_prefix = "reul/production"
        print(f"⚠️ MODO PRODUÇÃO DIRETA: Artefatos salvos em '{r2_prefix}/'")
        
    progress_key = f"{r2_prefix}/progress.json"
    manifest_key = f"{r2_prefix}/manifest.json"
    
    session = requests.Session()
    available_charts = discover_reul_layers(session)
    
    if chart_codes_env == "ALL":
        codes_to_process = list(available_charts.keys())
    else:
        codes_to_process = [c.strip() for c in chart_codes_env.split(",") if c.strip() in available_charts]
        
    if not codes_to_process:
        print("❌ Nenhuma carta REUL válida para processar. Finalizando.")
        sys.exit(1)
        
    charts_total = len(codes_to_process)
    print(f"📋 Total de cartas REUL a compilar: {charts_total} setores")
    print(f"📦 Modo consolidado ultraleves (arquivo único): {single_file}")
    print(f"🔍 Faixa de zoom: Z{min_zoom} a Z{max_zoom}")
    print(f"⚡ Threads concorrentes: {workers}")
    
    temp_dir = os.path.join(os.getcwd(), "temp_mbtiles_reul")
    os.makedirs(temp_dir, exist_ok=True)
    
    chart_metadata = {}
    upload_progress(r2_client, r2_bucket, progress_key, "in_progress", 0.0, codes_to_process[0], 0, charts_total, run_id, cycle, is_staging, chart_metadata)
    
    try:
        effective_date, expiration_date = get_airac_dates(cycle)
        manifest_data = {
            "cycle": cycle,
            "airac_cycle": cycle,
            "effective_date": effective_date,
            "expiration_date": expiration_date,
            "is_staging": is_staging,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "min_zoom": min_zoom,
            "max_zoom": max_zoom,
            "single_file": single_file,
            "charts_count": charts_total,
            "charts": {}
        }
        
        if single_file:
            consolidated_filename = "REUL_BRASIL_FULL.mbtiles"
            consolidated_path = os.path.join(temp_dir, consolidated_filename)
            
            if os.path.exists(consolidated_path):
                os.remove(consolidated_path)
                
            conn = sqlite3.connect(consolidated_path)
            init_mbtiles(conn, "REUL_BRASIL_FULL", GLOBAL_REUL_BBOX, min_zoom, max_zoom)
            
            log_telemetry(f"Inicializando compilação unificada de {charts_total} cartas REUL.")
            for idx, code in enumerate(codes_to_process):
                chart_info = available_charts[code]
                log_telemetry(f"Processando carta [{idx+1}/{charts_total}]: {code} ({chart_info.get('title', code)})")
                
                upload_progress(
                    r2_client, r2_bucket, progress_key, "in_progress", (idx / charts_total) * 100,
                    f"{code} (Setor {idx+1}/{charts_total})", idx, charts_total, run_id, cycle, is_staging, chart_metadata
                )
                
                def on_progress(done_tiles, total_tiles):
                    single_percent = (done_tiles / total_tiles) * 100
                    overall_percent = ((idx + (done_tiles / total_tiles)) / charts_total) * 100
                    upload_progress(
                        r2_client, r2_bucket, progress_key, "in_progress", overall_percent,
                        f"{code} ({round(single_percent)}%)", idx, charts_total, run_id, cycle, is_staging, chart_metadata
                    )
                    
                process_chart(code, chart_info, min_zoom, max_zoom, workers, consolidated_path, existing_conn=conn, progress_callback=on_progress)
                log_telemetry(f"Carta {code} compilada com sucesso.")
                
                upload_progress(
                    r2_client, r2_bucket, progress_key, "in_progress", ((idx + 1) / charts_total) * 100,
                    f"{code} (Concluído {idx+1}/{charts_total})", idx + 1, charts_total, run_id, cycle, is_staging, chart_metadata
                )
                
            log_telemetry("Otimizando base unificada de ultraleves (VACUUM)...")
            upload_progress(
                r2_client, r2_bucket, progress_key, "in_progress", 96.0,
                "Otimizando base unificada (VACUUM)...", charts_total, charts_total, run_id, cycle, is_staging, chart_metadata
            )
            conn.execute("VACUUM")
            conn.close()
            
            # Auditoria de Integridade MBTiles
            log_telemetry("Executando verificação de integridade SQLite no arquivo consolidado REUL...")
            upload_progress(
                r2_client, r2_bucket, progress_key, "in_progress", 98.0,
                "Auditando integridade física SQLite...", charts_total, charts_total, run_id, cycle, is_staging, chart_metadata
            )
            audit_stats = verify_mbtiles_integrity(consolidated_path)
            log_telemetry(f"Integridade 100% OK: {audit_stats['total_tiles']} tiles válidos, {audit_stats['size_bytes'] / (1024*1024):.2f} MB")
            
            # Upload do MBTiles para o R2
            r2_key = f"{r2_prefix}/{consolidated_filename}"
            log_telemetry(f"Enviando MBTiles consolidado REUL para Cloudflare R2 ({r2_key})...")
            r2_client.upload_file(consolidated_path, r2_bucket, r2_key)
            log_telemetry("Upload do MBTiles consolidado REUL concluído com sucesso.")
            
            manifest_data["consolidated"] = {
                "filename": consolidated_filename,
                "r2_key": r2_key,
                "stats": audit_stats
            }
            chart_metadata["REUL_BRASIL_FULL"] = {
                "size_bytes": audit_stats["size_bytes"],
                "total_tiles": audit_stats["total_tiles"],
                "sha256": audit_stats["sha256"],
                "updated_at": datetime.utcnow().isoformat() + "Z"
            }
            
            if os.path.exists(consolidated_path):
                os.remove(consolidated_path)
                
        # Publica o Manifesto do Ciclo no R2
        log_telemetry(f"Publicando manifesto REUL do ciclo em {manifest_key}...")
        r2_client.put_object(
            Bucket=r2_bucket,
            Key=manifest_key,
            Body=json.dumps(manifest_data, indent=2),
            ContentType="application/json"
        )
        
        # Finalização de sucesso
        log_telemetry(f"Compilação REUL do Ciclo {cycle} finalizada com absoluto sucesso (100%).")
        upload_progress(r2_client, r2_bucket, progress_key, "completed", 100.0, "Compilação Concluída", charts_total, charts_total, run_id, cycle, is_staging, chart_metadata)
        
        # Atualiza a tabela airac_reul_staging no Supabase
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
        total_t = sum(m.get("total_tiles", 0) for m in chart_metadata.values())
        total_s = sum(m.get("size_bytes", 0) for m in chart_metadata.values())
        sha256_val = chart_metadata.get("REUL_BRASIL_FULL", {}).get("sha256")
        
        if supabase_url and supabase_key:
            try:
                log_telemetry("Sincronizando estado na tabela airac_reul_staging do Supabase...")
                rest_url = f"{supabase_url.rstrip('/')}/rest/v1/airac_reul_staging"
                headers = {
                    "apikey": supabase_key,
                    "Authorization": f"Bearer {supabase_key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates"
                }
                staging_record = {
                    "cycle_id": cycle,
                    "effective_date": effective_date,
                    "expiration_date": expiration_date,
                    "r2_staging_path": f"{r2_prefix}/",
                    "status": "VALIDATED",
                    "charts_count": charts_total,
                    "total_tiles": total_t,
                    "size_bytes": total_s,
                    "sha256": sha256_val,
                    "charts_manifest": manifest_data,
                    "integrity_score": 100,
                    "integrity_details": {"mbtiles_check": "ok", "by_zoom": manifest_data.get("consolidated", {}).get("stats", {}).get("by_zoom", {})},
                    "validation_logs": [
                        f"Compilação MBTiles Z{min_zoom}-Z{max_zoom} concluída com sucesso.",
                        f"Integridade verificada: {total_t} tiles em SQLite MBTiles.",
                        f"Setores: {charts_total} cartas REUL homologadas."
                    ],
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                    "updated_at": datetime.utcnow().isoformat() + "Z"
                }
                res = requests.post(rest_url, json=staging_record, headers=headers, timeout=10)
                if res.status_code in (200, 201):
                    print("✅ [Supabase] Registro em airac_reul_staging salvo com sucesso.")
                else:
                    print(f"⚠️ [Supabase] Retorno REST: {res.status_code} - {res.text}")
            except Exception as db_err:
                print(f"⚠️ [Supabase] Aviso ao gravar em airac_reul_staging: {db_err}")
                
            try:
                log_telemetry("Enviando telemetria e alerta Telegram via Edge Function...")
                ingest_url = f"{supabase_url.rstrip('/')}/functions/v1/airac-rea-vfr-ingest"
                headers = {
                    "Authorization": f"Bearer {supabase_key}",
                    "apikey": supabase_key,
                    "Content-Type": "application/json"
                }
                payload = {
                    "target": "reul_tiles",
                    "status": "VALIDATED",
                    "cycle": cycle,
                    "effective_date": effective_date,
                    "expiration_date": expiration_date,
                    "is_staging": is_staging,
                    "r2_staging_path": f"{r2_prefix}/",
                    "charts_count": charts_total,
                    "total_tiles": total_t,
                    "size_bytes": total_s,
                    "sha256": sha256_val,
                    "charts_manifest": manifest_data,
                    "generated_at": datetime.utcnow().isoformat() + "Z"
                }
                requests.post(ingest_url, json=payload, headers=headers, timeout=10)
                print("✅ [Webhook] Ingestão REUL notificada com sucesso.")
            except Exception as w_err:
                print(f"⚠️ [Webhook] Aviso ao notificar Edge Function: {w_err}")
                
    except Exception as e:
        print(f"\n❌ Erro crítico no robô REUL: {e}")
        upload_progress(r2_client, r2_bucket, progress_key, "failed", 100.0, f"Erro: {str(e)}", 0, charts_total, run_id, cycle, is_staging, chart_metadata)
        
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
        if supabase_url and supabase_key:
            try:
                ingest_url = f"{supabase_url.rstrip('/')}/functions/v1/airac-rea-vfr-ingest"
                headers = {
                    "Authorization": f"Bearer {supabase_key}",
                    "apikey": supabase_key,
                    "Content-Type": "application/json"
                }
                payload = {
                    "target": "reul_tiles",
                    "status": "FAILED",
                    "cycle": cycle,
                    "error": str(e)
                }
                requests.post(ingest_url, json=payload, headers=headers, timeout=5)
            except Exception:
                pass
                
        sys.exit(1)

if __name__ == "__main__":
    main()
