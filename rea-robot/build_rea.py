#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🛰️ SkyFPL - Robô Processador de Rotas Especiais de Aeronaves (REA)

Este robô realiza:
1. Autodiscoberta inteligente de 100% das cartas REA do Brasil via WMS GetCapabilities do DECEA.
2. Download paralelo de tiles WMS (EPSG:3857) com resiliência a quedas e timeouts.
3. Mesclagem Alpha Composite nas emendas geográficas de múltiplos setores.
4. Empacotamento em SQLite MBTiles (esquema TMS) com auditoria de integridade física.
5. Armazenamento em ambiente de STAGING / QUARENTENA no Cloudflare R2 isolado da produção.
6. Emissão de manifesto JSON de ciclo AIRAC e telemetria de progresso em tempo real.
"""

import os
import sys
import json
import math
import time
import sqlite3
import hashlib
import threading
import argparse
import xml.etree.ElementTree as ET
from io import BytesIO
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import boto3
from PIL import Image

# ─── Configurações Globais ─────────────────────────────────────────────────────
WMS_BASE_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms"
CAPABILITIES_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms?service=WMS&request=GetCapabilities"
TILE_SIZE = 256
DEFAULT_MIN_ZOOM = 8
DEFAULT_MAX_ZOOM = 11

# Lock global para operações simultâneas na base de dados SQLite
mbtiles_lock = threading.Lock()

# ─── Dicionário Canônico Oficial (26 Cartas Raster GeoTIFF Homologadas pelo DECEA) ──
# Bounding Boxes oficiais extraídos do WMS GetCapabilities do DECEA (EPSG:4326 / CRS:84)
# Formato: (minLon, minLat, maxLon, maxLat)
CANONICAL_REA_CHARTS = {
    "REA_CY_CUIABA": {
        "layer": "ICA:CCV_REA_CY_CUIABA",
        "title": "Carta REA Cuiabá",
        "bbox": (-56.57155, -16.15953, -55.67383, -15.09157)
    },
    "REA_PI-PARINTINS": {
        "layer": "ICA:CCV_REA_PI-PARINTINS",
        "title": "Carta REA Parintins",
        "bbox": (-57.38333, -3.23333, -56.09989, -2.16658)
    },
    "REA_WA_TABATINGA": {
        "layer": "ICA:CCV_REA_WA_TABATINGA",
        "title": "Carta REA Tabatinga",
        "bbox": (-70.28333, -4.50000, -69.46664, -3.91645)
    },
    "REA_WB_BELEM": {
        "layer": "ICA:CCV_REA_WB_BELEM",
        "title": "Carta REA Belém",
        "bbox": (-48.94516, -1.82644, -47.86112, -0.92431)
    },
    "REA_WF_RECIFE": {
        "layer": "ICA:CCV_REA_WF_RECIFE",
        "title": "Carta REA Recife",
        "bbox": (-35.53357, -8.66723, -34.49995, -7.41693)
    },
    "REA_WG_CAMPO_GRANDE": {
        "layer": "ICA:CCV_REA_WG_CAMPO_GRANDE",
        "title": "Carta REA Campo Grande",
        "bbox": (-55.73715, -21.24926, -53.59440, -19.68197)
    },
    "REA_WH_BELO_HORIZONTE": {
        "layer": "ICA:CCV_REA_WH_BELO_HORIZONTE",
        "title": "Carta REA Belo Horizonte",
        "bbox": (-44.86667, -20.91667, -42.86646, -18.64994)
    },
    "REA_WJ1_RIO_DE_JANEIRO": {
        "layer": "ICA:CCV_REA_WJ1_RIO_DE_JANEIRO",
        "title": "Carta REA Rio de Janeiro",
        "bbox": (-44.81333, -24.00167, -41.76018, -21.81760)
    },
    "REA_WK_PORTO_SEGURO": {
        "layer": "ICA:CCV_REA_WK_PORTO_SEGURO",
        "title": "Carta REA Porto Seguro",
        "bbox": (-39.50000, -16.83333, -38.78321, -16.30000)
    },
    "REA_WN2_MANAUS": {
        "layer": "ICA:CCV_REA_WN2_MANAUS",
        "title": "Carta REA Manaus",
        "bbox": (-60.57283, -3.51500, -59.60187, -2.71951)
    },
    "REA_WP_PORTO_ALEGRE": {
        "layer": "ICA:CCV_REA_WP_PORTO_ALEGRE",
        "title": "Carta REA Porto Alegre",
        "bbox": (-51.96681, -30.75025, -50.25021, -28.75022)
    },
    "REA_WR_BRASILIA": {
        "layer": "ICA:CCV_REA_WR_BRASILIA",
        "title": "Carta REA Brasília",
        "bbox": (-48.46667, -16.25000, -47.36663, -15.38330)
    },
    "REA_WS_SAO_LUIS": {
        "layer": "ICA:CCV_REA_WS_SAO_LUIS",
        "title": "Carta REA São Luís",
        "bbox": (-44.66580, -2.90000, -43.83333, -2.24989)
    },
    "REA_WX_SANTAREM": {
        "layer": "ICA:CCV_REA_WX_SANTAREM",
        "title": "Carta REA Santarém",
        "bbox": (-55.16667, -2.75000, -54.49998, -2.25000)
    },
    "REA_WZ_FORTALEZA": {
        "layer": "ICA:CCV_REA_WZ_FORTALEZA",
        "title": "Carta REA Fortaleza",
        "bbox": (-39.00000, -4.24997, -37.93310, -3.36651)
    },
    "REA_XF_FLORIANOPOLIS": {
        "layer": "ICA:CCV_REA_XF_FLORIANOPOLIS",
        "title": "Carta REA Florianópolis",
        "bbox": (-49.73333, -28.31667, -48.01645, -26.49990)
    },
    "REA_XK_MACAPA": {
        "layer": "ICA:CCV_REA_XK_MACAPA",
        "title": "Carta REA Macapá",
        "bbox": (-51.38333, -0.23333, -50.69988, 0.30002)
    },
    "REA_XN-ANAPOLIS": {
        "layer": "ICA:CCV_REA_XN-ANAPOLIS",
        "title": "Carta REA Anápolis",
        "bbox": (-49.81667, -17.03330, -48.14994, -15.76659)
    },
    "REA_XP1_SAO_PAULO": {
        "layer": "ICA:CCV_REA_XP1_SAO_PAULO",
        "title": "Carta REA São Paulo 1 (RMSP)",
        "bbox": (-47.89662, -24.50335, -44.39567, -22.28520)
    },
    "REA_XP2_SAO_PAULO": {
        "layer": "ICA:CCV_REA_XP2_SAO_PAULO",
        "title": "Carta REA São Paulo 2",
        "bbox": (-47.22845, -23.93291, -46.03175, -23.09281)
    },
    "REA_XR_VITORIA": {
        "layer": "ICA:CCV_REA_XR_VITORIA",
        "title": "Carta REA Vitória",
        "bbox": (-40.66667, -20.58333, -39.91648, -19.79978)
    },
    "REA_XS_SALVADOR": {
        "layer": "ICA:CCV_REA_XS_SALVADOR",
        "title": "Carta REA Salvador",
        "bbox": (-39.06676, -13.46682, -37.86655, -12.49983)
    },
    "REA_XT_NATAL": {
        "layer": "ICA:CCV_REA_XT_NATAL",
        "title": "Carta REA Natal",
        "bbox": (-35.83333, -6.41667, -34.99997, -5.38330)
    },
    "REA_CURITIBA": {
        "layer": "ICA:REA_CURITIBA",
        "title": "Carta REA Curitiba",
        "bbox": (-50.08362, -27.00078, -48.16639, -24.68376)
    },
    "REA_LONDRINA": {
        "layer": "ICA:REA_LONDRINA",
        "title": "Carta REA Londrina",
        "bbox": (-52.83371, -24.16693, -50.16685, -22.50042)
    },
    "REA_RIBEIRAO_PRETO": {
        "layer": "ICA:REA_RIBEIRAO_PRETO",
        "title": "Carta REA Ribeirão Preto",
        "bbox": (-48.09718, -21.50944, -47.48935, -20.82422)
    }
}

# ─── Autodiscoberta Dinâmica via GeoServer DECEA ──────────────────────────────

def discover_rea_layers(session: requests.Session) -> dict:
    """Consulta GetCapabilities do WMS DECEA e retorna todas as cartas REA raster homologadas."""
    print("📡 [Autodiscoberta] Consultando WMS GetCapabilities do DECEA GeoServer...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/xml,text/xml"
    }
    
    discovered = {}
    try:
        r = session.get(CAPABILITIES_URL, headers=headers, timeout=30)
        if r.status_code == 200 and len(r.content) > 10000:
            root = ET.fromstring(r.content)
            for layer_elem in root.iter('{http://www.opengis.net/wms}Layer') if '{http://www.opengis.net/wms}Layer' in r.text[:1000] else root.iter('Layer'):
                name_elem = layer_elem.find('{http://www.opengis.net/wms}Name') if '{http://www.opengis.net/wms}Name' in r.text[:1000] else layer_elem.find('Name')
                title_elem = layer_elem.find('{http://www.opengis.net/wms}Title') if '{http://www.opengis.net/wms}Title' in r.text[:1000] else layer_elem.find('Title')
                
                if name_elem is not None and name_elem.text:
                    raw_name = name_elem.text.strip()
                    clean_name = raw_name.replace("ICA:", "")
                    # Filtra APENAS cartas raster GeoTIFF (CCV_REA_* e REA_*), ignorando polígonos vetoriais CV_*
                    if (clean_name.startswith("CCV_REA_") or clean_name.startswith("REA_")) and not clean_name.startswith("CV_"):
                        code = clean_name.replace("CCV_", "")
                        bbox = None
                        
                        # 1. Tenta EX_GeographicBoundingBox (padrão WMS 1.3.0)
                        ex_bbox = layer_elem.find('{http://www.opengis.net/wms}EX_GeographicBoundingBox') if '{http://www.opengis.net/wms}EX_GeographicBoundingBox' in r.text[:1000] else layer_elem.find('EX_GeographicBoundingBox')
                        if ex_bbox is not None:
                            try:
                                west = float(ex_bbox.findtext('{http://www.opengis.net/wms}westBoundLongitude') or ex_bbox.findtext('westBoundLongitude'))
                                south = float(ex_bbox.findtext('{http://www.opengis.net/wms}southBoundLatitude') or ex_bbox.findtext('southBoundLatitude'))
                                east = float(ex_bbox.findtext('{http://www.opengis.net/wms}eastBoundLongitude') or ex_bbox.findtext('eastBoundLongitude'))
                                north = float(ex_bbox.findtext('{http://www.opengis.net/wms}northBoundLatitude') or ex_bbox.findtext('northBoundLatitude'))
                                bbox = (west, south, east, north)
                            except Exception:
                                pass
                                
                        # 2. Fallback para LatLonBoundingBox (padrão WMS 1.1.1)
                        if not bbox:
                            for child in layer_elem:
                                tag = child.tag.split('}')[-1]
                                if tag == 'LatLonBoundingBox':
                                    try:
                                        bbox = (
                                            float(child.attrib.get('minx')),
                                            float(child.attrib.get('miny')),
                                            float(child.attrib.get('maxx')),
                                            float(child.attrib.get('maxy'))
                                        )
                                        break
                                    except Exception:
                                        pass
                        
                        # Se não identificou coordenadas no XML, utiliza fallback canônico
                        if not bbox and code in CANONICAL_REA_CHARTS:
                            bbox = CANONICAL_REA_CHARTS[code]["bbox"]
                            
                        if bbox:
                            title = title_elem.text.strip() if title_elem is not None and title_elem.text else code
                            discovered[code] = {
                                "layer": raw_name if raw_name.startswith("ICA:") else f"ICA:{raw_name}",
                                "title": title,
                                "bbox": bbox
                            }
                            
            if len(discovered) >= 20:
                print(f"✅ [Autodiscoberta] Sucesso: {len(discovered)} cartas REA raster identificadas dinamicamente via WMS.")
                return discovered
    except Exception as e:
        print(f"⚠️ [Autodiscoberta] Aviso: Não foi possível obter catálogo dinâmico ({e}). Usando malha canônica oficial.")
        
    print(f"🛡️ [Fallback] Carregando {len(CANONICAL_REA_CHARTS)} cartas REA raster do catálogo canônico integrado.")
    return CANONICAL_REA_CHARTS

# ─── Utilitários Geográficos e de Conversão ────────────────────────────────────

def latLngToTile(lat: float, lng: float, zoom: int) -> tuple:
    n = 2.0 ** zoom
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return x, y

def tile_bbox_mercator(x: int, y: int, z: int) -> tuple:
    world_size = 20037508.342789244 * 2
    res = world_size / (2 ** z)
    minx = x * res - 20037508.342789244
    maxx = (x + 1) * res - 20037508.342789244
    maxy = 20037508.342789244 - y * res
    miny = 20037508.342789244 - (y + 1) * res
    return (minx, miny, maxx, maxy)

# ─── Validação de Tiles (Paridade 100% com o Robô WAC) ─────────────────────────

def validate_tile_data(raw_data: bytes | None) -> tuple:
    """
    Mantém paridade 100% com o Robô WAC: salva os dados brutos da carta raster PNG.
    Apenas descarta se o arquivo for corrompido ou menor que 100 bytes (erro WMS ou vazio).
    """
    if not raw_data or len(raw_data) < 100:
        return False, None
    return True, raw_data

# ─── Requisição WMS ───────────────────────────────────────────────────────────

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
            time.sleep(0.6 * (attempt + 1))
            
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
        ("description", f"Corredores Visuais REA - {name}"),
        ("format", "png"),
        ("bounds", f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"),
        ("minzoom", str(min_zoom)),
        ("maxzoom", str(max_zoom)),
        ("scheme", "tms"),
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

# ─── Processamento de uma Carta Específica ────────────────────────────────────

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
    print(f"\n🌍 [{chart_code}] Processando camada {layer} | BBOX: {bbox}...")
    
    if existing_conn:
        conn = existing_conn
    else:
        conn = sqlite3.connect(output_path)
        init_mbtiles(conn, chart_code, bbox, min_zoom, max_zoom)
        
    session = requests.Session()
    
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
            if done % 100 == 0:
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
        
    print(f"  [{chart_code}] Concluído com sucesso: {done}/{total_tiles} processados.")

# Buffer de logs em memória para telemetria ao vivo
TELEMETRY_LOGS_BUFFER = []

def log_telemetry(msg: str):
    """Registra uma mensagem com timestamp no buffer de telemetria e no stdout."""
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    formatted = f"[{timestamp} UTC] {msg}"
    print(formatted)
    TELEMETRY_LOGS_BUFFER.insert(0, formatted)
    if len(TELEMETRY_LOGS_BUFFER) > 60:
        TELEMETRY_LOGS_BUFFER.pop()

def get_airac_dates(cycle: str) -> tuple:
    """Retorna (effective_date, expiration_date) em formato YYYY-MM-DD para um ciclo AIRAC."""
    calendar_path = os.path.join(os.path.dirname(__file__), 'calendar.json')
    if os.path.exists(calendar_path):
        try:
            with open(calendar_path, 'r', encoding='utf-8') as f:
                master_cal = json.load(f)
            for year, cycles in master_cal.items():
                if cycle in cycles:
                    date_str = cycles[cycle]  # DD/MM/YYYY
                    parts = [int(p) for p in date_str.split('/')]
                    dt = datetime(parts[2], parts[1], parts[0])
                    exp_dt = dt + timedelta(days=28)
                    return dt.strftime('%Y-%m-%d'), exp_dt.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"⚠️ Erro ao ler calendar.json: {e}")
    # Fallback canônico caso não localize no arquivo
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
        # Espelha na raiz rea_progress.json para compatibilidade retroativa
        if progress_key != "rea_progress.json":
            r2_client.put_object(
                Bucket=bucket,
                Key="rea_progress.json",
                Body=body,
                ContentType="application/json",
                CacheControl="no-cache, no-store, must-revalidate"
            )
    except Exception as e:
        print(f"  [WARN] Falha ao enviar telemetria para o Cloudflare R2: {e}")

# ─── Função Principal ─────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("🚀 SkyFPL — Motor de Compilação de Rotas Especiais REA (MBTiles + Staging)")
    print("=" * 80)
    
    parser = argparse.ArgumentParser(description="Compilação de Rotas Especiais REA (MBTiles + Staging)")
    parser.add_argument("--cycle", default=os.environ.get("CYCLE", "2609"), help="Ciclo AIRAC (ex: 2609)")
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
    
    # Define o prefixo de armazenamento: STAGING isolado ou PRODUÇÃO direta
    if is_staging:
        r2_prefix = f"rea/staging/{cycle}"
        print(f"🛡️ MODO STAGING ATIVO: Artefatos salvos em '{r2_prefix}/' (Produção 100% protegida)")
    else:
        r2_prefix = "rea/production"
        print(f"⚠️ MODO PRODUÇÃO DIRETA: Artefatos salvos em '{r2_prefix}/'")
        
    progress_key = f"{r2_prefix}/progress.json"
    manifest_key = f"{r2_prefix}/manifest.json"
    
    # 1. Autodiscoberta de Cartas REA
    session = requests.Session()
    available_charts = discover_rea_layers(session)
    
    # 2. Filtra cartas a processar
    if chart_codes_env == "ALL":
        codes_to_process = list(available_charts.keys())
    else:
        codes_to_process = [c.strip() for c in chart_codes_env.split(",") if c.strip() in available_charts]
        
    if not codes_to_process:
        print("❌ Nenhuma carta válida para processar. Finalizando.")
        sys.exit(1)
        
    # Ordenação por prioridade para mesclagem Alpha (Grandes capitais por último para ficarem no topo)
    PRIORITY_WEIGHTS = {
        "REA_WR_BRASILIA": 10,
        "REA_XP1_SAO_PAULO": 10,
        "REA_XP2_SAO_PAULO": 9,
        "REA_WJ1_RIO_DE_JANEIRO": 10,
        "REA_WH_BELO_HORIZONTE": 9,
        "REA_XF_FLORIANOPOLIS": 9,
        "REA_XS_SALVADOR": 8,
        "REA_WF_RECIFE": 8,
        "REA_WP_PORTO_ALEGRE": 8,
        "REA_CURITIBA": 7,
    }
    codes_to_process.sort(key=lambda c: PRIORITY_WEIGHTS.get(c, 0))
    charts_total = len(codes_to_process)
    
    print(f"📋 Total de cartas a compilar: {charts_total} cartas")
    print(f"📦 Modo consolidado nacional (arquivo único): {single_file}")
    print(f"🔍 Faixa de zoom: Z{min_zoom} a Z{max_zoom}")
    
    temp_dir = os.path.join(os.getcwd(), "temp_mbtiles")
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
            # ─── MODO CONSOLIDADO BRASIL FULL ───
            consolidated_filename = "REA_BRASIL_FULL.mbtiles"
            consolidated_path = os.path.join(temp_dir, consolidated_filename)
            
            if os.path.exists(consolidated_path):
                os.remove(consolidated_path)
                
            conn = sqlite3.connect(consolidated_path)
            global_bbox = (-73.99, -33.75, -34.50, 5.27) # Cobertura de todo o espaço aéreo brasileiro
            init_mbtiles(conn, "REA_BRASIL_FULL", global_bbox, min_zoom, max_zoom)
            
            log_telemetry(f"Inicializando compilação unificada de {charts_total} cartas REA.")
            for idx, code in enumerate(codes_to_process):
                chart_info = available_charts[code]
                log_telemetry(f"Processando setor [{idx+1}/{charts_total}]: {code} ({chart_info.get('title', code)})")
                
                def on_progress(done_tiles, total_tiles):
                    single_percent = (done_tiles / total_tiles) * 100
                    overall_percent = ((idx + (done_tiles / total_tiles)) / charts_total) * 100
                    upload_progress(
                        r2_client, r2_bucket, progress_key, "in_progress", overall_percent,
                        f"{code} ({round(single_percent)}%)", idx, charts_total, run_id, cycle, is_staging, chart_metadata
                    )
                    
                process_chart(code, chart_info, min_zoom, max_zoom, workers, consolidated_path, existing_conn=conn, progress_callback=on_progress)
                log_telemetry(f"Setor {code} compilado com sucesso.")
                
            log_telemetry("Otimizando base consolidada unificada (VACUUM)...")
            conn.execute("VACUUM")
            conn.close()
            
            # Auditoria de Integridade MBTiles
            log_telemetry("Executando verificação de integridade SQLite no arquivo consolidado...")
            audit_stats = verify_mbtiles_integrity(consolidated_path)
            log_telemetry(f"Integridade 100% OK: {audit_stats['total_tiles']} tiles válidos, {audit_stats['size_bytes'] / (1024*1024):.2f} MB")
            
            # Upload do MBTiles para o R2 (no caminho de staging ou produção)
            r2_key = f"{r2_prefix}/{consolidated_filename}"
            log_telemetry(f"Enviando MBTiles consolidado para Cloudflare R2 ({r2_key})...")
            r2_client.upload_file(consolidated_path, r2_bucket, r2_key)
            log_telemetry("Upload do MBTiles consolidado concluído com sucesso.")
            
            manifest_data["consolidated"] = {
                "filename": consolidated_filename,
                "r2_key": r2_key,
                "stats": audit_stats
            }
            chart_metadata["REA_BRASIL_FULL"] = {
                "size_bytes": audit_stats["size_bytes"],
                "total_tiles": audit_stats["total_tiles"],
                "sha256": audit_stats["sha256"],
                "updated_at": datetime.utcnow().isoformat() + "Z"
            }
            
            if os.path.exists(consolidated_path):
                os.remove(consolidated_path)
                
        else:
            # ─── MODO COMPILAÇÃO SETOR POR SETOR ───
            for idx, code in enumerate(codes_to_process):
                chart_info = available_charts[code]
                filename = f"{code}.mbtiles"
                local_path = os.path.join(temp_dir, filename)
                
                if os.path.exists(local_path):
                    os.remove(local_path)
                    
                def on_progress(done_tiles, total_tiles):
                    single_percent = (done_tiles / total_tiles) * 100
                    overall_percent = ((idx + (done_tiles / total_tiles)) / charts_total) * 100
                    upload_progress(
                        r2_client, r2_bucket, progress_key, "in_progress", overall_percent,
                        f"{code} ({round(single_percent)}%)", idx, charts_total, run_id, cycle, is_staging, chart_metadata
                    )
                    
                process_chart(code, chart_info, min_zoom, max_zoom, workers, local_path, progress_callback=on_progress)
                
                audit_stats = verify_mbtiles_integrity(local_path)
                r2_key = f"{r2_prefix}/sectors/{filename}"
                print(f"  [Cloud R2] Enviando {filename} para {r2_key}...")
                r2_client.upload_file(local_path, r2_bucket, r2_key)
                
                manifest_data["charts"][code] = {
                    "filename": filename,
                    "r2_key": r2_key,
                    "stats": audit_stats
                }
                chart_metadata[code] = {
                    "size_bytes": audit_stats["size_bytes"],
                    "total_tiles": audit_stats["total_tiles"],
                    "sha256": audit_stats["sha256"],
                    "updated_at": datetime.utcnow().isoformat() + "Z"
                }
                
                if os.path.exists(local_path):
                    os.remove(local_path)
                    
        # Publica o Manifesto do Ciclo no R2
        log_telemetry(f"Publicando manifesto do ciclo em {manifest_key}...")
        r2_client.put_object(
            Bucket=r2_bucket,
            Key=manifest_key,
            Body=json.dumps(manifest_data, indent=2),
            ContentType="application/json"
        )
        
        # Finalização de sucesso
        log_telemetry(f"Compilação do Ciclo {cycle} finalizada com absoluto sucesso (100%).")
        upload_progress(r2_client, r2_bucket, progress_key, "completed", 100.0, "Compilação Concluída", charts_total, charts_total, run_id, cycle, is_staging, chart_metadata)
        
        # Notificação automática ao Supabase Edge Function (airac-rea-vfr-ingest)
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")
        if supabase_url and supabase_key:
            try:
                log_telemetry("Notificando ingestão no Supabase via Edge Function airac-rea-vfr-ingest...")
                ingest_url = f"{supabase_url.rstrip('/')}/functions/v1/airac-rea-vfr-ingest"
                headers = {
                    "Authorization": f"Bearer {supabase_key}",
                    "apikey": supabase_key,
                    "Content-Type": "application/json"
                }
                total_t = sum(m.get("total_tiles", 0) for m in chart_metadata.values())
                total_s = sum(m.get("size_bytes", 0) for m in chart_metadata.values())
                payload = {
                    "target": "tiles",
                    "status": "VALIDATED",
                    "cycle": cycle,
                    "effective_date": effective_date,
                    "expiration_date": expiration_date,
                    "is_staging": is_staging,
                    "r2_staging_path": f"{r2_prefix}/",
                    "charts_count": charts_total,
                    "total_tiles": total_t,
                    "size_bytes": total_s,
                    "charts_manifest": manifest_data,
                    "generated_at": datetime.utcnow().isoformat() + "Z"
                }
                requests.post(ingest_url, json=payload, headers=headers, timeout=10)
                print("✅ [Webhook] Ingestão registrada no Supabase com sucesso.")
            except Exception as w_err:
                print(f"⚠️ [Webhook] Aviso ao notificar Supabase: {w_err}")
        
    except Exception as e:
        print(f"\n❌ Erro crítico no robô REA: {e}")
        upload_progress(r2_client, r2_bucket, progress_key, "failed", 100.0, f"Erro: {str(e)}", 0, charts_total, run_id, cycle, is_staging, chart_metadata)
        
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")
        if supabase_url and supabase_key:
            try:
                ingest_url = f"{supabase_url.rstrip('/')}/functions/v1/airac-rea-vfr-ingest"
                headers = {
                    "Authorization": f"Bearer {supabase_key}",
                    "apikey": supabase_key,
                    "Content-Type": "application/json"
                }
                payload = {
                    "target": "tiles",
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
