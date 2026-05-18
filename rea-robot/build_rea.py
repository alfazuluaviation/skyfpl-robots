#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🛰️ SkyFPL - Robô Processador de Rotas Especiais de Aeronaves (REA)

Este robô realiza o download paralelo de tiles de cartas de Corredores Visuais (REA)
diretamente do GeoServer do DECEA via WMS (EPSG:3857), aplica mesclagem Alpha Composite 
nas emendas geográficas e empacota tudo em arquivos SQLite MBTiles otimizados no Cloudflare R2.
"""

import os
import sys
import json
import math
import time
import sqlite3
import threading
from io import BytesIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import boto3
from PIL import Image

# ─── Configurações Gerais ─────────────────────────────────────────────────────
WMS_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms"
TILE_SIZE = 256
DEFAULT_MIN_ZOOM = 8
DEFAULT_MAX_ZOOM = 11

# Lock global para operações simultâneas na base de dados SQLite
mbtiles_lock = threading.Lock()

# ─── Mapeamento Geográfico de Bounding Boxes (BBOX) ───────────────────────────
# Coordenadas georreferenciadas exatas (minLon, minLat, maxLon, maxLat)
REA_BBOXES = {
    "REA_PI_PARINTINS": (-57.17892446305804, -3.118413186024014, -56.32451510440087, -2.283406035846115),
    "REA_WA_TABATINGA": (-70.2115198871506, -4.420586518190401, -69.51330499240112, -4.266989630450815),
    "REA_WB_BELEM": (-48.82177737929154, -1.693386948900882, -47.955102819150476, -1.042439191742151),
    "REA_WF_RECIFE": (-35.23155814695412, -8.51538651602695, -34.80669013105232, -7.50286010324279),
    "REA_WG_CAMPO_GRANDE": (-55.377291746928144, -21.128230122389333, -53.93345969828556, -19.807863983320757),
    "REA_WJ1_RIO_DE_JANEIRO": (-44.556691662692465, -23.329820810628654, -42.04068300098855, -21.94787322294413),
    "REA_WK_PORTO_SEGURO": (-39.282187494725704, -16.748601267718865, -39.04423202870435, -16.486386331980988),
    "REA_WN_MANAUS": (-60.556005109242584, -3.290610545737803, -59.627426679574704, -2.898912158554002),
    "REA_WP1_PORTO_ALEGRE": (-51.83101901434175, -30.159251995886024, -50.44852068017018, -28.830199581695016),
    "REA_WR_BRASILIA": (-48.262217551640596, -16.15468519808023, -47.573952928779065, -15.544972654224932),
    "REA_WS_SAO_LUIS": (-44.51474319299905, -2.783590866473686, -44.033514792502785, -2.390193004507077),
    "REA_WX_SANTAREM": (-55.018361197457516, -2.652643120693034, -54.57097014096276, -2.286073970837863),
    "REA_WY_CUIABA": (-56.492650022829075, -15.958285360659367, -55.79190666379166, -15.333716526974923),
    "REA_WZ_FORTALEZA": (-38.90350051594101, -4.102946968280818, -38.17038688134388, -3.520099565490647),
    "REA_XF_FLORIANOPOLIS": (-49.62580328519761, -28.234827176140044, -48.30615151983976, -26.600169455920888),
    "REA_XK_MACAPA": (-51.31140822943274, -0.122611419056813, -50.78147720995653, 0.226827556060604),
    "REA_XN_ANAPOLIS": (-49.53202749340708, -17.005472316966593, -48.68344824621946, -15.821715128922966),
    "REA_XO_LONDRINA": (-52.640976852766634, -24.03089663564089, -50.34752750547414, -22.63696928576837),
    "REA_XP1_SAO_PAULO": (-47.75951609495089, -24.22047062906321, -44.44370618502077, -22.490139016563624),
    "REA_XQ_RIBEIRAO_PRETO": (-48.05704502999893, -21.259264659154027, -47.58562063703641, -20.981168332885204),
    "REA_XR_VITORIA": (-40.53010562097639, -20.531262325817032, -40.06207670436895, -19.830466378995226),
    "REA_XS_SALVADOR": (-38.93313560033532, -13.40061270924209, -37.984373860136145, -12.56647368211773),
    "REA_XT_NATAL": (-35.73758867365102, -6.324589306193678, -35.06459754286324, -5.509268477935918),
    "REA_BR_COMPLETO": (-70.2115198871506, -30.159251995886024, -34.80669013105232, 0.226827556060604),
}

# Camadas correspondentes no GeoServer
REA_LAYERS = {
    "REA_PI_PARINTINS": "ICA:CV_REA_PI_PARINTINS",
    "REA_WA_TABATINGA": "ICA:CV_REA_WA_TABATINGA",
    "REA_WB_BELEM": "ICA:CV_REA_WB_BELEM",
    "REA_WF_RECIFE": "ICA:CV_REA_WF_RECIFE",
    "REA_WG_CAMPO_GRANDE": "ICA:CV_REA_WG_CAMPO_GRANDE",
    "REA_WJ1_RIO_DE_JANEIRO": "ICA:CV_REA_WJ1_RIO_DE_JANEIRO",
    "REA_WK_PORTO_SEGURO": "ICA:CV_REA_WK_PORTO_SEGURO",
    "REA_WN_MANAUS": "ICA:CV_REA_WN_MANAUS",
    "REA_WP1_PORTO_ALEGRE": "ICA:CV_REA_WP1_PORTO_ALEGRE",
    "REA_WR_BRASILIA": "ICA:CV_REA_WR_BRASILIA",
    "REA_WS_SAO_LUIS": "ICA:CV_REA_WS_SAO_LUIS",
    "REA_WX_SANTAREM": "ICA:CV_REA_WX_SANTAREM",
    "REA_WY_CUIABA": "ICA:CV_REA_WY_CUIABA",
    "REA_WZ_FORTALEZA": "ICA:CV_REA_WZ_FORTALEZA",
    "REA_XF_FLORIANOPOLIS": "ICA:CV_REA_XF_FLORIANOPOLIS",
    "REA_XK_MACAPA": "ICA:CV_REA_XK_MACAPA",
    "REA_XN_ANAPOLIS": "ICA:CV_REA_XN_ANAPOLIS",
    "REA_XO_LONDRINA": "ICA:CV_REA_XO_LONDRINA",
    "REA_XP1_SAO_PAULO": "ICA:CV_REA_XP1_SAO_PAULO",
    "REA_XQ_RIBEIRAO_PRETO": "ICA:CV_REA_XQ_RIBEIRAO_PRETO",
    "REA_XR_VITORIA": "ICA:CV_REA_XR_VITORIA",
    "REA_XS_SALVADOR": "ICA:CV_REA_XS_SALVADOR",
    "REA_XT_NATAL": "ICA:CV_REA_XT_NATAL",
    "REA_BR_COMPLETO": "ICA:CV_REA_BR_COMPLETO",
}

# ─── Utilitários Geográficos e de Conversão de Coordenadas ────────────────────

def latLngToTile(lat: float, lng: float, zoom: int) -> tuple:
    n = 2.0 ** zoom
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return x, y

def tile_bbox_mercator(x: int, y: int, z: int) -> tuple:
    """Retorna (minX, minY, maxX, maxY) em metros Mercator (EPSG:3857)."""
    world_size = 20037508.342789244 * 2
    res = world_size / (2 ** z)
    minx = x * res - 20037508.342789244
    maxx = (x + 1) * res - 20037508.342789244
    maxy = 20037508.342789244 - y * res
    miny = 20037508.342789244 - (y + 1) * res
    return (minx, miny, maxx, maxy)

# ─── Validação de Tiles em Branco/Transparentes (1.7KB Threshold) ─────────────

def validate_tile_data(raw_data: bytes | None) -> tuple:
    """Valida se a imagem retornada é real ou apenas uma área transparente/vazia (<1700 bytes)."""
    if not raw_data:
        return False, None
    
    # Se o tamanho da imagem for muito pequeno (< 1.7KB), é uma imagem vazia/transparente
    if len(raw_data) < 1700:
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
            r = session.get(WMS_URL, params=params, timeout=45)
            if r.status_code == 200 and r.headers.get("Content-Type", "").startswith("image"):
                return r.content
            elif r.status_code == 429:
                time.sleep(1)
        except Exception:
            if attempt == 4:
                print(f"  [ERR] Falha no download z={z} x={x} y={y}")
            time.sleep(0.5)
            
    return None

# ─── Mecanismo de Inicialização do Banco SQLite MBTiles ───────────────────────

def init_mbtiles(conn: sqlite3.Connection, name: str, bbox: tuple, min_zoom: int, max_zoom: int):
    conn.execute("CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_idx ON tiles (zoom_level, tile_column, tile_row)")
    
    metadata = [
        ("name", name),
        ("type", "overlay"),
        ("version", "1.0.0"),
        ("description", f"Corredores Visuais REA - {name}"),
        ("format", "png"),
        ("bounds", f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"),
        ("minzoom", str(min_zoom)),
        ("maxzoom", str(max_zoom)),
    ]
    
    conn.executemany("INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)", metadata)
    conn.commit()

# ─── Processamento de uma Carta Específica ────────────────────────────────────

def process_chart(
    chart_code: str,
    bbox: tuple,
    min_zoom: int,
    max_zoom: int,
    workers: int,
    output_path: str,
    existing_conn: sqlite3.Connection | None = None,
    progress_callback=None
):
    print(f"\n🌍 [{chart_code}] Iniciando processamento da área: {bbox}...")
    
    # Estabelece ou reaproveita a conexão com o banco
    if existing_conn:
        conn = existing_conn
    else:
        conn = sqlite3.connect(output_path)
        init_mbtiles(conn, chart_code, bbox, min_zoom, max_zoom)
        
    session = requests.Session()
    
    # ─── Calcula a Grade de Tiles Necessários ──────────────────────────────────
    tiles_to_fetch = []
    for z in range(min_zoom, max_zoom + 1):
        # Convertemos os cantos lat/lng do BBOX nos limites X/Y do tile
        x_min, y_max_tile = latLngToTile(bbox[1], bbox[0], z)
        x_max, y_min_tile = latLngToTile(bbox[3], bbox[2], z)
        
        # Garante a ordenação correta das grades
        x_start = min(x_min, x_max)
        x_end = max(x_min, x_max)
        y_start = min(y_min_tile, y_max_tile)
        y_end = max(y_min_tile, y_max_tile)
        
        # Margem de segurança de 1 tile
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
    layer = REA_LAYERS.get(chart_code, "ICA:CV_REA_BR_COMPLETO")
    
    # Inicia downloads paralelos
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
                    # MBTiles usa coordenadas TMS (Y invertido)
                    tms_y = (2 ** z) - 1 - y
                    
                    with mbtiles_lock:
                        # Em modo consolidated/single_file, podemos ter colisões de tiles nas divisas.
                        # Fazemos a mesclagem Alpha Composite usando a biblioteca PIL.
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
                            except Exception as e:
                                print(f"  [WARN] Falha na mesclagem Alpha do tile z={z} x={x} y={y}: {e}")
                                
                        conn.execute(
                            "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                            (z, x, tms_y, tile_data)
                        )
            except Exception as e:
                print(f"  [WARN] Erro ao salvar tile z={z} x={x} y={y}: {e}")
                
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
        
    print(f"  [{chart_code}] Completo com sucesso! {done}/{total_tiles} processados.")

# ─── Telemetria Real-Time via R2 Progress JSON ────────────────────────────────

def upload_progress(
    r2_client,
    bucket: str,
    status: str,
    percent: float,
    current_chart: str,
    charts_done: int,
    charts_total: int,
    run_id: str,
    metadata: dict
):
    progress_data = {
        "status": status,
        "percent": percent,
        "current_chart": current_chart,
        "charts_done": charts_done,
        "charts_total": charts_total,
        "run_id": run_id,
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "metadata": metadata
    }
    
    try:
        r2_client.put_object(
            Bucket=bucket,
            Key="rea_progress.json",
            Body=json.dumps(progress_data, indent=2),
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
    except Exception as e:
        print(f"  [WARN] Falha ao enviar progresso para o Cloudflare R2: {e}")

# ─── Função Principal ─────────────────────────────────────────────────────────

def main():
    print("🚀 Iniciando Motor de Processamento de Rotas Especiais REA...")
    
    # Parâmetros vindos do Ambiente (GitHub Dispatch / Supabase Trigger)
    chart_codes_env = os.environ.get("CHART_CODES", "ALL").strip()
    min_zoom = int(os.environ.get("MIN_ZOOM", DEFAULT_MIN_ZOOM))
    max_zoom = int(os.environ.get("MAX_ZOOM", DEFAULT_MAX_ZOOM))
    single_file = os.environ.get("SINGLE_FILE", "false").lower() == "true"
    workers = int(os.environ.get("WORKERS", "6"))
    run_id = os.environ.get("RUN_ID", "local_dev")
    
    # Credenciais do Cloudflare R2
    r2_endpoint = os.environ.get("CLOUDFLARE_R2_ENDPOINT") or os.environ.get("R2_ENDPOINT", "")
    r2_access_key = os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID") or os.environ.get("R2_ACCESS_KEY_ID", "")
    r2_secret_key = os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY", "")
    r2_bucket = os.environ.get("CLOUDFLARE_R2_BUCKET") or os.environ.get("R2_BUCKET", "skyfpl-charts")
    
    # Valida R2
    if not r2_endpoint or not r2_access_key or not r2_secret_key:
        print("❌ Chaves do Cloudflare R2 ausentes! Interrompendo execução.")
        sys.exit(1)
        
    r2_client = boto3.client(
        "s3",
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key
    )
    
    # Filtra cartas a processar
    if chart_codes_env == "ALL":
        # Ignora o consolidador global da lista primária para evitar looping
        codes_to_process = [k for k in REA_BBOXES.keys() if k != "REA_BR_COMPLETO"]
    else:
        codes_to_process = [c.strip() for c in chart_codes_env.split(",") if c.strip() in REA_BBOXES]
        
    if not codes_to_process:
        print("❌ Nenhuma carta válida para processar. Finalizando.")
        sys.exit(1)
        
    charts_total = len(codes_to_process)
    print(f"📦 Cartas selecionadas para processamento: {codes_to_process}")
    print(f"🔍 Modo de arquivo único (Consolidado): {single_file}")
    
    # Cria pasta temporária
    temp_dir = os.path.join(os.getcwd(), "temp_mbtiles")
    os.makedirs(temp_dir, exist_ok=True)
    
    # Baixa ou inicializa o metadados de progresso existentes no R2
    chart_metadata = {}
    try:
        progress_obj = r2_client.get_object(Bucket=r2_bucket, Key="rea_progress.json")
        existing_progress = json.loads(progress_obj["Body"].read().decode("utf-8"))
        chart_metadata = existing_progress.get("metadata", {})
    except Exception:
        print("ℹ️ Nenhum arquivo 'rea_progress.json' encontrado no R2. Criando novo.")
        
    # Inicializa progresso no R2
    upload_progress(r2_client, r2_bucket, "in_progress", 0.0, codes_to_process[0], 0, charts_total, run_id, chart_metadata)
    
    try:
        if single_file:
            # ─── MODO COMPILAÇÃO GLOBAL (MÚLTIPLOS CORREDORES MESCLADOS EM UM SÓ) ───
            consolidated_filename = "REA_BRASIL_FULL.mbtiles"
            consolidated_path = os.path.join(temp_dir, consolidated_filename)
            
            # Remove base antiga se existir localmente
            if os.path.exists(consolidated_path):
                os.remove(consolidated_path)
                
            conn = sqlite3.connect(consolidated_path)
            # Usa o BBOX completo do Brasil para o consolidador
            global_bbox = REA_BBOXES["REA_BR_COMPLETO"]
            init_mbtiles(conn, "REA_BRASIL_FULL", global_bbox, min_zoom, max_zoom)
            
            for idx, code in enumerate(codes_to_process):
                bbox = REA_BBOXES[code]
                
                # Callback de progresso interno
                def on_progress(done_tiles, total_tiles):
                    single_percent = (done_tiles / total_tiles) * 100
                    overall_percent = ((idx + (done_tiles / total_tiles)) / charts_total) * 100
                    upload_progress(
                        r2_client, r2_bucket, "in_progress", overall_percent,
                        f"{code} ({round(single_percent)}%)", idx, charts_total, run_id, chart_metadata
                    )
                    
                process_chart(code, bbox, min_zoom, max_zoom, workers, consolidated_path, existing_conn=conn, progress_callback=on_progress)
                
            # Otimização final do arquivo mesclado
            print("  [Brasil Consolidated] Otimizando base unificada (VACUUM)...")
            conn.execute("VACUUM")
            conn.close()
            
            # Envia arquivo completo para o R2
            print("  [Cloud R2] Enviando arquivo consolidado para o Storage...")
            file_size = os.path.getsize(consolidated_path)
            r2_key = f"rea/{consolidated_filename}"
            r2_client.upload_file(consolidated_path, r2_bucket, r2_key)
            
            # Atualiza o tamanho e timestamp do consolidador na telemetria
            chart_metadata["REA_BRASIL_FULL"] = {
                "size_bytes": file_size,
                "updated_at": datetime.utcnow().isoformat() + "Z"
            }
            
            # Remove arquivo temporário local
            if os.path.exists(consolidated_path):
                os.remove(consolidated_path)
                
        else:
            # ─── MODO COMPILAÇÃO INDIVIDUAL (UMA BASE POR CORREDOR) ───
            for idx, code in enumerate(codes_to_process):
                bbox = REA_BBOXES[code]
                filename = f"{code}.mbtiles"
                local_path = os.path.join(temp_dir, filename)
                
                if os.path.exists(local_path):
                    os.remove(local_path)
                    
                def on_progress(done_tiles, total_tiles):
                    single_percent = (done_tiles / total_tiles) * 100
                    overall_percent = ((idx + (done_tiles / total_tiles)) / charts_total) * 100
                    upload_progress(
                        r2_client, r2_bucket, "in_progress", overall_percent,
                        f"{code} ({round(single_percent)}%)", idx, charts_total, run_id, chart_metadata
                    )
                    
                process_chart(code, bbox, min_zoom, max_zoom, workers, local_path, progress_callback=on_progress)
                
                # Upload do arquivo gerado para o R2
                print(f"  [Cloud R2] Enviando {filename} para o Storage...")
                file_size = os.path.getsize(local_path)
                r2_key = f"rea/{filename}"
                r2_client.upload_file(local_path, r2_bucket, r2_key)
                
                # Registra metadados específicos
                chart_metadata[code] = {
                    "size_bytes": file_size,
                    "updated_at": datetime.utcnow().isoformat() + "Z"
                }
                
                # Limpa local
                if os.path.exists(local_path):
                    os.remove(local_path)
                    
        # Finalização de sucesso
        print("\n🏆 Processamento REA completo com absoluto sucesso!")
        upload_progress(r2_client, r2_bucket, "completed", 100.0, "Sucesso", charts_total, charts_total, run_id, chart_metadata)
        
    except Exception as e:
        print(f"\n❌ Erro crítico no pipeline do Robô REA: {e}")
        upload_progress(r2_client, r2_bucket, "failed", 100.0, f"Erro: {str(e)}", 0, charts_total, run_id, chart_metadata)
        sys.exit(1)

if __name__ == "__main__":
    main()
