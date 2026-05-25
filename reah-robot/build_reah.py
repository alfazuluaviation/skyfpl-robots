#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🛰️ SkyFPL - Robô Processador de Rotas Especiais de Helicópteros (REAH)

Este robô realiza o download paralelo de tiles de cartas de Corredores Visuais (REAH)
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
TILE_SIZE = 512
DEFAULT_MIN_ZOOM = 8
DEFAULT_MAX_ZOOM = 11

# Lock global para operações simultâneas na base de dados SQLite
mbtiles_lock = threading.Lock()

# ─── Mapeamento Geográfico de Bounding Boxes (BBOX) ───────────────────────────
REH_BBOXES = {
    "REH_WJ2_RIO_DE_JANEIRO": (-44.813333333333325, -24.00166666666666, -41.76017583793333, -21.81760169266666),
    "REH_XP_SAO_PAULO": (-47.89661794556251, -24.503348570316604, -44.395672115362515, -22.285199031516605),
    "REH_XR_VITORIA": (-40.66666666666665, -20.583333333333336, -39.91648482946665, -19.799779604533324),
    "REH_BR_COMPLETO": (-70.2833333333323, -30.750250584388976, -34.999969916666664, 0.3000192533333317),
}

# Camadas correspondentes no GeoServer
REH_LAYERS = {
    "REH_WJ2_RIO_DE_JANEIRO": "ICA:CV_REH_WJ2_RIO_DE_JANEIRO",
    "REH_XP_SAO_PAULO": "ICA:CV_REH_XP_SAO_PAULO",
    "REH_XR_VITORIA": "ICA:CV_REH_XR_VITORIA",
    "REH_BR_COMPLETO": "ICA:CV_REH_BR_COMPLETO",
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
    """Valida se a imagem retornada é real ou apenas uma área transparente/vazia/sólida/branca."""
    if not raw_data:
        return False, None
    
    try:
        img = Image.open(BytesIO(raw_data))
        img_rgb = img.convert("RGB")
        
        # 1. Se a imagem tiver apenas 1 única cor sólida em toda a sua extensão,
        # ela é 100% sólida (cinza, branca, etc.) e deve ser descartada do banco.
        colors = img_rgb.getcolors(maxcolors=2)
        if colors and len(colors) == 1:
            return False, None
            
        # 2. Se a imagem tiver 93% ou mais de pixels branco puro (255, 255, 255),
        # ela representa uma margem branca de papel vazia e deve ser descartada do banco.
        total_pixels = img_rgb.width * img_rgb.height
        all_colors = img_rgb.getcolors(maxcolors=total_pixels)
        if all_colors:
            white_pixels = 0
            for count, rgb in all_colors:
                if rgb == (255, 255, 255):
                    white_pixels = count
                    break
            if (white_pixels / total_pixels) >= 0.93:
                return False, None
                
    except Exception as e:
        print(f"  [WARN] Erro ao validar cores do tile: {e}")
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
        "TRANSPARENT": "TRUE", # 🛡️ Ativa transparência nativa de 32 bits (preserva o relevo e remove fundo de borda)
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
        ("description", f"Corredores Visuais REAH - {name}"),
        ("format", "png"),
        ("bounds", f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"),
        ("minzoom", str(min_zoom)),
        ("maxzoom", str(max_zoom)),
        ("scheme", "tms"), # 🛰️ Declara padrão TMS para inversão perfeita do eixo Y no Android
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
    layer = REH_LAYERS.get(chart_code, "ICA:CV_REH_BR_COMPLETO")
    
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
            Key="reah_progress.json",
            Body=json.dumps(progress_data, indent=2),
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
    except Exception as e:
        print(f"  [WARN] Falha ao enviar progresso para o Cloudflare R2: {e}")

# ─── Função Principal ─────────────────────────────────────────────────────────

def main():
    print("🚀 Iniciando Motor de Processamento de Rotas Especiais REAH...")
    
    # Parâmetros vindos do Ambiente (GitHub Dispatch / Supabase Trigger)
    chart_codes_env = os.environ.get("CHART_CODES", "ALL").strip()
    min_zoom = int(os.environ.get("MIN_ZOOM", DEFAULT_MIN_ZOOM))
    max_zoom = int(os.environ.get("MAX_ZOOM", DEFAULT_MAX_ZOOM))
    single_file = os.environ.get("SINGLE_FILE", "true").lower() == "true" # For REAH, single file makes sense too
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
        codes_to_process = [k for k in REH_BBOXES.keys() if k != "REH_BR_COMPLETO"]
    else:
        codes_to_process = [c.strip() for c in chart_codes_env.split(",") if c.strip() in REH_BBOXES]
        
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
        progress_obj = r2_client.get_object(Bucket=r2_bucket, Key="reah_progress.json")
        existing_progress = json.loads(progress_obj["Body"].read().decode("utf-8"))
        chart_metadata = existing_progress.get("metadata", {})
    except Exception:
        print("ℹ️ Nenhum arquivo 'telemetry.json' encontrado no R2 para REAH. Criando novo.")
        
    # Inicializa progresso no R2
    upload_progress(r2_client, r2_bucket, "in_progress", 0.0, codes_to_process[0], 0, charts_total, run_id, chart_metadata)
    
    try:
        if single_file:
            # ─── MODO COMPILAÇÃO GLOBAL (MÚLTIPLOS CORREDORES MESCLADOS EM UM SÓ) ───
            consolidated_filename = "REAH_BRASIL_FULL.mbtiles"
            consolidated_path = os.path.join(temp_dir, consolidated_filename)
            
            # Remove base antiga se existir localmente
            if os.path.exists(consolidated_path):
                os.remove(consolidated_path)
                
            conn = sqlite3.connect(consolidated_path)
            # Usa o BBOX completo do Brasil para o consolidador
            global_bbox = REH_BBOXES["REH_BR_COMPLETO"]
            init_mbtiles(conn, "REAH_BRASIL_FULL", global_bbox, min_zoom, max_zoom)
            
            for idx, code in enumerate(codes_to_process):
                bbox = REH_BBOXES[code]
                
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
            r2_key = f"reah/{consolidated_filename}"
            r2_client.upload_file(consolidated_path, r2_bucket, r2_key)
            
            # Atualiza o tamanho e timestamp do consolidador na telemetria
            chart_metadata["REAH_BRASIL_FULL"] = {
                "size_bytes": file_size,
                "updated_at": datetime.utcnow().isoformat() + "Z"
            }
            
            # Remove arquivo temporário local
            if os.path.exists(consolidated_path):
                os.remove(consolidated_path)
                
        else:
            # ─── MODO COMPILAÇÃO INDIVIDUAL (UMA BASE POR CORREDOR) ───
            for idx, code in enumerate(codes_to_process):
                bbox = REH_BBOXES[code]
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
                r2_key = f"reah/{filename}"
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
        print("\n🏆 Processamento REAH completo com absoluto sucesso!")
        upload_progress(r2_client, r2_bucket, "completed", 100.0, "Sucesso", charts_total, charts_total, run_id, chart_metadata)
        
    except Exception as e:
        print(f"\n❌ Erro crítico no pipeline do Robô REAH: {e}")
        upload_progress(r2_client, r2_bucket, "error", 100.0, f"Erro: {str(e)}", 0, charts_total, run_id, chart_metadata)
        sys.exit(1)

if __name__ == "__main__":
    main()
