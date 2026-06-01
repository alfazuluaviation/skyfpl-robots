#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🛰️ SkyFPL - Robô Processador de Rotas Especiais de Helicópteros (REH)

Este robô realiza o download paralelo de tiles de cartas de Corredores Visuais (REH)
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
import re
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

# ─── Mapeamento Geográfico Estático (Fallback) ────────────────────────────────
REH_BBOXES_STATIC = {
    "CCV_REH_WJ2_RIO_DE_JANEIRO": (-43.93321557582174, -23.167597156715217, -42.97107698737478, -22.52656143551633),
    "CCV_REH_WJ1_CABO_FRIO":       (-43.02902427325738, -23.211679759243978, -41.88701812169220, -22.391639786996812),
    "CCV_REH_WJ3_RIO_DE_JANEIRO": (-43.42943564045275, -23.04769591573280,  -43.06925616418908, -22.805966789642408),
    "CCV_REH_XP2_SAO_PAULO_1":    (-47.03059822493503, -23.839578618082196, -46.30765850004488, -23.308783000203956),
    "CCV_REH_XP2_SAO_PAULO_2":    (-46.783682787281315, -23.682140083214190, -46.58873787009814, -23.475774297793063),
    "CCV_REH_XP1_SAO_JOSE_DOS_CAMPOS": (-46.361243933728545, -23.591320884112307, -45.80977108389402, -22.902794908159528),
    "CCV_REH_XP1_SOROCABA":        (-47.660085387129485, -23.789128916852338, -46.93448027871988, -23.255110743711647),
    "CCV_REH_XP2_CAMPINAS":        (-47.27598025312896, -23.438636532995120, -46.72148193047676, -22.750404422034570),
    "CCV_REH_WH_BELO_HORIZONTE":   (-44.28333333333332, -20.200000000000003, -43.63330986833334, -19.383303851666668),
    "REH_BR_COMPLETO": (-47.66008538712948, -23.839578618082196, -39.91660592239991, -19.383303851666668),
}

REH_LAYERS_STATIC = {
    "CCV_REH_WJ2_RIO_DE_JANEIRO":      "ICA:CCV_REH_WJ2_RIO_DE_JANEIRO",
    "CCV_REH_WJ1_CABO_FRIO":           "ICA:CCV_REH_WJ1_CABO_FRIO",
    "CCV_REH_WJ3_RIO_DE_JANEIRO":      "ICA:CCV_REH_WJ3_RIO_DE_JANEIRO",
    "CCV_REH_XP2_SAO_PAULO_1":         "ICA:CCV_REH_XP2_SAO_PAULO_1",
    "CCV_REH_XP2_SAO_PAULO_2":         "ICA:CCV_REH_XP2_SAO_PAULO_2",
    "CCV_REH_XP1_SAO_JOSE_DOS_CAMPOS": "ICA:CCV_REH_XP1_SAO_JOSE_DOS_CAMPOS",
    "CCV_REH_XP1_SOROCABA":            "ICA:CCV_REH_XP1_SOROCABA",
    "CCV_REH_XP2_CAMPINAS":            "ICA:CCV_REH_XP2_CAMPINAS",
    "CCV_REH_WH_BELO_HORIZONTE":       "ICA:CCV_REH_WH_BELO_HORIZONTE",
    "REH_BR_COMPLETO":                 "ICA:CCV_REH_WH_BELO_HORIZONTE",
}

def fetch_reh_layers_from_capabilities():
    url = f"{WMS_URL}?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetCapabilities"
    try:
        print("📡 Consultando GetCapabilities do GeoServer DECEA para autodescoberta dinâmica de REH...")
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            print("⚠️ Falha ao obter GetCapabilities, usando bboxes estáticos.")
            return None
        
        xml_content = r.text
        layer_blocks = xml_content.split("<Layer")
        
        dynamic_bboxes = {}
        dynamic_layers = {}
        
        for block in layer_blocks:
            name_match = re.search(r"<Name>([^<]+)</Name>", block)
            if not name_match:
                continue
            name = name_match.group(1).strip()
            
            # Filtro inteligente e futuro-seguro:
            # 1. Deve possuir o padrão de nomenclatura de rotas REH do DECEA
            # 2. Exclui explicitamente camadas de vetor/polígono vazias (iniciadas por CV_)
            is_reh = False
            if (name.startswith("CCV_REH_") or name.startswith("CCV_RHE_") or name.startswith("REH_")) and not name.startswith("CV_"):
                is_reh = True
                
            if is_reh:
                bbox_match = re.search(r"LatLonBoundingBox[^>]+minx=\"([^\"]+)\"[^>]+miny=\"([^\"]+)\"[^>]+maxx=\"([^\"]+)\"[^>]+maxy=\"([^\"]+)\"", block)
                if bbox_match:
                    minx = float(bbox_match.group(1))
                    miny = float(bbox_match.group(2))
                    maxx = float(bbox_match.group(3))
                    maxy = float(bbox_match.group(4))
                    
                    dynamic_bboxes[name] = (minx, miny, maxx, maxy)
                    dynamic_layers[name] = f"ICA:{name}"
                    print(f"  [Autodescoberta] Camada REH encontrada: {name} -> BBOX: ({minx}, {miny}, {maxx}, {maxy})")
        
        if dynamic_bboxes:
            # Calcula o envelope global dinamicamente contendo todas as BBoxes encontradas
            all_minx = min(b[0] for b in dynamic_bboxes.values())
            all_miny = min(b[1] for b in dynamic_bboxes.values())
            all_maxx = max(b[2] for b in dynamic_bboxes.values())
            all_maxy = max(b[3] for b in dynamic_bboxes.values())
            dynamic_bboxes["REH_BR_COMPLETO"] = (all_minx, all_miny, all_maxx, all_maxy)
            dynamic_layers["REH_BR_COMPLETO"] = "ICA:REH_VITORIA"
            print(f"🌍 BBOX Envelope Global Unificado: {dynamic_bboxes['REH_BR_COMPLETO']}")
            return dynamic_bboxes, dynamic_layers
    except Exception as e:
        print(f"⚠️ Erro ao analisar XML de Capabilities: {e}")
    return None

# Inicialização Dinâmica dos dados de BBOX
dynamic_data = fetch_reh_layers_from_capabilities()
if dynamic_data:
    REH_BBOXES, REH_LAYERS = dynamic_data
else:
    print("⚠️ Usando Bboxes estáticos de fallback.")
    REH_BBOXES, REH_LAYERS = REH_BBOXES_STATIC, REH_LAYERS_STATIC

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
    """Valida se a imagem retornada tem pixels visíveis reais (não apenas transparente/branca).
    
    Estratégia RGBA-aware:
    1. Descarta tiles com 100% de pixels totalmente transparentes (alpha=0).
    2. Entre os pixels visíveis (alpha > 0), descarta se 100% têm exatamente a mesma cor.
    3. Descarta se os pixels visíveis são 100% branco puro (255,255,255) — fundo de papel vazio.
    """
    if not raw_data:
        return False, None
    
    try:
        img = Image.open(BytesIO(raw_data)).convert("RGBA")
        pixels = list(img.getdata())
        total = len(pixels)
        
        # 1. Filtra apenas pixels com alguma opacidade
        visible = [(r, g, b) for r, g, b, a in pixels if a > 0]
        
        if not visible:
            return False, None  # 100% transparente
        
        # 2. Verifica se todos os pixels visíveis têm exatamente a mesma cor
        unique_colors = set(visible)
        if len(unique_colors) == 1:
            return False, None  # Cor sólida única (sem dados reais de carta)
        
        # 3. Verifica se 100% dos pixels visíveis são branco puro (papel vazio)
        white = sum(1 for r, g, b in visible if r == 255 and g == 255 and b == 255)
        if white / len(visible) >= 1.0:
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
        ("description", f"Corredores Visuais REH - {name}"),
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
    layer = REH_LAYERS.get(chart_code, "ICA:CCV_REH_WH_BELO_HORIZONTE")
    
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
            Key="reh_progress.json",
            Body=json.dumps(progress_data, indent=2),
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
    except Exception as e:
        print(f"  [WARN] Falha ao enviar progresso para o Cloudflare R2: {e}")

# ─── Função Principal ─────────────────────────────────────────────────────────

def main():
    print("🚀 Iniciando Motor de Processamento de Rotas Especiais REH...")
    
    # Parâmetros vindos do Ambiente (GitHub Dispatch / Supabase Trigger)
    chart_codes_env = os.environ.get("CHART_CODES", "ALL").strip()
    min_zoom = int(os.environ.get("MIN_ZOOM", DEFAULT_MIN_ZOOM))
    max_zoom = int(os.environ.get("MAX_ZOOM", DEFAULT_MAX_ZOOM))
    single_file = os.environ.get("SINGLE_FILE", "true").lower() == "true" # For REH, single file makes sense too
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
        codes_to_process = [k for k in REH_BBOXES.keys() if k not in ("REH_BR_COMPLETO",)]
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
        progress_obj = r2_client.get_object(Bucket=r2_bucket, Key="reh_progress.json")
        existing_progress = json.loads(progress_obj["Body"].read().decode("utf-8"))
        chart_metadata = existing_progress.get("metadata", {})
    except Exception:
        print("ℹ️ Nenhum arquivo 'telemetry.json' encontrado no R2 para REH. Criando novo.")
        
    # Inicializa progresso no R2
    upload_progress(r2_client, r2_bucket, "in_progress", 0.0, codes_to_process[0], 0, charts_total, run_id, chart_metadata)
    
    try:
        if single_file:
            # ─── MODO COMPILAÇÃO GLOBAL (MÚLTIPLOS CORREDORES MESCLADOS EM UM SÓ) ───
            consolidated_filename = "REH_BRASIL_FULL.mbtiles"
            consolidated_path = os.path.join(temp_dir, consolidated_filename)
            
            # Remove base antiga se existir localmente
            if os.path.exists(consolidated_path):
                os.remove(consolidated_path)
                
            conn = sqlite3.connect(consolidated_path)
            # Usa o envelope global de todas as regiões REH
            global_bbox = REH_BBOXES["REH_BR_COMPLETO"]
            init_mbtiles(conn, "REH_BRASIL_FULL", global_bbox, min_zoom, max_zoom)
            
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
            r2_key = f"reh/{consolidated_filename}"
            r2_client.upload_file(consolidated_path, r2_bucket, r2_key)
            
            # Atualiza o tamanho e timestamp do consolidador na telemetria
            chart_metadata["REH_BRASIL_FULL"] = {
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
                r2_key = f"reh/{filename}"
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
        print("\n🏆 Processamento REH completo com absoluto sucesso!")
        upload_progress(r2_client, r2_bucket, "completed", 100.0, "Sucesso", charts_total, charts_total, run_id, chart_metadata)
        
    except Exception as e:
        print(f"\n❌ Erro crítico no pipeline do Robô REH: {e}")
        upload_progress(r2_client, r2_bucket, "error", 100.0, f"Erro: {str(e)}", 0, charts_total, run_id, chart_metadata)
        sys.exit(1)

if __name__ == "__main__":
    main()

