"""
build_enrc.py — SkyFPL ENRC Chart Processor (LOW & HIGH)
==========================================================
Baixa tiles WMS do DECEA GeoServer para as cartas ENRC (L e H) selecionadas,
empacota cada uma como um arquivo MBTiles individual ou Brasil Full e faz upload para o Cloudflare R2.

Uso:
  LAYER_TYPE=LOW CHART_CODES=ALL python scripts/build_enrc.py
  LAYER_TYPE=HIGH CHART_CODES=H1,H2 python scripts/build_enrc.py

O arquivo enrcl_progress.json ou enrch_progress.json é atualizado a cada carta concluída.
"""

import os
import sys
import json
import math
import time
import sqlite3
import tempfile
import requests
import boto3
import threading
from io import BytesIO
from datetime import datetime, timezone
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed

# Lock global para operações atômicas no MBTiles (Single File Mode)
mbtiles_lock = threading.Lock()

# ─── Configuração ───────────────────────────────────────────────────────────

WMS_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms"
TILE_SIZE = 256

LAYER_TYPE = os.environ.get("LAYER_TYPE", "LOW").upper() # "LOW" ou "HIGH"

try:
    MIN_ZOOM = int(os.environ.get("MIN_ZOOM", 5))
    MAX_ZOOM = int(os.environ.get("MAX_ZOOM", 12))
except:
    MIN_ZOOM, MAX_ZOOM = 5, 12

# ─── Thresholds de Qualidade ──────────────────────────────────────────────────
ENRC_EMPTY_THRESHOLD = 1700      # Tiles abaixo disso são considerados irrelevantes/vazios
ENRC_EXISTING_THRESHOLD = 4000   # Se o tile existente for > 4KB, não sobrescrever com um leve
MARGIN_DEG = 0.2                # Margem de sobreposição em graus para evitar frestas (seams)

SINGLE_FILE = os.environ.get("SINGLE_FILE", "false").lower() == "true"
CHART_CODES_ENV = os.environ.get("CHART_CODES", "ALL").strip()

R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]

# ─── Mapa de Bounding Boxes (L1-L9 e H1-H9 compartilham a mesma grade) ───────
ENRC_BBOXES = {
    "1": (-58.0, -36.0, -44.0, -24.0), # Sul (Porto Alegre)
    "2": (-51.0, -24.0, -38.0, -12.0), # Sudeste/Centro (SP/RJ/BSB)
    "3": (-46.0, -13.0, -33.0, 1.0),   # Nordeste Litoral (REC/SSA/FOR)
    "4": (-46.0, 0.0, -23.0, 13.0),    # Nordeste Oceânico
    "5": (-63.0, -27.0, -49.0, -13.0), # Centro-Oeste Sul (CGR/CGB)
    "6": (-62.0, -14.0, -46.0, 2.0),   # Centro/Norte Interior
    "7": (-61.0, 0.0, -45.0, 11.0),    # Norte Oriental (BEL/MCP)
    "8": (-74.0, 0.0, -60.0, 11.0),    # Norte Central (BVB/MAO)
    "9": (-76.0, -16.0, -60.0, -3.0),  # Norte Ocidental (RBR/PVH)
}

# ─── Utilitários de Tiles ─────────────────────────────────────────────────────

def latLngToTile(lat, lng, zoom):
    n = 2.0 ** zoom
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return x, y

def tile_bbox_mercator(x: int, y: int, z: int):
    world_size = 20037508.342789244 * 2
    res = world_size / (2 ** z)
    minx = x * res - 20037508.342789244
    maxx = (x + 1) * res - 20037508.342789244
    maxy = 20037508.342789244 - y * res
    miny = 20037508.342789244 - (y + 1) * res
    return (minx, miny, maxx, maxy)

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
    
    for attempt in range(10):
        try:
            r = session.get(WMS_URL, params=params, timeout=45)
            if r.status_code == 200 and r.headers.get("Content-Type", "").startswith("image"):
                return r.content
            
            # 🛡️ RESILIÊNCIA WMS: Backoff Exponencial para 429 ou timeouts
            wait_time = pow(2, attempt)
            if r.status_code == 429:
                print(f"  [WMS_429] Rate limit (429) em Z{z} X{x} Y{y}. Tentativa {attempt+1}/10. Aguardando {wait_time}s...")
                time.sleep(wait_time)
            elif r.status_code >= 500:
                print(f"  [WMS_{r.status_code}] Erro Servidor em Z{z} X{x} Y{y}. Tentativa {attempt+1}/10. Aguardando {wait_time}s...")
                time.sleep(wait_time)
            
        except Exception as e:
            wait_time = pow(2, attempt)
            if attempt == 9:
                print(f"  [ERR] Falha fatal no tile z={z} x={x} y={y} após 10 tentativas: {e}")
            time.sleep(wait_time)
            
    return None

def validate_tile_data(tile_bytes: bytes) -> tuple[bool, bytes]:
    if not tile_bytes or len(tile_bytes) < 100:
        return False, b""
    return True, tile_bytes

def create_mbtiles(chart_code: str, bbox: tuple, output_path: str, layer: str, existing_conn=None, progress_callback=None):
    minLon, minLat, maxLon, maxLat = bbox
    
    if existing_conn:
        conn = existing_conn
    else:
        conn = sqlite3.connect(output_path)
        conn.execute("PRAGMA journal_mode=DELETE")

    conn.execute("CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT)")
    conn.execute("""CREATE TABLE IF NOT EXISTS tiles (
        zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB,
        PRIMARY KEY (zoom_level, tile_column, tile_row)
    )""")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_idx ON tiles (zoom_level, tile_column, tile_row)")

    if not existing_conn:
        name = f"ENRC {LAYER_TYPE} {chart_code}"
        conn.executemany("INSERT OR REPLACE INTO metadata VALUES (?, ?)", [
            ("name", name),
            ("type", "overlay"),
            ("version", "1.0"),
            ("description", f"Enroute Chart {LAYER_TYPE} {chart_code} — DECEA ICA"),
            ("format", "png"),
            ("minzoom", str(MIN_ZOOM)),
            ("maxzoom", str(MAX_ZOOM)),
            ("bounds", f"{minLon},{minLat},{maxLon},{maxLat}"),
            # 🔑 PADRÃO SkyFPL: tile_scheme sempre declarado para evitar ambiguidade no App
            ("scheme", "tms"),
        ])
    
    conn.commit()

    session = requests.Session()
    session.headers.update({"User-Agent": "SkyFPL-Bot/1.0"})

    tiles_to_fetch = []
    total_tiles = 0
    for z in range(MIN_ZOOM, MAX_ZOOM + 1):
        # 🟢 APLICAÇÃO DE MARGEM (Antifresta):
        # Solicitamos um pouco mais de área ao WMS para garantir sobreposição segura entre cartas.
        x0, y_max_lat = latLngToTile(maxLat + MARGIN_DEG, minLon - MARGIN_DEG, z)
        x1, y_min_lat = latLngToTile(minLat - MARGIN_DEG, maxLon + MARGIN_DEG, z)
        y0 = y_max_lat
        y1 = y_min_lat
        
        for x in range(x0, x1 + 1):
            for y in range(y0, y1 + 1):
                total_tiles += 1
                tiles_to_fetch.append((x, y, z))

    workers_env = os.environ.get("WORKERS", "").strip()
    if workers_env.isdigit() and int(workers_env) > 0:
        workers = int(workers_env)
    else:
        # 🛡️ CAP DE SEGURANÇA: 5 workers é o padrão ouro para resiliência no DECEA
        workers = 5 if SINGLE_FILE else 8
        
    print(f"  [{LAYER_TYPE}-{chart_code}] Total de tiles: {total_tiles} (Z{MIN_ZOOM}-Z{MAX_ZOOM}) - Baixando com {workers} Workers...")

    done = 0
    failed_count = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(download_wms_tile, t[0], t[1], t[2], session, layer): t 
            for t in tiles_to_fetch
        }
        
        for future in as_completed(futures):
            x, y, z = futures[future]
            try:
                raw_data = future.result()
                if raw_data:
                    # 🛡️ VALIDAÇÃO DE CAMADA (V24.70):
                    # O DECEA GeoServer retorna imagens de ~1.6KB para áreas sem dados.
                    # Vamos verificar se o tile tem conteúdo real antes de salvar.
                    try:
                        tile_img = Image.open(BytesIO(raw_data)).convert("RGBA")
                        # getbbox() retorna None se a imagem for totalmente transparente
                        if not tile_img.getbbox():
                            # Se for uma área marginal (<1700), apenas pula. Se for >1700 e transparente, alerta.
                            if len(raw_data) > ENRC_EMPTY_THRESHOLD:
                                print(f"  [INFO] Tile transparente descartado (Z{z} X{x} Y{y}) - {len(raw_data)} bytes")
                            done += 1
                            continue
                        
                        tile_data = raw_data
                    except Exception as e:
                        print(f"  [WARN] Erro ao processar imagem (z={z} x={x} y={y}): {e}")
                        failed_count += 1
                        done += 1
                        continue

                    # MBTiles usa TMS (Y invertido)
                    tms_y = (2 ** z) - 1 - y

                    with mbtiles_lock:
                        if existing_conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (z, x, tms_y))
                            row = cursor.fetchone()
                            
                            if row:
                                # 🛡️ PROTEÇÃO DE DADOS (Anti-Buraco):
                                # Se o tile novo for leve e já tivermos um tile substancial,
                                # assumimos que o novo é apenas "borda transparente" e preservamos o anterior.
                                if len(tile_data) < ENRC_EMPTY_THRESHOLD and len(row[0]) > ENRC_EXISTING_THRESHOLD:
                                    done += 1
                                    continue

                                try:
                                    bg_img = Image.open(BytesIO(row[0])).convert("RGBA")
                                    # tile_img já está aberto do check de transparência acima
                                    bg_img.alpha_composite(tile_img)
                                    out_io = BytesIO()
                                    bg_img.save(out_io, format="PNG")
                                    tile_data = out_io.getvalue()
                                except Exception as e:
                                    print(f"  [WARN] Falha na mesclagem (z={z} x={x} y={y}): {e}")

                        conn.execute(
                            "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                            (z, x, tms_y, tile_data)
                        )
                else:
                    failed_count += 1
                    # 🔑 TELEMETRIA DE FALHA GEOGRÁFICA
                    # Converte X,Y em Lat/Lon para que o usuário identifique o buraco no mapa
                    n = 2.0 ** z
                    lon_deg = x / n * 360.0 - 180.0
                    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
                    lat_deg = math.degrees(lat_rad)
                    
                    if progress_callback:
                        progress_callback(done, total_tiles, failed_in_round=1, failed_coord={
                            "z": z, "x": x, "y": y, 
                            "lat": round(lat_deg, 4), "lon": round(lon_deg, 4)
                        })
                
                done += 1
                if done % 100 == 0:
                    with mbtiles_lock:
                        conn.commit()
                    print(f"  [{chart_code}] Progresso: {done}/{total_tiles} tiles (Falhas: {failed_count})")
                    if progress_callback:
                        progress_callback(done, total_tiles, failed_in_round=0)
            except Exception as e:
                failed_count += 1
                print(f"  [WARN] Erro ao processar resultado do tile z={z} x={x} y={y}: {e}")

    conn.commit()
    
    if not existing_conn:
        conn.execute("VACUUM")
        conn.close()

def audit_mbtiles(path: str) -> tuple[bool, str]:
    """
    🛡️ AUDITORIA DE PRÉ-UPLOAD (V24.70)
    Garante que o arquivo MBTiles gerado é válido para o SkyFPL.
    Retorna (sucesso, mensagem).
    """
    fname = os.path.basename(path)
    print(f"  🔍 [AUDIT] Iniciando auditoria em: {fname}...")
    try:
        conn = sqlite3.connect(path)
        
        # 1. Check de Integridade Física
        check = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if check != "ok":
            msg = f"Banco de dados corrompido: {check}"
            print(f"  🛑 [AUDIT_FAIL] {msg}")
            return False, msg
            
        # 2. Validação de Metadados Críticos
        metadata = dict(conn.execute("SELECT name, value FROM metadata").fetchall())
        
        # Check de Scheme (O coração do desalinhamento)
        scheme = metadata.get("scheme")
        if scheme != "tms":
            msg = f"Esquema de coordenadas inválido ({scheme}). Esperado: tms"
            print(f"  🛑 [AUDIT_FAIL] {msg}")
            return False, msg
            
        # Check de Formato
        fmt = metadata.get("format")
        if fmt != "png":
            msg = f"Formato de imagem inválido ({fmt}). Esperado: png"
            print(f"  🛑 [AUDIT_FAIL] {msg}")
            return False, msg

        # 3. Validação de Conteúdo (Garante que não é um arquivo vazio)
        tile_count = conn.execute("SELECT count(*) FROM tiles").fetchone()[0]
        if tile_count == 0:
            msg = "O banco de dados não contém tiles!"
            print(f"  🛑 [AUDIT_FAIL] {msg}")
            return False, msg
            
        success_msg = f"{tile_count} tiles validados (TMS/PNG)"
        print(f"  ✅ [AUDIT_PASS] {success_msg}")
        conn.close()
        return True, success_msg
        
    except Exception as e:
        err_msg = f"Erro fatal durante a auditoria: {str(e)}"
        print(f"  🔥 [AUDIT_ERR] {err_msg}")
        return False, err_msg

# ─── Upload R2 ────────────────────────────────────────────────────────────────

def upload_to_r2(local_path: str, r2_key: str):
    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="wnam",
    )
    print(f"  Fazendo upload para R2: {r2_key}...")
    s3.upload_file(local_path, R2_BUCKET, r2_key)
    size_bytes = os.path.getsize(local_path)
    print(f"  Upload concluído: {r2_key} ({size_bytes / 1024 / 1024:.2f} MB)")
    return size_bytes

def upload_progress_json(s3_client, charts_done: list, charts_total: list, current: str = "", metadata: dict = None):
    percent = int(len(charts_done) / len(charts_total) * 100) if charts_total else 100
    if metadata is None:
        metadata = {}
    
    data = {
        "status": "in_progress" if len(charts_done) < len(charts_total) else "completed",
        "percent": percent,
        "current_chart": current,
        "charts_done": charts_done,
        "charts_total": charts_total,
        "run_id": os.environ.get("RUN_ID"),
        "metadata": metadata,
        # 🔑 TELEMETRIA DE INTEGRIDADE: Reporta tiles perdidos para o Admin Dashboard.
        "failed_tiles_count": metadata.get("failed_total", 0),
        "failed_coords": metadata.get("failed_coords", []),
        "tile_scheme": "tms",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    # Adiciona sub-progresso se disponível
    if metadata and "sub_percent" in metadata:
        data["sub_percent"] = metadata["sub_percent"]
        # Calcula um percentual global mais preciso: (charts_concluídas + progresso_da_atual) / total
        if charts_total:
            base_percent = (len(charts_done) / len(charts_total)) * 100
            current_contribution = (1 / len(charts_total)) * metadata["sub_percent"]
            data["percent"] = int(base_percent + current_contribution)

    filename = "enrcl_progress.json" if LAYER_TYPE == "LOW" else "enrch_progress.json"
    s3_client.put_object(
        Bucket=R2_BUCKET,
        Key=filename,
        Body=json.dumps(data, ensure_ascii=False),
        ContentType="application/json",
        CacheControl="no-cache, no-store",
    )

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    l_prefix = "L" if LAYER_TYPE == "LOW" else "H"
    all_codes = [f"{l_prefix}{k}" for k in ENRC_BBOXES.keys()]

    if CHART_CODES_ENV.upper() == "ALL":
        codes_to_process = all_codes
    else:
        codes_to_process = [c.strip().upper() for c in CHART_CODES_ENV.split(",") if c.strip().upper() in all_codes]

    if not codes_to_process:
        print(f"[ERRO] Nenhum código {LAYER_TYPE} válido encontrado em CHART_CODES:", CHART_CODES_ENV)
        sys.exit(1)

    print(f"[ENRC {LAYER_TYPE} Processor] Processando {len(codes_to_process)} carta(s): {', '.join(codes_to_process)}")

    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="wnam",
    )

    charts_done = []
    metadata = {
        "global_config": {
            "minzoom": MIN_ZOOM,
            "maxzoom": MAX_ZOOM,
            "single_file": SINGLE_FILE
        }
    }
    
    single_conn = None
    single_output_path = ""
    global_bbox = [180.0, 90.0, -180.0, -90.0]

    with tempfile.TemporaryDirectory() as tmpdir:
        if SINGLE_FILE:
            filename = f"ENRCL_BRASIL_FULL.mbtiles" if LAYER_TYPE == "LOW" else f"ENRCH_BRASIL_FULL.mbtiles"
            single_output_path = os.path.join(tmpdir, filename)
            single_conn = sqlite3.connect(single_output_path)
            single_conn.execute("PRAGMA journal_mode=DELETE")
            print(f"[ENRC Processor] 🛡️ Modo ARQUIVO ÚNICO ATIVADO: Criando {single_output_path}")

        for i, code in enumerate(codes_to_process):
            print(f"\n[{i+1}/{len(codes_to_process)}] Processando: {code}")
            upload_progress_json(s3, charts_done, codes_to_process, current=code, metadata=metadata)

            region_key = code[1:] # "L1" -> "1"
            bbox = ENRC_BBOXES[region_key]
            layer = f"ICA:ENRC_{code}"
            
            global_bbox[0] = min(global_bbox[0], bbox[0])
            global_bbox[1] = min(global_bbox[1], bbox[1])
            global_bbox[2] = max(global_bbox[2], bbox[2])
            global_bbox[3] = max(global_bbox[3], bbox[3])

            output_path = single_output_path if SINGLE_FILE else os.path.join(tmpdir, f"{code}.mbtiles")

            try:
                def on_tile_progress(done, total, failed_in_round=0, failed_coord=None):
                    sub_p = (done / total) * 100
                    metadata["sub_percent"] = sub_p
                    
                    if "failed_total" not in metadata: metadata["failed_total"] = 0
                    if "failed_coords" not in metadata: metadata["failed_coords"] = []
                    
                    metadata["failed_total"] += failed_in_round
                    if failed_coord:
                        metadata["failed_coords"].append(failed_coord)
                        # Mantém apenas as últimas 500 falhas para não estourar o JSON
                        if len(metadata["failed_coords"]) > 500:
                            metadata["failed_coords"].pop(0)

                    upload_progress_json(s3, charts_done, codes_to_process, current=code, metadata=metadata)

                create_mbtiles(code, bbox, output_path, layer, existing_conn=single_conn, progress_callback=on_tile_progress)
                
                if not SINGLE_FILE:
                    is_ok, audit_msg = audit_mbtiles(output_path)
                    if is_ok:
                        prefix = "enrc"
                        size_bytes = upload_to_r2(output_path, f"{prefix}/{code}.mbtiles")
                        charts_done.append(code)
                        metadata[code] = {
                            "size_bytes": size_bytes,
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                            "audit": "PASS"
                        }
                    else:
                        print(f"  🛑 [FATAL_AUDIT] {code} falhou na auditoria e NÃO será enviado!")
                        metadata[code] = {
                            "status": "audit_failed",
                            "error": audit_msg,
                            "updated_at": datetime.now(timezone.utc).isoformat()
                        }
                        # Reporta falha ao Dashboard IMEDIATAMENTE
                        upload_progress_json(s3, charts_done, codes_to_process, current=code, metadata=metadata)

                    if os.path.exists(output_path):
                        os.remove(output_path)
                else:
                    charts_done.append(code)
                    upload_progress_json(s3, charts_done, codes_to_process, metadata=metadata)
            except Exception as e:
                print(f"  [ERRO] Falha ao processar {code}: {e}")
                continue

        if SINGLE_FILE and single_conn:
            print(f"\n[ENRC Processor] 🏁 Finalizando Arquivo Único Brasileiro ({LAYER_TYPE})...")
            single_conn.executemany("INSERT OR REPLACE INTO metadata VALUES (?, ?)", [
                ("name", f"SkyFPL ENRC {LAYER_TYPE} Brasil Full"),
                ("type", "overlay"),
                ("version", "1.0"),
                ("description", f"Enroute Chart {LAYER_TYPE} — Brasil Completo"),
                ("format", "png"),
                ("minzoom", str(MIN_ZOOM)),
                ("maxzoom", str(MAX_ZOOM)),
                ("bounds", f"{global_bbox[0]},{global_bbox[1]},{global_bbox[2]},{global_bbox[3]}"),
                # 🔑 PADRÃO SkyFPL: tile_scheme sempre declarado para eliminar ambiguidade no App nativo
                ("scheme", "tms"),
            ])
            single_conn.commit()
            single_conn.execute("VACUUM")
            single_conn.close()
            
            file_key = f"enrc/ENRCL_BRASIL_FULL.mbtiles" if LAYER_TYPE == "LOW" else f"enrc/ENRCH_BRASIL_FULL.mbtiles"
            
            is_ok, audit_msg = audit_mbtiles(single_output_path)
            full_meta_key = f"ENRCL_BRASIL_FULL" if LAYER_TYPE == "LOW" else f"ENRCH_BRASIL_FULL"
            
            if is_ok:
                size_bytes = upload_to_r2(single_output_path, file_key)
                metadata[full_meta_key] = {
                    "size_bytes": size_bytes,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "minzoom": MIN_ZOOM,
                    "maxzoom": MAX_ZOOM,
                    "audit": "PASS"
                }
            else:
                print(f"  🛑 [FATAL_AUDIT] O arquivo FULL falhou na auditoria e NÃO será enviado!")
                metadata[full_meta_key] = {
                    "status": "audit_failed",
                    "error": audit_msg,
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
                # Garante que o dashboard saiba da tragédia
                upload_progress_json(s3, charts_done, codes_to_process, metadata=metadata)

    upload_progress_json(s3, charts_done, codes_to_process, metadata=metadata)
    print(f"\n[ENRC Processor] ✅ Concluído! {len(charts_done)}/{len(codes_to_process)} cartas processadas.")

if __name__ == "__main__":
    main()
