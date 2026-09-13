"""
build_wac.py — SkyFPL WAC Chart Processor
==========================================
Baixa tiles WMS do DECEA GeoServer para as cartas WAC selecionadas,
empacota cada uma como um arquivo MBTiles individual e faz upload para o Cloudflare R2.

Uso:
  CHART_CODES=ALL python scripts/build_wac.py
  CHART_CODES=WAC3140,WAC3263 python scripts/build_wac.py

Cada carta é salva no R2 como: wac/WAC{codigo}.mbtiles
O arquivo wac_progress.json é atualizado a cada carta concluída.
"""

import os
import sys
import json
import math
import time
import struct
import sqlite3
import hashlib
import tempfile
import threading
import requests
import boto3
from io import BytesIO
from datetime import datetime, timezone
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed

# Lock global para operações atômicas no MBTiles (Single File Mode)
mbtiles_lock = threading.Lock()

# ─── Configuração ───────────────────────────────────────────────────────────

# ─── Configuração Dinâmica (Injetada pelo Dashboard) ──────────────────────
WMS_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms"
TILE_SIZE = 256

try:
    MIN_ZOOM = int(os.environ.get("MIN_ZOOM", 5))
    MAX_ZOOM = int(os.environ.get("MAX_ZOOM", 12))
except:
    MIN_ZOOM, MAX_ZOOM = 5, 12

SINGLE_FILE = os.environ.get("SINGLE_FILE", "false").lower() == "true"
CHART_CODES_ENV = os.environ.get("CHART_CODES", "ALL").strip()

R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]

# ─── Mapa de Bounding Boxes por Carta WAC ────────────────────────────────────
# Formato: (minLon, minLat, maxLon, maxLat) — cobertura da folha WAC em graus
# Cada folha WAC cobre aproximadamente 4° lat x 6° lon

WAC_BBOXES = {
    "WAC2825": (-56.0, 4.0,  -50.0, 8.0),   # Cabo Orange
    "WAC2826": (-62.0, 4.0,  -56.0, 8.0),   # Monte Roraima
    "WAC2827": (-68.0, 4.0,  -62.0, 8.0),   # Serra Paracaima
    "WAC2892": (-70.0, 0.0,  -64.0, 4.0),   # Pico da Neblina
    "WAC2893": (-64.0, 0.0,  -58.0, 4.0),   # Boa Vista
    "WAC2894": (-58.0, 0.0,  -52.0, 4.0),   # Tumucumaque
    "WAC2895": (-52.0, 0.0,  -46.0, 4.0),   # Macapá
    "WAC2944": (-41.0, -4.0, -35.0, 0.0),   # Fortaleza
    "WAC2945": (-47.0, -4.0, -41.0, 0.0),   # São Luís
    "WAC2946": (-53.0, -4.0, -47.0, 0.0),   # Belém
    "WAC2947": (-59.0, -4.0, -53.0, 0.0),   # Santarém
    "WAC2948": (-65.0, -4.0, -59.0, 0.0),   # Manaus
    "WAC2949": (-71.0, -4.0, -65.0, 0.0),   # São Gabriel da Cachoeira
    "WAC3012": (-76.0, -8.0, -70.0, -4.0),  # Cruzeiro do Sul
    "WAC3013": (-70.0, -8.0, -64.0, -4.0),  # Tabatinga
    "WAC3014": (-64.0, -8.0, -58.0, -4.0),  # Humaitá
    "WAC3015": (-58.0, -8.0, -52.0, -4.0),  # Itaituba
    "WAC3016": (-52.0, -8.0, -46.0, -4.0),  # Imperatriz
    "WAC3017": (-46.0, -8.0, -40.0, -4.0),  # Teresina
    "WAC3018": (-40.0, -8.0, -34.0, -4.0),  # Natal
    "WAC3019": (-36.0, -8.3, -30.0, -4.0),  # Fernando de Noronha
    "WAC3066": (-39.0, -12.0, -33.0, -8.0), # Recife
    "WAC3067": (-45.0, -12.0, -39.0, -8.0), # Petrolina
    "WAC3068": (-51.0, -12.0, -45.0, -8.0), # Porto Nacional
    "WAC3069": (-57.0, -12.0, -51.0, -8.0), # Cachimbo
    "WAC3070": (-63.0, -12.0, -57.0, -8.0), # Ji-Paraná
    "WAC3071": (-69.0, -12.0, -63.0, -8.0), # Porto Velho
    "WAC3072": (-75.0, -12.0, -69.0, -8.0), # Tarauacá
    "WAC3137": (-67.0, -16.0, -61.0, -12.0), # Príncipe da Beira
    "WAC3138": (-61.0, -16.0, -55.0, -12.0), # Cuiabá
    "WAC3139": (-55.0, -16.0, -49.0, -12.0), # Aragarças
    "WAC3140": (-49.0, -16.0, -43.0, -12.0), # Brasília
    "WAC3141": (-43.0, -16.0, -37.0, -12.0), # Salvador
    "WAC3189": (-44.0, -20.0, -38.0, -16.0), # Belo Horizonte
    "WAC3190": (-50.0, -20.0, -44.0, -16.0), # Goiânia
    "WAC3191": (-56.0, -20.0, -50.0, -16.0), # Rondonópolis
    "WAC3192": (-62.0, -20.0, -56.0, -16.0), # Corumbá
    "WAC3260": (-62.0, -24.0, -56.0, -20.0), # Bela Vista
    "WAC3261": (-56.0, -24.0, -50.0, -20.0), # Campo Grande
    "WAC3262": (-50.0, -24.0, -44.0, -20.0), # São Paulo
    "WAC3263": (-44.0, -24.0, -38.0, -20.0), # Rio de Janeiro
    "WAC3313": (-51.0, -28.0, -45.0, -24.0), # Curitiba
    "WAC3314": (-57.0, -28.0, -51.0, -24.0), # Foz do Iguaçu
    "WAC3383": (-60.0, -32.0, -54.0, -28.0), # Uruguaiana
    "WAC3384": (-54.0, -32.0, -48.0, -28.0), # Porto Alegre
    "WAC3434": (-59.0, -36.0, -52.0, -32.0), # Rio da Prata
}

WAC_LAYERS = {
    "WAC2825": "ICA:WAC_2825_CABO_ORANGE",
    "WAC2826": "ICA:WAC_2826_MONTE_RORAIMA",
    "WAC2827": "ICA:WAC_2827_SERRA_PACARAIMA",
    "WAC2892": "ICA:WAC_2892_PICO_DA_NEBLINA",
    "WAC2893": "ICA:WAC_2893_BOA_VISTA",
    "WAC2894": "ICA:WAC_2894_TUMUCUMAQUE",
    "WAC2895": "ICA:WAC_2895_MACAPA",
    "WAC2944": "ICA:WAC_2944_FORTALEZA",
    "WAC2945": "ICA:WAC_2945_SAO_LUIS",
    "WAC2946": "ICA:WAC_2946_BELEM",
    "WAC2947": "ICA:WAC_2947_SANTAREM",
    "WAC2948": "ICA:WAC_2948_MANAUS",
    "WAC2949": "ICA:WAC_2949_SAO_GABRIEL_DA_CACHOEIRA",
    "WAC3012": "ICA:WAC_3012_CRUZEIRO_DO_SUL",
    "WAC3013": "ICA:WAC_3013_TABATINGA",
    "WAC3014": "ICA:WAC_3014_HUMAITA",
    "WAC3015": "ICA:WAC_3015_ITAITUBA",
    "WAC3016": "ICA:WAC_3016_IMPERATRIZ",
    "WAC3017": "ICA:WAC_3017_TERESINA",
    "WAC3018": "ICA:WAC_3018_NATAL",
    "WAC3019": "ICA:WAC_3019_FERNANDO_DE_NORONHA",
    "WAC3066": "ICA:WAC_3066_RECIFE",
    "WAC3067": "ICA:WAC_3067_PETROLINA",
    "WAC3068": "ICA:WAC_3068_PORTO_NACIONAL",
    "WAC3069": "ICA:WAC_3069_CACHIMBO",
    "WAC3070": "ICA:WAC_3070_JI_PARANA",
    "WAC3071": "ICA:WAC_3071_PORTO_VELHO",
    "WAC3072": "ICA:WAC_3072_TARAUACA",
    "WAC3137": "ICA:WAC_3137_PRINCIPE_DA_BEIRA",
    "WAC3138": "ICA:WAC_3138_CUIABA",
    "WAC3139": "ICA:WAC_3139_ARAGARCAS",
    "WAC3140": "ICA:WAC_3140_BRASILIA",
    "WAC3141": "ICA:WAC_3141_SALVADOR",
    "WAC3189": "ICA:WAC_3189_BELO_HORIZONTE",
    "WAC3190": "ICA:WAC_3190_GOIANIA",
    "WAC3191": "ICA:WAC_3191_RONDONOPOLIS",
    "WAC3192": "ICA:WAC_3192_CORUMBA",
    "WAC3260": "ICA:WAC_3260_BELA_VISTA",
    "WAC3261": "ICA:WAC_3261_CAMPO_GRANDE",
    "WAC3262": "ICA:WAC_3262_SAO_PAULO",
    "WAC3263": "ICA:WAC_3263_RIO_DE_JANEIRO",
    "WAC3313": "ICA:WAC_3313_CURITIBA",
    "WAC3314": "ICA:WAC_3314_FOZ_DO_IGUACU",
    "WAC3383": "ICA:WAC_3383_URUGUAIANA",
    "WAC3384": "ICA:WAC_3384_PORTO_ALEGRE",
    "WAC3434": "ICA:WAC_3434_RIO_DA_PRATA",
}

# ─── Utilitários de Tiles ─────────────────────────────────────────────────────

def latLngToTile(lat, lng, zoom):
    n = 2.0 ** zoom
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return x, y

def tile_bbox_mercator(x: int, y: int, z: int):
    """Retorna (minX, minY, maxX, maxY) em metros Mercator (EPSG:3857)."""
    world_size = 20037508.342789244 * 2
    res = world_size / (2 ** z)
    minx = x * res - 20037508.342789244
    maxx = (x + 1) * res - 20037508.342789244
    maxy = 20037508.342789244 - y * res
    miny = 20037508.342789244 - (y + 1) * res
    return (minx, miny, maxx, maxy)

# ─── Download de tile WMS ─────────────────────────────────────────────────────

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
    
    # Sistema de Retries Robustos (V12.4)
    for attempt in range(5):
        try:
            r = session.get(WMS_URL, params=params, timeout=45) # Aumentado para 45s por ser WMS pesado
            if r.status_code == 200 and r.headers.get("Content-Type", "").startswith("image"):
                return r.content
            elif r.status_code == 429:
                time.sleep(1)
        except Exception as e:
            if attempt == 4:
                print(f"  [ERR] Falha fatal no tile z={z} x={x} y={y} após 5 tentativas.")
            time.sleep(0.5)
            
    return None

# ─── Criação de MBTiles ───────────────────────────────────────────────────────

def validate_tile_data(tile_bytes: bytes) -> tuple[bool, bytes]:
    """
    Mantém 100% de paridade com o Robô do Mapa Base: Salva dados brutos PNG.
    Apenas descarta se o arquivo for inválido ou minúsculo.
    """
    if not tile_bytes or len(tile_bytes) < 100:
        return False, b""
    return True, tile_bytes

def create_mbtiles(chart_code: str, bbox: tuple, output_path: str, existing_conn=None, progress_callback=None):
    minLon, minLat, maxLon, maxLat = bbox
    
    # Se já temos uma conexão aberta (Modo Arquivo Único), usamos ela.
    if existing_conn:
        conn = existing_conn
    else:
        conn = sqlite3.connect(output_path)
        conn.execute("PRAGMA journal_mode=DELETE")

    # Garante estrutura nas tabelas
    conn.execute("""CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS tiles (
        zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB,
        PRIMARY KEY (zoom_level, tile_column, tile_row)
    )""")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_idx ON tiles (zoom_level, tile_column, tile_row)")

    if not existing_conn:
        name = f"WAC {chart_code}"
        conn.executemany("INSERT OR REPLACE INTO metadata VALUES (?, ?)", [
            ("name", name),
            ("type", "overlay"),
            ("version", "1.0"),
            ("description", f"World Aeronautical Chart {chart_code} — DECEA ICA"),
            ("format", "png"),
            ("minzoom", str(MIN_ZOOM)),
            ("maxzoom", str(MAX_ZOOM)),
            ("bounds", f"{minLon},{minLat},{maxLon},{maxLat}"),
            ("scheme", "tms"),
        ])
    
    conn.commit()

    session = requests.Session()
    session.headers.update({"User-Agent": "SkyFPL-Bot/1.0"})

    tiles_to_fetch = []
    total_tiles = 0
    tiles_to_fetch = []
    total_tiles = 0
    for z in range(MIN_ZOOM, MAX_ZOOM + 1):
        x0, y_max_lat = latLngToTile(maxLat, minLon, z) # latLngToTile(lat, lng, z)
        x1, y_min_lat = latLngToTile(minLat, maxLon, z)
        
        y0 = y_max_lat # maxLat → menor y
        y1 = y_min_lat # minLat → maior y
        
        for x in range(x0, x1 + 1):
            for y in range(y0, y1 + 1):
                total_tiles += 1
                tiles_to_fetch.append((x, y, z))

    # O DECEA derruba conexões massivas em processamento contínuo (Ataque DDOS)
    # Limitando para 10 threads conforme solicitado para equilibrar velocidade e estabilidade.
    workers_env = os.environ.get("WORKERS", "").strip()
    if workers_env.isdigit() and int(workers_env) > 0:
        workers = int(workers_env)
    else:
        workers = 10 if SINGLE_FILE else 20
    print(f"  [{chart_code}] Total de tiles: {total_tiles} (Z{MIN_ZOOM}-Z{MAX_ZOOM}) - Baixando com {workers} Workers...")

    done = 0

    layer = WAC_LAYERS.get(chart_code, "ICA:WAC")

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
                    # MBTiles usa TMS (Y invertido)
                    tms_y = (2 ** z) - 1 - y
                    
                    # 🔴 SEÇÃO CRÍTICA ATÔMICA (Lock para evitar buracos em áreas de sobreposição)
                    with mbtiles_lock:
                        if existing_conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (z, x, tms_y))
                            row = cursor.fetchone()
                            
                            if row:
                                try:
                                    # O tile já existe (pertencente a uma carta vizinha processada antes)
                                    bg_img = Image.open(BytesIO(row[0])).convert("RGBA")
                                    fg_img = Image.open(BytesIO(tile_data)).convert("RGBA")
                                    
                                    # Cola a imagem nova mantendo pixels vivos por baixo do Alpha 0 (transparência)
                                    bg_img.alpha_composite(fg_img)
                                    
                                    out_io = BytesIO()
                                    bg_img.save(out_io, format="PNG")
                                    tile_data = out_io.getvalue()
                                except Exception as e:
                                    print(f"  [WARN] Falha na mesclagem fotográfica (z={z} x={x} y={y}): {e}")

                        # 🟢 INSERÇÃO COMUM TBD MODO (LEGADO E ÚNICA COM MERGE)
                        conn.execute(
                            "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                            (z, x, tms_y, tile_data)
                        )
            except Exception as e:
                print(f"  [WARN] Erro ao processar resultado do tile z={z} x={x} y={y}: {e}")
                
            done += 1
            if done % 100 == 0:
                with mbtiles_lock:
                    conn.commit()
                print(f"  [{chart_code}] Progresso: {done}/{total_tiles} tiles")
                if progress_callback:
                    progress_callback(done, total_tiles)

    conn.commit()
    
    if not existing_conn:
        print(f"  [{chart_code}] Otimizando MBTiles (VACUUM)...")
        conn.execute("VACUUM")
        conn.close()

    # --- AUDITORIA DE INTEGRIDADE PÓS-GERAÇÃO ---
    print(f"  [{chart_code}] Auditando integridade do arquivo: {output_path}...")
    try:
        check_conn = sqlite3.connect(output_path)
        result = check_conn.execute("PRAGMA integrity_check").fetchone()[0]
        check_conn.close()
        if result != "ok":
            raise Exception(f"MBTiles corrompido detectado: {result}")
        print(f"  [{chart_code}] ✅ Integridade 100% Confirmada!")
    except Exception as e:
        if os.path.exists(output_path): os.remove(output_path)
        raise e

    print(f"  [{chart_code}] MBTiles gerado e auditado: {output_path}")

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
        "run_id": os.environ.get("RUN_ID"), # 🔑 Enable remote cancellation
        "metadata": metadata,
        "tile_scheme": "tms", # 🔑 SkyFPL Standard
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    # Adiciona sub-progresso (parciais dentro da carta atual) se disponível
    if metadata and "sub_percent" in metadata:
        data["sub_percent"] = metadata["sub_percent"]
        if charts_total:
            base_percent = (len(charts_done) / len(charts_total)) * 100
            current_contribution = (1 / len(charts_total)) * metadata["sub_percent"]
            data["percent"] = int(base_percent + current_contribution)

    s3_client.put_object(
        Bucket=R2_BUCKET,
        Key="wac_progress.json",
        Body=json.dumps(data, ensure_ascii=False),
        ContentType="application/json",
        CacheControl="no-cache, no-store",
    )

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    all_codes = list(WAC_BBOXES.keys())

    if CHART_CODES_ENV.upper() == "ALL":
        codes_to_process = all_codes
    else:
        codes_to_process = [c.strip().upper() for c in CHART_CODES_ENV.split(",") if c.strip().upper() in WAC_BBOXES]

    if not codes_to_process:
        print("[ERRO] Nenhum código WAC válido encontrado em CHART_CODES:", CHART_CODES_ENV)
        sys.exit(1)

    print(f"[WAC Processor] Processando {len(codes_to_process)} carta(s): {', '.join(codes_to_process)}")

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
    
    # Acumuladores para Modo Arquivo Único
    single_conn = None
    single_output_path = ""
    global_bbox = [180.0, 90.0, -180.0, -90.0] # [minLon, minLat, maxLon, maxLat]

    with tempfile.TemporaryDirectory() as tmpdir:
        if SINGLE_FILE:
            single_output_path = os.path.join(tmpdir, "WAC_BRASIL_FULL.mbtiles")
            single_conn = sqlite3.connect(single_output_path)
            single_conn.execute("PRAGMA journal_mode=DELETE")
            print(f"[WAC Processor] 🛡️ Modo ARQUIVO ÚNICO ATIVADO: Criando {single_output_path}")

        for i, code in enumerate(codes_to_process):
            print(f"\n[{i+1}/{len(codes_to_process)}] Processando: {code}")
            upload_progress_json(s3, charts_done, codes_to_process, current=code, metadata=metadata)

            bbox = WAC_BBOXES[code]
            
            # Atualiza o BBox Global para o metadados final
            global_bbox[0] = min(global_bbox[0], bbox[0])
            global_bbox[1] = min(global_bbox[1], bbox[1])
            global_bbox[2] = max(global_bbox[2], bbox[2])
            global_bbox[3] = max(global_bbox[3], bbox[3])

            output_path = single_output_path if SINGLE_FILE else os.path.join(tmpdir, f"{code}.mbtiles")

            try:
                def on_tile_progress(done, total):
                    sub_p = (done / total) * 100
                    metadata["sub_percent"] = sub_p
                    upload_progress_json(s3, charts_done, codes_to_process, current=code, metadata=metadata)

                create_mbtiles(code, bbox, output_path, existing_conn=single_conn, progress_callback=on_tile_progress)
                
                if not SINGLE_FILE:
                    size_bytes = upload_to_r2(output_path, f"wac/{code}.mbtiles")
                    charts_done.append(code)
                    metadata[code] = {
                        "size_bytes": size_bytes,
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    }
                    os.remove(output_path)
                else:
                    charts_done.append(code)
                    # ✅ FIX: Atualiza o progresso mesmo no modo único
                    upload_progress_json(s3, charts_done, codes_to_process, metadata=metadata)
            except Exception as e:
                print(f"  [ERRO] Falha ao processar {code}: {e}")
                continue

        # Finalização Modo Único
        if SINGLE_FILE and single_conn:
            print(f"\n[WAC Processor] 🏁 Finalizando Arquivo Único Brasileiro...")
            # Injeta Metadados Globais
            single_conn.executemany("INSERT OR REPLACE INTO metadata VALUES (?, ?)", [
                ("name", "SkyFPL WAC Brasil Full"),
                ("type", "overlay"),
                ("version", "1.0"),
                ("description", "World Aeronautical Chart — Brasil Completo"),
                ("format", "png"),
                ("minzoom", str(MIN_ZOOM)),
                ("maxzoom", str(MAX_ZOOM)),
                ("bounds", f"{global_bbox[0]},{global_bbox[1]},{global_bbox[2]},{global_bbox[3]}"),
                ("scheme", "tms"),
            ])
            single_conn.commit()
            print("  Otimizando (VACUUM)...")
            single_conn.execute("VACUUM")
            single_conn.close()
            
            size_bytes = upload_to_r2(single_output_path, "wac/WAC_BRASIL_FULL.mbtiles")
            metadata["WAC_BRASIL_FULL"] = {
                "size_bytes": size_bytes,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "minzoom": MIN_ZOOM,
                "maxzoom": MAX_ZOOM
            }

    # Upload final de progresso: completed
    upload_progress_json(s3, charts_done, codes_to_process, metadata=metadata)
    print(f"\n[WAC Processor] ✅ Concluído! {len(charts_done)}/{len(codes_to_process)} cartas processadas.")

if __name__ == "__main__":
    main()
