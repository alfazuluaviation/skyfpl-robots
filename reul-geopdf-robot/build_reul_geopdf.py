"""
build_reul_geopdf.py — SkyFPL High-Definition REUL Chart Engine (GeoPDF / GDAL) v2.0
===================================================================================
Processa as cartas aeronáuticas de Rotas Especiais de Ultraleves (REUL) do DECEA
individualmente em altíssima definição, rasterizadas a partir dos arquivos mestres GeoPDF
vetoriais oficiais do AISWEB (com fallback para os GeoTIFFs oficiais do DECEA).

Pilares e Padrão Ouro:
  1. Processamento 100% individual setor a setor (cada setor gera seu próprio MBTiles e PMTiles).
  2. Leitura vetorial em alta resolução com GDAL (DPI 300, 450 ou 600, Web Mercator EPSG:3857).
  3. Recorte exato pela BBOX / Máscara oficial do Catálogo REUL DECEA.
  4. Fatiamento em blocos nativos WebP RGBA (qualidade 85) e pirâmides overviews Lanczos/Cubic.
  5. Descarte de tiles vazios/transparentes (< 100 bytes para WebP) seguido de VACUUM.
  6. Conversão instantânea para Protomaps PMTiles v3 (consumo Web HTTP Range).
  7. Quarentena no Cloudflare R2 (reul/staging/{code}_HD.mbtiles e .pmtiles).
  8. Telemetria em tempo real no R2 (reul_hd_progress.json) e alertas opcionais no Telegram.
  9. Limpeza imediata de memória e disco: PDFs e rasters intermediários são descartados
     imediatamente após a conclusão de cada setor.

Uso:
  CHART_CODES=REUL_WJ3_RIO_DE_JANEIRO DPI=600 MAX_ZOOM=11 python build_reul_geopdf.py
  CHART_CODES=ALL python build_reul_geopdf.py
"""

import os
import sys
import json
import time
import shutil
import sqlite3
import tempfile
import subprocess
import requests
try:
    import boto3
except ImportError:
    boto3 = None
from datetime import datetime, timezone, timedelta

# ─── Configurações Dinâmicas (Dashboard Admin / GitHub Actions / CLI) ──────────

CHART_CODES_ENV = os.environ.get("CHART_CODES", "").strip()
DPI = int(os.environ.get("DPI", 600))
RESAMPLING = os.environ.get("RESAMPLING", "cubic").strip().lower()
TILE_FORMAT = os.environ.get("TILE_FORMAT", "webp").strip().lower()
WEBP_QUALITY = int(os.environ.get("WEBP_QUALITY", 85))
MIN_ZOOM = int(os.environ.get("MIN_ZOOM", 6))
MAX_ZOOM = int(os.environ.get("MAX_ZOOM", 11))
R2_PREFIX = os.environ.get("R2_PREFIX", "reul/staging").strip().rstrip("/")
PROGRESS_KEY = os.environ.get("PROGRESS_KEY", "reul_hd_progress.json").strip()

def get_empty_tile_threshold(tile_format: str) -> int:
    """Para WebP, tiles 100% transparentes têm ~30-45 bytes; usamos 100B de corte de segurança."""
    fmt = (tile_format or "").strip().lower()
    return 100 if fmt == "webp" else 350

REUL_EMPTY_THRESHOLD = get_empty_tile_threshold(TILE_FORMAT)

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")

# ─── Carregamento do Catálogo e Polígonos Oficiais REUL ────────────────────────

CATALOG_FILE = os.path.join(os.path.dirname(__file__), "reul_catalog.json")
POLYGONS_FILE = os.path.join(os.path.dirname(__file__), "reul_official_polygons.json")

def load_official_polygons() -> dict:
    if os.path.exists(POLYGONS_FILE):
        try:
            with open(POLYGONS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[Aviso] Falha ao ler polígonos oficiais ({e})", flush=True)
    return {}

OFFICIAL_POLYGONS = load_official_polygons()

def fetch_live_reul_catalog() -> dict:
    """
    Consulta a página oficial de Cartas Visuais do AISWEB em tempo real
    para obter links vigentes dos GeoPDFs das cartas REUL (ultraleves).
    """
    live_urls = {}
    try:
        url = "https://aisweb.decea.mil.br/?i=cartas&p=visuais"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "close"
        }
        res = requests.get(url, headers=headers, timeout=25)
        if res.status_code == 200:
            import re
            rows = re.findall(r"<tr[^>]*>.*?</tr>", res.text, re.DOTALL)
            reul_rows = [row for row in rows if 'data-order="REUL"' in row or 'REUL' in row.upper()]
            for row in reul_rows:
                m = re.search(r'href=[\'"]([^\'"]+\.pdf)[\'"]', row)
                if m:
                    pdf_url = m.group(1).strip()
                    fname = pdf_url.split("/")[-1]
                    live_urls[fname.lower()] = pdf_url
            if live_urls:
                print(f"[AISWEB] Descoberta dinâmica: {len(live_urls)} cartas REUL encontradas no portal.", flush=True)
    except Exception as e:
        print(f"[Aviso] Falha ao consultar catálogo dinâmico do AISWEB: {e}", flush=True)
    return live_urls

def load_catalog() -> dict:
    catalog = {}
    if os.path.exists(CATALOG_FILE):
        try:
            with open(CATALOG_FILE, "r", encoding="utf-8") as f:
                catalog = json.load(f)
        except Exception as e:
            print(f"[Aviso] Falha ao ler catálogo local ({e})", flush=True)

    # Se URLs dinâmicas do AISWEB estiverem disponíveis, atualiza no catálogo em memória
    live_map = fetch_live_reul_catalog()
    if live_map:
        for code, item in catalog.items():
            c_lower = code.lower()
            for fname, u in live_map.items():
                if "wj3" in fname or "reul" in fname:
                    item["pdf_url"] = u
                    break

    return catalog

def get_airac_cycle_info() -> dict:
    """Calcula ciclo AIRAC vigente a partir de calendar.json ou fallback."""
    cal_file = os.path.join(os.path.dirname(__file__), "calendar.json")
    master_cal = {}
    if os.path.exists(cal_file):
        try:
            with open(cal_file, "r", encoding="utf-8") as f:
                master_cal = json.load(f)
        except Exception as e:
            print(f"[Aviso] Falha ao ler calendar.json: {e}", flush=True)

    now = datetime.now(timezone.utc)
    all_cycles = []
    for yr, cycles in master_cal.items():
        for cid, dt_str in cycles.items():
            p = [int(x) for x in dt_str.split("/")]
            dt = datetime(p[2], p[1], p[0], tzinfo=timezone.utc)
            all_cycles.append({
                "cycle": cid,
                "amdt": cid,
                "effective_dt": dt,
                "effective_date": dt_str,
                "iso_effective_date": dt.strftime("%Y-%m-%d"),
                "publication_date": (dt - timedelta(days=14)).strftime("%d/%m/%Y"),
                "expiration_date": (dt + timedelta(days=28)).strftime("%d/%m/%Y"),
            })

    all_cycles.sort(key=lambda x: x["effective_dt"])

    current = None
    next_c = None
    for i, c in enumerate(all_cycles):
        if c["effective_dt"] <= now:
            current = c
            if i + 1 < len(all_cycles):
                next_c = all_cycles[i + 1]

    if not current and all_cycles:
        current = all_cycles[0]

    target = current
    if next_c:
        days = (next_c["effective_dt"] - now).days
        if 0 <= days <= 14:
            target = next_c

    return target or {
        "cycle": "2609",
        "amdt": "2609",
        "effective_date": "03/09/2026",
        "publication_date": "20/08/2026",
        "expiration_date": "01/10/2026"
    }

# ─── Notificações Táticas no Telegram ─────────────────────────────────────────

def send_telegram_notification(
    title: str,
    status: str,
    cycle: str = "",
    effective_date: str = "",
    processed_items: list = None,
    error_msg: str = None,
    step: str = ""
) -> bool:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        return False

    now_str = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M:%S UTC")
    icon = "✅" if status == "SUCCESS" else "🚨"

    msg_lines = [
        f"{icon} *{title}*",
        f"📅 *Data/Hora:* `{now_str}`",
        f"🔄 *Ciclo AIRAC:* `{cycle}` (Efetivação: `{effective_date}`)" if cycle else "",
        f"📍 *Etapa:* `{step}`" if step else "",
    ]

    if processed_items:
        msg_lines.append(f"📦 *Setores REUL Processados ({len(processed_items)}):*")
        for it in processed_items[:15]:
            msg_lines.append(f"  • `{it}`")

    if error_msg:
        msg_lines.append(f"❌ *Erro:* `{error_msg}`")

    text = "\n".join([line for line in msg_lines if line])
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload, timeout=12)
        return r.status_code == 200
    except Exception:
        return False

# ─── Gerenciador de Telemetria e Progresso ao Vivo ────────────────────────────

class TelemetryManager:
    def __init__(self, s3_client, target_codes: list, airac: dict = None):
        self.s3 = s3_client
        self.target_codes = target_codes
        self.total = len(target_codes)
        self.airac = airac or {}
        self.status = "processing"
        self.logs = []
        self.metadata = {}
        self.done_codes = set()
        self.start_time = datetime.now(timezone.utc).isoformat()
        self._load_existing_metadata()

    def _load_existing_metadata(self):
        if not self.s3:
            return
        try:
            resp = self.s3.get_object(Bucket=R2_BUCKET, Key=PROGRESS_KEY)
            data = json.loads(resp["Body"].read().decode("utf-8"))
            if data and isinstance(data.get("metadata"), dict):
                self.metadata = data["metadata"]
                print(f"[Telemetria] Metadados pré-existentes carregados: {len(self.metadata)} setor(es) mantidos.", flush=True)
        except Exception:
            pass

    def log(self, message: str, current_idx: int = 0, chart_progress_pct: int = 0, level: str = "INFO"):
        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        prefix = "ℹ️" if level == "INFO" else ("⚠️" if level == "WARN" else "❌")
        log_line = f"[{now_str}] {prefix} {message}"
        self.logs.append(log_line)
        print(log_line, flush=True)

        if self.total > 0:
            done_cnt = len(self.done_codes)
            overall_pct = int(((done_cnt + (chart_progress_pct / 100.0)) / self.total) * 100)
            overall_pct = min(max(overall_pct, 1), 99)
        else:
            overall_pct = 0

        current_code = self.target_codes[current_idx] if current_idx < len(self.target_codes) else ""
        self._push(overall_pct, current_code)

    def chart_completed(self, code: str, mbtiles_size: int, pmtiles_size: int, min_zoom: int, max_zoom: int, bounds: str, chart_info: dict = None):
        self.done_codes.add(code)
        now_iso = datetime.now(timezone.utc).isoformat()
        info = chart_info or {}

        self.metadata[code] = {
            "code": code,
            "name": info.get("name", code),
            "ident": info.get("ident", code),
            "cycle": self.airac.get("cycle", "2609"),
            "effective_date": self.airac.get("effective_date", "03/09/2026"),
            "size_bytes": mbtiles_size,
            "size_mb": f"{mbtiles_size / (1024 * 1024):.1f}",
            "pmtiles_bytes": pmtiles_size,
            "pmtiles_mb": f"{pmtiles_size / (1024 * 1024):.1f}",
            "min_zoom": min_zoom,
            "max_zoom": max_zoom,
            "bounds": bounds,
            "processed_at": now_iso,
            "r2_url_mbtiles": f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{R2_PREFIX}/{code}_HD.mbtiles",
            "r2_url_pmtiles": f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{R2_PREFIX}/{code}.pmtiles",
        }

        done_cnt = len(self.done_codes)
        overall_pct = int((done_cnt / self.total) * 100) if self.total > 0 else 100
        overall_pct = min(overall_pct, 99)
        self._push(overall_pct, code)

    def finish(self, status: str = "completed"):
        self.status = status
        pct = 100 if status == "completed" else int((len(self.done_codes) / max(self.total, 1)) * 100)
        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self.logs.append(f"[{now_str}] 🏁 Processamento REUL HD finalizado com status: {status.upper()}!")
        self._push(pct, "")

    def _push(self, progress_pct: int, current_code: str):
        if not self.s3:
            return

        payload = {
            "status": self.status,
            "progress_percent": progress_pct,
            "current_chart": current_code,
            "charts_done": len(self.done_codes),
            "charts_total": self.total,
            "target_charts": self.target_codes,
            "cycle": self.airac.get("cycle", "2609"),
            "effective_date": self.airac.get("effective_date", "03/09/2026"),
            "dpi": DPI,
            "resampling": RESAMPLING,
            "tile_format": TILE_FORMAT,
            "webp_quality": WEBP_QUALITY,
            "min_zoom": MIN_ZOOM,
            "max_zoom": MAX_ZOOM,
            "started_at": self.start_time,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "metadata": self.metadata,
            "logs": self.logs[-250:]
        }

        try:
            self.s3.put_object(
                Bucket=R2_BUCKET,
                Key=PROGRESS_KEY,
                Body=json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
                CacheControl="no-cache, no-store, must-revalidate"
            )
        except Exception:
            pass

def find_gdal_tool(tool_name: str) -> str:
    found = shutil.which(tool_name)
    if found:
        return found

    if sys.platform.startswith("win"):
        found = shutil.which(f"{tool_name}.exe")
        if found:
            return found

        possible_paths = [
            f"C:\\Program Files\\QGIS 3.40.15\\bin\\{tool_name}.exe",
            f"C:\\Program Files\\QGIS 3.38.3\\bin\\{tool_name}.exe",
            f"C:\\OSGeo4W\\bin\\{tool_name}.exe",
            f"C:\\OSGeo4W64\\bin\\{tool_name}.exe"
        ]
        for p in possible_paths:
            if os.path.exists(p):
                return p

    return tool_name

def download_file(url: str, dest_path: str, telemetry: TelemetryManager, chart_idx: int, label: str = "Arquivo", max_retries: int = 5) -> bool:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Connection": "close"
    }
    retry_delays = [5, 12, 25, 45, 60]

    session = requests.Session()
    session.headers.update(headers)

    for attempt in range(1, max_retries + 1):
        try:
            telemetry.log(f"Baixando {label} (tentativa {attempt}/{max_retries})...", chart_idx, 8)
            with session.get(url, stream=True, timeout=90, allow_redirects=True) as r:
                if r.status_code == 200:
                    with open(dest_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=128 * 1024):
                            if chunk:
                                f.write(chunk)

                    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000:
                        size_mb = os.path.getsize(dest_path) / (1024 * 1024)
                        telemetry.log(f"Download de {label} concluído com sucesso ({size_mb:.2f} MB)!", chart_idx, 15)
                        return True
                    else:
                        telemetry.log(f"Arquivo recebido vazio ou incompleto para {url}", chart_idx, 10, level="WARN")
                else:
                    telemetry.log(f"HTTP {r.status_code} ao baixar {url}", chart_idx, 10, level="WARN")
        except Exception as e:
            telemetry.log(f"Aviso de download na tentativa {attempt}: {e}", chart_idx, 10, level="WARN")

        wait_s = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
        time.sleep(wait_s)

    return False

def get_pdf_suppressed_layers(gdalinfo_bin: str, pdf_path: str) -> list:
    try:
        cmd = [gdalinfo_bin, "-mdd", "LAYERS", pdf_path]
        res = subprocess.run(cmd, capture_output=True, text=True, errors="replace")
        if res.returncode == 0:
            unwanted = ["suppressed", "boundary_box"]
            off_layers = []
            for line in res.stdout.splitlines():
                if "=" in line and "LAYER_" in line:
                    val = line.split("=", 1)[1].strip()
                    if any(p in val.lower() for p in unwanted):
                        if val and val not in off_layers:
                            off_layers.append(val)
            return off_layers
    except Exception:
        pass
    return []

# ─── Processamento Individual de Setor REUL para MBTiles ──────────────────────

def process_chart_to_mbtiles(
    code: str,
    chart_info: dict,
    output_mbtiles: str,
    telemetry: TelemetryManager,
    chart_idx: int,
    airac: dict = None
) -> bool:
    bbox = chart_info.get("bbox")
    name = chart_info.get("name", code)
    pdf_url = chart_info.get("pdf_url", "").strip()
    geotiff_url = chart_info.get("geotiff_url", "").strip()

    telemetry.log(f"Iniciando processamento em alta definição de {code} ({name})...", chart_idx, 5)

    gdalinfo = find_gdal_tool("gdalinfo")
    gdalwarp = find_gdal_tool("gdalwarp")
    gdal_translate = find_gdal_tool("gdal_translate")
    gdaladdo = find_gdal_tool("gdaladdo")

    with tempfile.TemporaryDirectory() as tmpdir:
        input_file = None

        # 1. Tenta baixar o GeoPDF vetorial mestre oficial do AISWEB
        if pdf_url:
            pdf_path = os.path.join(tmpdir, f"{code}.pdf")
            ok_pdf = download_file(pdf_url, pdf_path, telemetry, chart_idx, label="GeoPDF mestre")
            if ok_pdf and os.path.exists(pdf_path):
                with open(pdf_path, "rb") as pf:
                    header = pf.read(5)
                if header.startswith(b"%PDF"):
                    input_file = pdf_path
                else:
                    telemetry.log(f"Arquivo baixado para {code} não é PDF válido.", chart_idx, 15, level="WARN")

        # Fallback: GeoTIFF oficial do DECEA
        if not input_file and geotiff_url:
            telemetry.log(f"Utilizando GeoTIFF oficial do DECEA como fonte para {code}...", chart_idx, 16)
            tif_path = os.path.join(tmpdir, f"{code}_source.tif")
            ok_tif = download_file(geotiff_url, tif_path, telemetry, chart_idx, label="GeoTIFF oficial")
            if ok_tif and os.path.exists(tif_path):
                input_file = tif_path

        if not input_file:
            telemetry.log(f"ERRO FATAL: Falha ao obter fonte de dados para {code}.", chart_idx, 18, level="ERROR")
            return False

        warped_tif = os.path.join(tmpdir, f"{code}_warped.tif")

        # 2. GDALWARP: Reprojeção em EPSG:3857 com DPI calibrado e recorte oficial
        telemetry.log(f"Renderizando via GDAL ({DPI} DPI, EPSG:3857, Recorte da Moldura Oficial)...", chart_idx, 25)
        warp_cmd = [
            gdalwarp,
            "--config", "GDAL_PDF_DPI", str(DPI),
            "-t_srs", "EPSG:3857",
            "-r", RESAMPLING,
            "-co", "COMPRESS=DEFLATE",
            "-co", "TILED=YES",
            "-dstalpha",
            "-overwrite",
            "-q"
        ]

        if input_file.lower().endswith(".pdf"):
            off_layers = get_pdf_suppressed_layers(gdalinfo, input_file)
            if off_layers:
                warp_cmd.extend(["--config", "GDAL_PDF_LAYERS_OFF", ",".join(off_layers)])

        # Aplica máscara de polígono oficial se disponível, ou a BBOX canônica
        clean_ident = chart_info.get("ident", code).replace("ICA:", "")
        poly_source = OFFICIAL_POLYGONS.get(clean_ident) or OFFICIAL_POLYGONS.get(code)
        
        if poly_source and poly_source.get("coordinates"):
            poly_data = {
                "type": "FeatureCollection",
                "crs": {
                    "type": "name",
                    "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}
                },
                "features": [{
                    "type": "Feature",
                    "properties": {"code": code},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": poly_source["coordinates"]
                    }
                }]
            }
            cutline_path = os.path.join(tmpdir, f"cutline_{code}.geojson")
            with open(cutline_path, "w", encoding="utf-8") as pf:
                json.dump(poly_data, pf)
            warp_cmd.extend(["-cutline", cutline_path, "-crop_to_cutline"])
        elif bbox:
            warp_cmd.extend(["-te", str(bbox[0]), str(bbox[1]), str(bbox[2]), str(bbox[3]), "-te_srs", "EPSG:4326"])

        # Resolução Web Mercator em metros por pixel conforme MAX_ZOOM:
        if MAX_ZOOM >= 13:
            warp_cmd.extend(["-tr", "19.10925948", "19.10925948"])
        elif MAX_ZOOM == 12:
            warp_cmd.extend(["-tr", "38.21851897", "38.21851897"])
        elif MAX_ZOOM == 11:
            warp_cmd.extend(["-tr", "76.43703794", "76.43703794"])
        else:
            warp_cmd.extend(["-tr", "152.87407588", "152.87407588"])

        warp_cmd.extend([input_file, warped_tif])

        t0 = time.time()
        res1 = subprocess.run(warp_cmd, capture_output=True, text=True)
        if res1.returncode != 0:
            telemetry.log(f"ERRO GDALWARP em {code}: {res1.stderr[:200]}", chart_idx, 30, level="ERROR")
            return False
        telemetry.log(f"Rasterização e reprojeção concluídas em {time.time()-t0:.1f}s!", chart_idx, 48)

        # 3. GDAL_TRANSLATE: Empacota em MBTiles com compressão WebP
        tile_fmt_upper = TILE_FORMAT.upper()
        telemetry.log(f"Empacotando MBTiles WebP (Qualidade: {WEBP_QUALITY})...", chart_idx, 55)
        translate_cmd = [
            gdal_translate,
            "-of", "MBTILES",
            "-co", f"TILE_FORMAT={tile_fmt_upper}",
        ]
        if tile_fmt_upper == "WEBP":
            translate_cmd.extend(["-co", f"QUALITY={WEBP_QUALITY}"])

        translate_cmd.extend([warped_tif, output_mbtiles])
        t1 = time.time()
        res2 = subprocess.run(translate_cmd, capture_output=True, text=True)
        if res2.returncode != 0:
            telemetry.log(f"ERRO GDAL_TRANSLATE: {res2.stderr[:200]}", chart_idx, 60, level="ERROR")
            return False
        telemetry.log(f"Base MBTiles gerada em {time.time()-t1:.1f}s!", chart_idx, 68)

        # Metadados preliminares
        try:
            conn = sqlite3.connect(output_mbtiles)
            cur = conn.cursor()
            cur.execute("INSERT OR REPLACE INTO metadata (name, value) VALUES ('format', ?)", (TILE_FORMAT.lower(),))
            conn.commit()
            conn.close()
        except Exception:
            pass

        # 4. GDALADDO: Pirâmides completas de overviews
        telemetry.log(f"Gerando pirâmides de overviews Lanczos (Z{MIN_ZOOM} a Z{MAX_ZOOM})...", chart_idx, 72)
        if MAX_ZOOM >= 12 or DPI >= 600:
            overviews = ["2", "4", "8", "16", "32", "64", "128"]
        else:
            overviews = ["2", "4", "8", "16", "32", "64"]

        addo_cmd = [
            gdaladdo,
            "-r", RESAMPLING,
            output_mbtiles,
            *overviews
        ]
        t2 = time.time()
        res3 = subprocess.run(addo_cmd, capture_output=True, text=True)
        if res3.returncode != 0:
            telemetry.log(f"ERRO GDALADDO: {res3.stderr[:200]}", chart_idx, 80, level="ERROR")
            return False
        telemetry.log(f"Pirâmides overviews concluídas em {time.time()-t2:.1f}s!", chart_idx, 82)

    # 5. Expulso de tiles vazios + VACUUM + Metadados Oficiais
    telemetry.log(f"Aplicando filtro SkyFPL (Threshold {REUL_EMPTY_THRESHOLD}B para {TILE_FORMAT.upper()})...", chart_idx, 85)
    try:
        conn = sqlite3.connect(output_mbtiles)
        cur = conn.cursor()

        # Converte overviews PNG gerados pelo gdaladdo para WebP puro se TILE_FORMAT == 'webp'
        if TILE_FORMAT.lower() == 'webp':
            try:
                from PIL import Image
                import io
                all_tiles = cur.execute("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles").fetchall()
                for tz, tx, ty, tdata in all_tiles:
                    if tdata and len(tdata) >= 4 and tdata[:4] == b'\x89PNG':
                        img = Image.open(io.BytesIO(tdata))
                        obuf = io.BytesIO()
                        img.save(obuf, format='WEBP', quality=WEBP_QUALITY, method=6)
                        cur.execute("UPDATE tiles SET tile_data = ? WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?", (obuf.getvalue(), tz, tx, ty))
                conn.commit()

                # Síntese QuadTree de pirâmides para zooms inferiores (Z9 e Z8)
                cur.execute("SELECT MIN(zoom_level) FROM tiles")
                curr_min_row = cur.fetchone()
                curr_min = curr_min_row[0] if (curr_min_row and curr_min_row[0] is not None) else 10
                target_min = max(MIN_ZOOM, 8)
                while curr_min > target_min:
                    parent_z = curr_min - 1
                    child_rows = cur.execute("SELECT tile_column, tile_row, tile_data FROM tiles WHERE zoom_level = ?", (curr_min,)).fetchall()
                    parent_tiles = {}
                    for tx, tms_y, data in child_rows:
                        xyz_y = (1 << curr_min) - 1 - tms_y
                        px = tx >> 1
                        py = xyz_y >> 1
                        qx = tx % 2
                        qy = xyz_y % 2
                        img = Image.open(io.BytesIO(data)).convert("RGBA")
                        parent_tiles.setdefault((px, py), {})[(qx, qy)] = img

                    for (px, py), quads in parent_tiles.items():
                        canvas = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
                        for (qx, qy), child_img in quads.items():
                            resized = child_img.resize((128, 128), Image.Resampling.LANCZOS)
                            canvas.paste(resized, (qx * 128, qy * 128), resized)
                        out_buf = io.BytesIO()
                        canvas.save(out_buf, format="WEBP", quality=WEBP_QUALITY, method=6)
                        parent_tms_y = (1 << parent_z) - 1 - py
                        cur.execute("INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                                    (parent_z, px, parent_tms_y, out_buf.getvalue()))
                    conn.commit()
                    curr_min = parent_z
            except Exception as pe:
                telemetry.log(f"Aviso na conversão WebP de overviews: {pe}", chart_idx, 86, level="WARN")

        cur.execute("DELETE FROM tiles WHERE length(tile_data) < ?", (REUL_EMPTY_THRESHOLD,))
        purged = cur.rowcount
        if purged > 0:
            telemetry.log(f"🛡️ {purged} tiles transparentes (<{REUL_EMPTY_THRESHOLD}B) expurgados!", chart_idx, 87)

        cur.execute("SELECT MIN(zoom_level), MAX(zoom_level), count(*) FROM tiles")
        row = cur.fetchone()
        actual_min = row[0] if (row and row[0] is not None) else MIN_ZOOM
        actual_max = row[1] if (row and row[1] is not None) else MAX_ZOOM
        total_tiles = row[2] if (row and row[2] is not None) else 0

        cur.execute("DELETE FROM metadata")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata_name ON metadata (name)")

        airac = airac or {}
        bounds_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}" if bbox else ""
        cur.execute("""
            INSERT OR REPLACE INTO metadata (name, value) VALUES
            ('name', ?),
            ('type', 'overlay'),
            ('version', '2.0-HD'),
            ('description', ?),
            ('format', ?),
            ('bounds', ?),
            ('minzoom', ?),
            ('maxzoom', ?),
            ('chart_code', ?),
            ('amdt', ?),
            ('cycle', ?),
            ('effective_date', ?),
            ('publication_date', ?)
        """, (
            f"SkyFPL REUL {code}",
            f"Carta REUL {code} - {name} (SkyFPL HD GeoPDF Lanczos)",
            TILE_FORMAT.lower(),
            bounds_str,
            str(actual_min),
            str(actual_max),
            code,
            airac.get("amdt", airac.get("cycle", "2609")),
            airac.get("cycle", "2609"),
            airac.get("effective_date", "03/09/2026"),
            airac.get("publication_date", "")
        ))

        conn.commit()
        cur.execute("VACUUM")
        conn.close()

        telemetry.log(f"Indexação concluída: {total_tiles} tiles gravados (Z{actual_min}-Z{actual_max}).", chart_idx, 90)
        return True
    except Exception as e:
        telemetry.log(f"Erro ao auditar base SQLite: {e}", chart_idx, 88, level="ERROR")
        return False

# ─── Conversão Nativa para PMTiles v3 ─────────────────────────────────────────

def convert_to_pmtiles(local_mbtiles: str, local_pmtiles: str, telemetry: TelemetryManager, chart_idx: int) -> bool:
    pmtiles_bin = shutil.which("pmtiles") or "/usr/local/bin/pmtiles" or "pmtiles"
    if not os.path.exists(pmtiles_bin) and not shutil.which("pmtiles"):
        telemetry.log("Binário 'pmtiles' não encontrado. Pulando conversão PMTiles.", chart_idx, 92, level="WARN")
        return False

    telemetry.log("Convertendo para Protomaps PMTiles v3...", chart_idx, 92)
    t0 = time.time()
    try:
        cmd = [pmtiles_bin, "convert", local_mbtiles, local_pmtiles]
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        telemetry.log(f"PMTiles gerado com sucesso em {time.time()-t0:.2f}s!", chart_idx, 94)
        return True
    except Exception as e:
        telemetry.log(f"Aviso conversão PMTiles: {e}", chart_idx, 93, level="WARN")
        return False

# ─── Upload para Cloudflare R2 ────────────────────────────────────────────────

def upload_to_r2(s3_client, local_path: str, r2_key: str) -> int:
    size_bytes = os.path.getsize(local_path)
    if local_path.endswith(".pmtiles"):
        content_type = "application/x-pmtiles"
    elif local_path.endswith(".mbtiles"):
        content_type = "application/vnd.sqlite3"
    else:
        content_type = "application/json"

    with open(local_path, "rb") as f:
        s3_client.put_object(
            Bucket=R2_BUCKET,
            Key=r2_key,
            Body=f,
            ContentType=content_type,
            CacheControl="no-cache, no-store"
        )
    return size_bytes

# ─── Execução Principal ───────────────────────────────────────────────────────

def main():
    print("=" * 72, flush=True)
    print("🪂 SkyFPL REUL High-Definition Engine (GeoPDF / GDAL) v2.0", flush=True)
    print("=" * 72, flush=True)
    print(f"  DPI de Rasterização: {DPI}", flush=True)
    print(f"  Zoom Alvo: Z{MIN_ZOOM} a Z{MAX_ZOOM}", flush=True)
    print(f"  Algoritmo de Resampling: {RESAMPLING}", flush=True)
    print(f"  Formato dos Tiles: {TILE_FORMAT.upper()} (Qualidade: {WEBP_QUALITY})", flush=True)
    print(f"  Prefixo Staging R2: {R2_PREFIX}/", flush=True)
    print(f"  Threshold de Purga: {REUL_EMPTY_THRESHOLD} bytes", flush=True)
    print(f"  Telemetria R2: {PROGRESS_KEY}", flush=True)
    print("=" * 72, flush=True)

    catalog = load_catalog()
    if not catalog:
        print("[ERRO FATAL] Não foi possível carregar o catálogo de cartas REUL.", flush=True)
        sys.exit(1)

    if CHART_CODES_ENV.upper() in ["ALL", ""]:
        target_codes = list(catalog.keys())
    else:
        requested = [c.strip().upper() for c in CHART_CODES_ENV.split(",") if c.strip()]
        target_codes = []
        for req in requested:
            if req in catalog:
                target_codes.append(req)
            else:
                for k in catalog.keys():
                    if req in k or k.replace("REUL_", "") == req:
                        if k not in target_codes:
                            target_codes.append(k)

    if not target_codes:
        print(f"[ERRO] Nenhum setor REUL válido encontrado em CHART_CODES='{CHART_CODES_ENV}'.", flush=True)
        sys.exit(1)

    s3_client = None
    if boto3 and R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY:
        s3_client = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name="auto"
        )
    else:
        print("[AVISO] Credenciais R2 ausentes. Telemetria e uploads serão simulados localmente.", flush=True)

    airac_info = get_airac_cycle_info()
    telemetry = TelemetryManager(s3_client, target_codes, airac=airac_info)
    telemetry.log(f"Iniciando processamento individual de {len(target_codes)} setor(es) REUL [Ciclo AIRAC {airac_info.get('cycle')} - Vigência: {airac_info.get('effective_date')}]: {', '.join(target_codes)}")

    success_count = 0
    with tempfile.TemporaryDirectory() as workdir:
        for idx, code in enumerate(target_codes):
            chart_info = catalog[code]
            telemetry.log(f"--- Processando [{idx+1}/{len(target_codes)}]: {code} ({chart_info['name']}) ---", idx, 5)

            out_mbtiles = os.path.join(workdir, f"{code}_HD.mbtiles")
            out_pmtiles = os.path.join(workdir, f"{code}.pmtiles")

            ok = process_chart_to_mbtiles(code, chart_info, out_mbtiles, telemetry, idx, airac=airac_info)
            if not ok or not os.path.exists(out_mbtiles):
                telemetry.log(f"Falha ao gerar MBTiles para {code}.", idx, 50, level="ERROR")
                continue

            mbtiles_size = os.path.getsize(out_mbtiles)
            r2_mbtiles_key = f"{R2_PREFIX}/{code}_HD.mbtiles"

            if s3_client:
                telemetry.log(f"Enviando MBTiles HD ({mbtiles_size / 1024 / 1024:.2f} MB) para R2...", idx, 90)
                upload_to_r2(s3_client, out_mbtiles, r2_mbtiles_key)
                telemetry.log(f"✅ Upload MBTiles HD concluído com sucesso no Cloudflare R2 ({r2_mbtiles_key})!", idx, 93)

            # Cópia para o cache local do Admin para visualização imediata no Dashboard
            admin_cache = r"c:\Users\josemir\Desktop\skynav-pro-official\admin\.cache\mbtiles"
            if os.path.exists(admin_cache):
                admin_dest = os.path.join(admin_cache, f"reul_{code}_HD.mbtiles")
                try:
                    shutil.copy2(out_mbtiles, admin_dest)
                    telemetry.log(f"💾 Cópia espelhada salva no cache do Dashboard: {admin_dest}", idx, 94)
                except Exception as e:
                    telemetry.log(f"Aviso ao copiar para cache do admin: {e}", idx, 94, level="WARN")

            # Conversão e Upload PMTiles
            pmtiles_ok = convert_to_pmtiles(out_mbtiles, out_pmtiles, telemetry, idx)
            pmtiles_size = 0
            if pmtiles_ok and os.path.exists(out_pmtiles):
                pmtiles_size = os.path.getsize(out_pmtiles)
                r2_pmtiles_key = f"{R2_PREFIX}/{code}.pmtiles"
                if s3_client:
                    telemetry.log(f"Enviando PMTiles ({pmtiles_size / 1024 / 1024:.2f} MB) para R2...", idx, 96)
                    upload_to_r2(s3_client, out_pmtiles, r2_pmtiles_key)
                    telemetry.log(f"✅ Upload PMTiles concluído com sucesso no Cloudflare R2 ({r2_pmtiles_key})!", idx, 98)

            bbox = chart_info.get("bbox", [])
            bounds_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}" if bbox else ""
            telemetry.chart_completed(code, mbtiles_size, pmtiles_size, MIN_ZOOM, MAX_ZOOM, bounds_str, chart_info=chart_info)
            telemetry.log(f"✅ Setor {code} concluído e registrado no R2 a partir do GeoPDF oficial!", idx, 100)
            success_count += 1

            # 🧹 Limpeza Imediata de Memória e Disco
            for f_tmp in [out_mbtiles, out_pmtiles]:
                try:
                    if os.path.exists(f_tmp):
                        os.remove(f_tmp)
                except Exception:
                    pass

    final_status = "completed" if success_count == len(target_codes) else "partial"
    telemetry.finish(final_status)

    send_telegram_notification(
        title="SkyFPL REUL HD Engine — Concluído",
        status="SUCCESS" if final_status == "completed" else "FAILED",
        cycle=airac_info.get("cycle"),
        effective_date=airac_info.get("effective_date"),
        processed_items=target_codes,
        step="Processamento Individual de Setores REUL"
    )

    print("=" * 72, flush=True)
    print(f"🎉 Processamento concluído! {success_count}/{len(target_codes)} setores REUL gerados.", flush=True)
    print("=" * 72, flush=True)

if __name__ == "__main__":
    main()
