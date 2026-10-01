"""
build_enrc_geopdf.py — SkyFPL High-Definition ENRC LOW Chart Engine (GeoPDF / GDAL) v2.2
========================================================================================
Processa cartas aeronáuticas de rota inferiores (ENRC LOW - L1 a L9) do DECEA diretamente
a partir dos arquivos mestres GeoPDF vetoriais de altíssima definição publicados no AISWEB.

Diferenciais e Padrão Ouro:
  1. Leitura direta dos arquivos mestres GeoPDF vetoriais oficiais do AISWEB (DPI de 300 a 600).
  2. Recorte cirúrgico pela máscara vetorial oficial dos 41 vértices da Projeção de Lambert do DECEA:
     Elimina 100% de bordas brancas, marcas de corte e as barras laterais de legendas/selos.
  3. Resolução calibrada em Web Mercator com pirâmides completas de overviews Lanczos (Z5 a Z11/Z12).
  4. Fatiamento nativo em blocos WebP RGBA (qualidade 85), reduzindo em 50-60% o peso do MBTiles.
  5. Respeito rigoroso ao THRESHOLD CRÍTICO DE 1700 BYTES para descarte de tiles vazios no oceano.
  6. Conversão híbrida instantânea para PMTiles v3 (para consumo HTTP Range no SkyFPL Web).
  7. Upload isolado para quarentena no Cloudflare R2 (enrc/staging/{code}_HD.mbtiles e .pmtiles).
  8. Telemetria e Logs ao Vivo em tempo real para o Dashboard Admin (enrcl_hd_progress.json).

Uso:
  CHART_CODES=L2 DPI=600 MAX_ZOOM=11 RESAMPLING=cubic TILE_FORMAT=webp python build_enrc_geopdf.py
  CHART_CODES=ALL python build_enrc_geopdf.py
"""

import os
import sys
import json
import time
import math
import shutil
import sqlite3
import tempfile
import subprocess
import requests
try:
    import boto3
except ImportError:
    boto3 = None
from datetime import datetime, timezone
from io import BytesIO

# ─── Configurações Dinâmicas (Injetadas pelo Dashboard / GitHub Actions) ───────

CHART_CODES_ENV = os.environ.get("CHART_CODES", "").strip()
DPI = int(os.environ.get("DPI", 600))
RESAMPLING = os.environ.get("RESAMPLING", "cubic").strip().lower()
TILE_FORMAT = os.environ.get("TILE_FORMAT", "webp").strip().lower()
WEBP_QUALITY = int(os.environ.get("WEBP_QUALITY", 85))
MIN_ZOOM = int(os.environ.get("MIN_ZOOM", 5))
MAX_ZOOM = int(os.environ.get("MAX_ZOOM", 11))
R2_PREFIX = os.environ.get("R2_PREFIX", "enrc/staging").strip().rstrip("/")
PROGRESS_KEY = os.environ.get("PROGRESS_KEY", "enrcl_hd_progress.json").strip()

# 🛡️ Threshold de Expugo SkyFPL:
# Para WebP, um tile 100% transparente tem ~30-45 bytes; tiles de cantos/bordas úteis têm entre 150 e 1600 bytes.
# Para PNG, um tile 100% transparente tem ~334 bytes.
# (O limiar legado de 1700B era exclusivo para descartar PNGs falsos do GeoServer WMS,
# e quando aplicado a WebP gerado via GDAL, expurgava acidentalmente os cantos e bordas da carta).
def get_empty_tile_threshold(tile_format: str) -> int:
    fmt = (tile_format or "").strip().lower()
    return 100 if fmt == "webp" else 350

ENRC_EMPTY_THRESHOLD = get_empty_tile_threshold(TILE_FORMAT)

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")

# ─── Catálogo Oficial e Polígonos das 9 Cartas ENRC L ─────────────────────────

CATALOG_FILE = os.path.join(os.path.dirname(__file__), "enrc_catalog.json")
POLYGONS_FILE = os.path.join(os.path.dirname(__file__), "enrc_official_polygons.json")

def load_official_polygons() -> dict:
    if os.path.exists(POLYGONS_FILE):
        try:
            with open(POLYGONS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[Aviso] Falha ao ler polígonos oficiais ({e})")
    return {}

OFFICIAL_POLYGONS = load_official_polygons()

def fetch_live_enrc_catalog() -> dict:
    """
    Consulta em tempo real o portal AISWEB (inc/cartas/enrc/index.cfm)
    para extrair os links de download vigentes das cartas L1 a L9.
    Garante que atualizações de ciclos AIRAC do DECEA sejam detectadas automaticamente.
    """
    live_urls = {}
    try:
        page_url = "https://aisweb.decea.mil.br/inc/cartas/enrc/index.cfm"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "close"
        }
        res = requests.get(page_url, headers=headers, timeout=25)
        if res.status_code == 200:
            import re
            links = re.findall(r'href=["\']([^"\']*download/\?arquivo=[^"\']+)["\']', res.text)
            for link in links:
                clean_link = link.replace("&amp;", "&")
                m_code = re.search(r'nome=ENRC(?:\+|%20|\s+)(L\d)', clean_link, re.IGNORECASE)
                if m_code:
                    code = m_code.group(1).upper()
                    full_url = clean_link if clean_link.startswith("http") else f"https://aisweb.decea.mil.br/{clean_link.lstrip('../').lstrip('/')}"
                    live_urls[code] = full_url
            if live_urls:
                print(f"[AISWEB] Descoberta dinâmica bem-sucedida! {len(live_urls)} cartas ENRC L encontradas no ciclo vigente.")
    except Exception as e:
        print(f"[Aviso] Falha ao consultar catálogo dinâmico do AISWEB: {e}")
    return live_urls

def load_catalog() -> dict:
    catalog = {}
    if os.path.exists(CATALOG_FILE):
        try:
            with open(CATALOG_FILE, "r", encoding="utf-8") as f:
                catalog = json.load(f)
        except Exception as e:
            print(f"[Aviso] Falha ao ler catálogo local ({e}). Usando fallback integrado...")

    # Fallback canônico baseado nos downloads reais do AISWEB (Ciclo 01/10/2026)
    if not catalog:
        catalog = {
            "L1": {
                "code": "L1", "name": "Região Sul (Porto Alegre, Curitiba, Foz)",
                "layer": "ICA:ENRC_L1", "bbox": [-59.1915, -35.1336, -40.7361, -23.6989],
                "pdf_url": "https://aisweb.decea.mil.br/download/?arquivo=795fea1d-284a-4251-af8a1cb1f722d88c&nome=ENRC L1", "z_order": 2
            },
            "L2": {
                "code": "L2", "name": "Sudeste/Centro (SP/RJ/BSB/BH)",
                "layer": "ICA:ENRC_L2", "bbox": [-46.5628, -24.5372, -30.0679, -14.0602],
                "pdf_url": "https://aisweb.decea.mil.br/download/?arquivo=63b3ad52-f1a4-4d5d-bb881464b7d77c44&nome=ENRC L2", "z_order": 3
            },
            "L3": {
                "code": "L3", "name": "Nordeste Litoral (REC/SSA/FOR/NAT)",
                "layer": "ICA:ENRC_L3", "bbox": [-45.2284, -14.6637, -29.7087, -4.2552],
                "pdf_url": "https://aisweb.decea.mil.br/download/?arquivo=6883fc34-17cb-4987-9b34dc86efbd1eeb&nome=ENRC L3", "z_order": 1
            },
            "L4": {
                "code": "L4", "name": "Nordeste Oceânico (Atlântico / F. Noronha)",
                "layer": "ICA:ENRC_L4", "bbox": [-42.0407, -4.8905, -26.8957, 5.9842],
                "pdf_url": "https://aisweb.decea.mil.br/download/?arquivo=af9bc32e-bd64-4f46-b0656cbc3ad2cc37&nome=ENRC L4", "z_order": 6
            },
            "L5": {
                "code": "L5", "name": "Centro-Oeste Sul (CGR/CGB/Rondonópolis)",
                "layer": "ICA:ENRC_L5", "bbox": [-61.703, -25.4271, -45.139, -14.7391],
                "pdf_url": "https://aisweb.decea.mil.br/download/?arquivo=e50c698a-d32b-4f66-a9e94cf40baeebeb&nome=ENRC L5", "z_order": 4
            },
            "L6": {
                "code": "L6", "name": "Centro/Norte Interior (Porto Nacional / Cachimbo)",
                "layer": "ICA:ENRC_L6", "bbox": [-59.4753, -15.2618, -43.9354, -4.8545],
                "pdf_url": "https://aisweb.decea.mil.br/download/?arquivo=b9e63d21-9d73-4c18-9142bf81227b5279&nome=ENRC L6", "z_order": 5
            },
            "L7": {
                "code": "L7", "name": "Norte Oriental (Belém / Macapá / Santarém)",
                "layer": "ICA:ENRC_L7", "bbox": [-56.2437, -5.4284, -41.1565, 4.9379],
                "pdf_url": "https://aisweb.decea.mil.br/download/?arquivo=6dec370f-aadc-457f-8cdae62d4554d559&nome=ENRC L7", "z_order": 7
            },
            "L8": {
                "code": "L8", "name": "Norte Central (Boa Vista / Manaus)",
                "layer": "ICA:ENRC_L8", "bbox": [-70.9413, -5.3416, -55.8549, 5.5722],
                "pdf_url": "https://aisweb.decea.mil.br/download/?arquivo=3e560047-79d7-4313-abbeeb44b8619613&nome=ENRC L8", "z_order": 8
            },
            "L9": {
                "code": "L9", "name": "Norte Ocidental (Rio Branco / Porto Velho)",
                "layer": "ICA:ENRC_L9", "bbox": [-74.1917, -14.9904, -58.654, -4.0446],
                "pdf_url": "https://aisweb.decea.mil.br/download/?arquivo=03d26d62-d5b4-4b29-a7ba7658036d1d14&nome=ENRC L9", "z_order": 9
            }
        }

    # Atualiza dinamicamente as URLs do catálogo com as URLs vigentes capturadas do portal AISWEB
    live_urls = fetch_live_enrc_catalog()
    for code, live_url in live_urls.items():
        if code in catalog:
            old_url = catalog[code].get("pdf_url")
            if old_url != live_url:
                print(f"[AISWEB] Atualizando URL da carta {code}: {live_url}")
                catalog[code]["pdf_url"] = live_url

    return catalog

# ─── Gerenciador de Telemetria R2 em Tempo Real ───────────────────────────────

class TelemetryManager:
    def __init__(self, s3_client, target_codes: list):
        self.s3 = s3_client
        self.target_codes = target_codes
        self.total = len(target_codes)
        self.logs = []
        self.metadata = {}
        self.status = "processing"
        self.start_time = datetime.now(timezone.utc).isoformat()
        self._load_existing_metadata()
        self.sync_r2()

    def _load_existing_metadata(self):
        if not self.s3:
            return
        try:
            resp = self.s3.get_object(Bucket=R2_BUCKET, Key=PROGRESS_KEY)
            data = json.loads(resp["Body"].read().decode("utf-8"))
            self.metadata = data.get("metadata", {})
        except Exception:
            pass

    def log(self, message: str, current_idx: int = 0, chart_percent: int = 0, level: str = "INFO"):
        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        entry = f"[{now_str}] [{level}] {message}"
        print(entry, flush=True)
        self.logs.append(entry)
        if len(self.logs) > 300:
            self.logs = self.logs[-300:]

        current_code = self.target_codes[current_idx] if current_idx < self.total else ""
        base_progress = (current_idx / self.total) * 100 if self.total > 0 else 0
        step_progress = (chart_percent / 100) * (100 / self.total) if self.total > 0 else 0
        overall_progress = min(100, int(base_progress + step_progress))

        self._save(current_code, overall_progress)

    def chart_completed(self, code: str, size_bytes: int, pmtiles_size: int = 0, actual_min: int = 5, actual_max: int = 11, bounds: str = ""):
        self.metadata[code] = {
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / (1024 * 1024), 2),
            "pmtiles_size_bytes": pmtiles_size,
            "pmtiles_size_mb": round(pmtiles_size / (1024 * 1024), 2) if pmtiles_size else None,
            "min_zoom": actual_min,
            "max_zoom": actual_max,
            "bounds": bounds,
            "r2_url_mbtiles": f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{R2_PREFIX}/{code}_HD.mbtiles",
            "r2_url_pmtiles": f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{R2_PREFIX}/{code}.pmtiles",
            "processed_at": datetime.now(timezone.utc).isoformat()
        }

    def finish(self, status: str = "completed"):
        self.status = status
        self.log(f"Processamento finalizado com status: {status.upper()}!", self.total, 100)
        self._save("", 100)

    def _save(self, current_code: str, progress: int):
        payload = {
            "status": self.status,
            "current_chart": current_code,
            "progress_percent": progress,
            "charts_total": self.total,
            "charts_processed": len([c for c in self.target_codes if c in self.metadata]),
            "started_at": self.start_time,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "dpi": DPI,
            "resampling": RESAMPLING,
            "tile_format": TILE_FORMAT,
            "r2_prefix": R2_PREFIX,
            "logs": self.logs,
            "metadata": self.metadata
        }
        if self.s3:
            try:
                self.s3.put_object(
                    Bucket=R2_BUCKET,
                    Key=PROGRESS_KEY,
                    Body=json.dumps(payload, indent=2).encode("utf-8"),
                    ContentType="application/json",
                    CacheControl="no-cache, no-store"
                )
            except Exception as e:
                print(f"[Telemetria] Aviso R2 put_object: {e}", flush=True)

    def sync_r2(self):
        self._save("", 0)

# ─── Utilitários Cartográficos GDAL ──────────────────────────────────────────

def find_gdal_tool(tool_name: str) -> str:
    """Encontra os utilitários do GDAL no Linux (GitHub Actions) ou Windows (QGIS/OSGeo)."""
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

def download_geopdf(url: str, dest_path: str, telemetry: TelemetryManager, chart_idx: int, max_retries: int = 5) -> bool:
    """Baixa o GeoPDF mestre com session, retries, backoff progressivo para rate-limit e verificação de integridade."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "application/pdf,application/octet-stream,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "close"
    }
    retry_delays = [5, 15, 30, 60, 90]

    session = requests.Session()
    session.headers.update(headers)

    for attempt in range(1, max_retries + 1):
        try:
            telemetry.log(f"Baixando GeoPDF mestre do AISWEB (tentativa {attempt}/{max_retries})...", chart_idx, 8)
            with session.get(url, stream=True, timeout=90, allow_redirects=True) as r:
                if r.status_code == 200:
                    with open(dest_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=128 * 1024):
                            if chunk:
                                f.write(chunk)
                    
                    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000:
                        with open(dest_path, "rb") as f:
                            header = f.read(5)
                        if not header.startswith(b"%PDF"):
                            try:
                                with open(dest_path, "r", errors="ignore") as f_err:
                                    msg = f_err.read(300).replace("\n", " ").strip()
                            except Exception:
                                msg = str(header)
                            telemetry.log(f"Arquivo baixado não é PDF (header={header}). Resposta AISWEB: {msg}", chart_idx, 10, level="WARN")
                            continue
                        size_mb = os.path.getsize(dest_path) / (1024 * 1024)
                        telemetry.log(f"Download GeoPDF concluído com sucesso! Tamanho: {size_mb:.2f} MB", chart_idx, 15)
                        return True
                    else:
                        telemetry.log(f"Arquivo baixado vazio para {url}", chart_idx, 10, level="WARN")
                else:
                    telemetry.log(f"HTTP {r.status_code} ao baixar {url}", chart_idx, 10, level="WARN")
        except Exception as e:
            telemetry.log(f"Aviso/Instabilidade na tentativa {attempt}: {e}", chart_idx, 10, level="WARN")
        
        wait_s = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
        telemetry.log(f"Pausa de segurança ({wait_s}s) para liberação de conexão/rate-limit no AISWEB...", chart_idx, 9, level="WARN")
        time.sleep(wait_s)
    return False

def get_pdf_suppressed_layers(gdalinfo_bin: str, pdf_path: str) -> list:
    """
    Inspeciona o GeoPDF mestre do DECEA e detecta camadas OCG suprimidas/ocultas.
    No GeoPDF oficial do DECEA, rotas descartadas, textos conflitantes e CTAs de regiões vizinhas
    (ex: CTA RECIFE 9 e CTA CURITIBA 3 sobre a carta L2) são alocados em camadas do tipo
    'Labels_SuppressedTexts' e mantidos desligados no leitor de PDF oficial.
    Além disso, na carta L1 (Região Sul), espaços aéreos terminais como FIZ (ex: FIZ Joinville, FIZ Bacacheri),
    ATZ (ex: ATZ Navegantes) e ARC (Área de Rotas de Circulação com molduras pretas) não pertencem a cartas
    de rota enroute de baixa altitude e geram poluição visual severa quando renderizados indiscriminadamente
    pelo GDAL. Esta função identifica e desativa essas camadas no gdalwarp.
    """
    try:
        cmd = [gdalinfo_bin, "-mdd", "LAYERS", pdf_path]
        res = subprocess.run(cmd, capture_output=True, text=True, errors="replace")
        if res.returncode == 0:
            unwanted_patterns = [
                "suppressed",
                "boundary_box",
                ".airspace.fiz",
                ".airspace.atz",
                ".airspace.arc",
                ".labels_texts.fiz",
                ".labels_texts.atz",
                ".labels_texts.arc",
                ".labels_lines.fiz",
                ".labels_lines.atz",
                ".labels_areas.fiz",
                ".labels_areas.atz",
            ]
            off_layers = []
            for line in res.stdout.splitlines():
                if "=" in line and "LAYER_" in line:
                    val = line.split("=", 1)[1].strip()
                    val_lower = val.lower()
                    if any(p in val_lower for p in unwanted_patterns):
                        if val and val not in off_layers:
                            off_layers.append(val)
            return off_layers
    except Exception as e:
        print(f"[Aviso] Falha ao extrair camadas suprimidas do PDF ({e})")
    return []

def process_chart_to_mbtiles(
    code: str,
    chart_info: dict,
    output_mbtiles: str,
    telemetry: TelemetryManager,
    chart_idx: int
) -> bool:
    """Rasteriza o GeoPDF vetorial oficial, aplica corte pela cutline dos 41 vértices e gera MBTiles WebP HD."""
    bbox = chart_info.get("bbox")
    name = chart_info.get("name", code)
    pdf_url = chart_info.get("pdf_url", "").strip()

    telemetry.log(f"Iniciando processamento em alta definição de {code} ({name})...", chart_idx, 5)

    gdalinfo = find_gdal_tool("gdalinfo")
    gdalwarp = find_gdal_tool("gdalwarp")
    gdal_translate = find_gdal_tool("gdal_translate")
    gdaladdo = find_gdal_tool("gdaladdo")

    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Obter arquivo de entrada: GeoPDF mestre exclusivo do AISWEB (NUNCA WMS)
        if not pdf_url:
            telemetry.log(f"ERRO FATAL: Nenhuma URL de GeoPDF configurada para a carta {code}!", chart_idx, 15, level="ERROR")
            return False

        pdf_path = os.path.join(tmpdir, f"{code}.pdf")
        ok_dl = download_geopdf(pdf_url, pdf_path, telemetry, chart_idx)
        if not ok_dl or not os.path.exists(pdf_path):
            telemetry.log(f"ERRO FATAL: Falha ao baixar GeoPDF vetorial oficial de {code}. Abortando processamento (WMS não permitido).", chart_idx, 15, level="ERROR")
            return False

        input_file = pdf_path

        warped_tif = os.path.join(tmpdir, f"{code}_warped.tif")

        # 2. GDALWARP: Rasteriza GeoPDF vetorial (DPI nativo), reprojeta em EPSG:3857 e aplica cutline oficial DECEA
        telemetry.log(f"Renderizando via GDAL ({DPI} DPI, EPSG:3857, Recorte da Moldura Oficial DECEA)...", chart_idx, 25)
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

        # 🛡️ Anti-Poluição OCG: Desliga camadas de textos/rotas suprimidas pelo DECEA
        if input_file.lower().endswith(".pdf"):
            off_layers = get_pdf_suppressed_layers(gdalinfo, input_file)
            if off_layers:
                warp_cmd.extend(["--config", "GDAL_PDF_LAYERS_OFF", ",".join(off_layers)])
                telemetry.log(f"Filtro OCG ativo: {len(off_layers)} camadas suprimidas desligadas (anti-poluição)", chart_idx, 27)

        if code in OFFICIAL_POLYGONS and OFFICIAL_POLYGONS[code].get("coordinates"):
            poly_data = {
                "type": "FeatureCollection",
                "crs": {
                    "type": "name",
                    "properties": {
                        "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
                    }
                },
                "features": [{
                    "type": "Feature",
                    "properties": {"code": code},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": OFFICIAL_POLYGONS[code]["coordinates"]
                    }
                }]
            }
            cutline_path = os.path.join(tmpdir, f"cutline_{code}.geojson")
            with open(cutline_path, "w", encoding="utf-8") as pf:
                json.dump(poly_data, pf)
            warp_cmd.extend(["-cutline", cutline_path, "-crop_to_cutline"])
        elif bbox:
            warp_cmd.extend(["-te", str(bbox[0]), str(bbox[1]), str(bbox[2]), str(bbox[3]), "-te_srs", "EPSG:4326"])

        # Resolução calculada para Web Mercator conforme MAX_ZOOM:
        if MAX_ZOOM >= 12:
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
        telemetry.log(f"Rasterização vetorial e recorte concluídos em {time.time()-t0:.1f}s!", chart_idx, 48)

        # 3. GDAL_TRANSLATE: Converte para MBTiles com compressão WebP
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

        # 3.1. Formato WebP no metadata antes do gdaladdo
        try:
            conn = sqlite3.connect(output_mbtiles)
            cur = conn.cursor()
            cur.execute("INSERT OR REPLACE INTO metadata (name, value) VALUES ('format', ?)", (TILE_FORMAT.lower(),))
            conn.commit()
            conn.close()
        except Exception:
            pass

        # 4. GDALADDO: Pirâmides completas Lanczos de Z5 até MAX_ZOOM
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
        telemetry.log(f"Pirâmides Lanczos concluídas em {time.time()-t2:.1f}s!", chart_idx, 82)

    # 5. Filtragem rigorosa do Threshold de 1700 bytes + Otimização de Metadados
    telemetry.log(f"Aplicando filtro de qualidade SkyFPL (Threshold {ENRC_EMPTY_THRESHOLD}B para {TILE_FORMAT.upper()}) e metadados...", chart_idx, 85)
    try:
        conn = sqlite3.connect(output_mbtiles)
        cur = conn.cursor()

        # Remove tiles menores que o threshold (oceano vazio / nulo / transparente puro)
        cur.execute("DELETE FROM tiles WHERE length(tile_data) < ?", (ENRC_EMPTY_THRESHOLD,))
        purged = cur.rowcount
        if purged > 0:
            telemetry.log(f"🛡️ {purged} tiles vazios (<{ENRC_EMPTY_THRESHOLD}B) expurgados com sucesso!", chart_idx, 87)

        cur.execute("SELECT MIN(zoom_level), MAX(zoom_level), count(*) FROM tiles")
        row = cur.fetchone()
        actual_min, actual_max, total_tiles = row[0] or MIN_ZOOM, row[1] or MAX_ZOOM, row[2] or 0

        # Normaliza metadados sem duplicatas
        cur.execute("DELETE FROM metadata")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata_name ON metadata (name)")

        bounds_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}" if bbox else ""
        cur.execute("""
            INSERT OR REPLACE INTO metadata (name, value) VALUES
            ('name', ?),
            ('type', 'overlay'),
            ('version', '2.2-HD'),
            ('description', ?),
            ('format', ?),
            ('bounds', ?),
            ('minzoom', ?),
            ('maxzoom', ?),
            ('chart_code', ?)
        """, (
            f"SkyFPL ENRC LOW {code}",
            f"Carta de Rota Inferior {code} - {name} (SkyFPL HD GeoPDF Lanczos)",
            TILE_FORMAT.lower(),
            bounds_str,
            str(actual_min),
            str(actual_max),
            code
        ))

        conn.commit()
        # Otimiza espaço em disco após purga
        cur.execute("VACUUM")
        conn.close()

        telemetry.log(f"Indexação concluída: {total_tiles} tiles gravados (Z{actual_min}-Z{actual_max}).", chart_idx, 90)
        return True
    except Exception as e:
        telemetry.log(f"Erro ao auditar banco SQLite: {e}", chart_idx, 88, level="ERROR")
        return False

# ─── Conversão Nativa para PMTiles v3 ─────────────────────────────────────────

def convert_to_pmtiles(local_mbtiles: str, local_pmtiles: str, telemetry: TelemetryManager, chart_idx: int) -> bool:
    """Converte MBTiles para Protomaps PMTiles v3 em frações de segundo."""
    pmtiles_bin = shutil.which("pmtiles") or "/usr/local/bin/pmtiles" or "pmtiles"
    if not os.path.exists(pmtiles_bin) and not shutil.which("pmtiles"):
        telemetry.log("Binário 'pmtiles' não encontrado. Pulando conversão.", chart_idx, 92, level="WARN")
        return False

    telemetry.log("Convertendo para Protomaps PMTiles v3 (Web HTTP Range)...", chart_idx, 92)
    t0 = time.time()
    try:
        cmd = [pmtiles_bin, "convert", local_mbtiles, local_pmtiles]
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        telemetry.log(f"PMTiles gerado com sucesso em {time.time()-t0:.2f}s!", chart_idx, 94)
        return True
    except Exception as e:
        telemetry.log(f"Aviso conversão PMTiles: {e}", chart_idx, 93, level="WARN")
        return False

# ─── Upload Cloudflare R2 ───────────────────────────────────────────────────

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
    print("=" * 70, flush=True)
    print("✈️  SkyFPL ENRC LOW High-Definition Engine (GeoPDF / GDAL) v2.2", flush=True)
    print("=" * 70, flush=True)
    print(f"  DPI de Rasterização: {DPI}", flush=True)
    print(f"  Zoom Alvo: Z{MIN_ZOOM} a Z{MAX_ZOOM}", flush=True)
    print(f"  Algoritmo de Resampling: {RESAMPLING}", flush=True)
    print(f"  Formato dos Tiles: {TILE_FORMAT.upper()} (Qualidade: {WEBP_QUALITY})", flush=True)
    print(f"  Prefixo de Destino R2: {R2_PREFIX}/", flush=True)
    print(f"  Threshold Mandatório: {ENRC_EMPTY_THRESHOLD} bytes", flush=True)
    print(f"  Arquivo de Telemetria: {PROGRESS_KEY}", flush=True)
    print("=" * 70, flush=True)

    catalog = load_catalog()
    if not catalog:
        print("[ERRO FATAL] Não foi possível carregar o catálogo de cartas ENRC.", flush=True)
        sys.exit(1)

    if CHART_CODES_ENV.upper() == "ALL":
        target_codes = [c for c in catalog.keys() if c != "FULL"]
    elif CHART_CODES_ENV.upper() == "FULL":
        target_codes = ["FULL"]
    else:
        requested = [c.strip().upper() for c in CHART_CODES_ENV.split(",") if c.strip()]
        target_codes = [c for c in requested if c in catalog]

    if not target_codes:
        print(f"[ERRO] Nenhum código ENRC válido encontrado em CHART_CODES='{CHART_CODES_ENV}'.", flush=True)
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

    telemetry = TelemetryManager(s3_client, target_codes)
    telemetry.log(f"Iniciando processamento de {len(target_codes)} carta(s): {', '.join(target_codes)}")

    success_count = 0
    with tempfile.TemporaryDirectory() as workdir:
        for idx, code in enumerate(target_codes):
            chart_info = catalog[code]
            telemetry.log(f"--- Processando [{idx+1}/{len(target_codes)}]: {code} ({chart_info['name']}) ---", idx, 5)

            out_mbtiles = os.path.join(workdir, f"{code}_HD.mbtiles")
            out_pmtiles = os.path.join(workdir, f"{code}.pmtiles")

            ok = process_chart_to_mbtiles(code, chart_info, out_mbtiles, telemetry, idx)
            if not ok or not os.path.exists(out_mbtiles):
                telemetry.log(f"Falha ao gerar MBTiles para {code}.", idx, 50, level="ERROR")
                continue

            mbtiles_size = os.path.getsize(out_mbtiles)
            r2_mbtiles_key = f"{R2_PREFIX}/{code}_HD.mbtiles"
            
            if s3_client:
                telemetry.log(f"Enviando MBTiles HD ({mbtiles_size / 1024 / 1024:.2f} MB) para R2...", idx, 90)
                upload_to_r2(s3_client, out_mbtiles, r2_mbtiles_key)

            # Cópia para o cache local do Admin para visualização imediata no Dashboard
            admin_cache = r"c:\Users\josemir\Desktop\skynav-pro-official\admin\.cache\mbtiles"
            if os.path.exists(admin_cache):
                admin_dest = os.path.join(admin_cache, f"enrc_{code}_HD.mbtiles")
                shutil.copy2(out_mbtiles, admin_dest)
                telemetry.log(f"💾 Cópia espelhada salva no cache do Dashboard: {admin_dest}", idx, 92)

            output_dir = os.path.join(os.path.dirname(__file__), "output")
            os.makedirs(output_dir, exist_ok=True)
            local_dest = os.path.join(output_dir, f"{code}_HD.mbtiles")
            shutil.copy2(out_mbtiles, local_dest)
            telemetry.log(f"💾 Cópia local salva em: {local_dest}", idx, 93)

            # Conversão e Upload PMTiles
            pmtiles_ok = convert_to_pmtiles(out_mbtiles, out_pmtiles, telemetry, idx)
            pmtiles_size = 0
            if pmtiles_ok and os.path.exists(out_pmtiles):
                pmtiles_size = os.path.getsize(out_pmtiles)
                r2_pmtiles_key = f"{R2_PREFIX}/{code}.pmtiles"
                if s3_client:
                    telemetry.log(f"Enviando PMTiles ({pmtiles_size / 1024 / 1024:.2f} MB) para R2...", idx, 96)
                    upload_to_r2(s3_client, out_pmtiles, r2_pmtiles_key)

            bbox = chart_info.get("bbox", [])
            bounds_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}" if bbox else ""
            telemetry.chart_completed(code, mbtiles_size, pmtiles_size, MIN_ZOOM, MAX_ZOOM, bounds_str)
            telemetry.log(f"✅ Carta {code} concluída com sucesso a partir do GeoPDF oficial!", idx, 100)
            success_count += 1

    final_status = "completed" if success_count == len(target_codes) else "partial"
    telemetry.finish(final_status)
    print(f"\n[SUCESSO] {success_count}/{len(target_codes)} cartas ENRC processadas.", flush=True)

if __name__ == "__main__":
    main()
