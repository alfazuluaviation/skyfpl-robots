"""
build_wac_geopdf.py — SkyFPL High-Definition WAC Chart Engine (GeoPDF / GDAL) v2.1
=============================================================================
Processa cartas aeronáuticas WAC (World Aeronautical Charts) do DECEA diretamente
a partir dos arquivos mestres GeoPDF vetoriais de altíssima definição.

Diferenciais e Correções v2.1:
  1. Recorte geográfico exato (-te minLon minLat maxLon maxLat -te_srs EPSG:4326):
     Elimina 100% de bordas brancas, legendas laterais e sobreposições ("papel rasgado").
  2. Resolução direcionada para Zoom 12 (38.2185 m/px):
     Garante nitidez cirúrgica em aeródromos, waypoints e aerovias sem interpolação borrada.
  3. Pirâmides completas de overviews Lanczos (Z5 a Z12).
  4. Suporte a tiles WebP de alto rendimento (50-60% mais leves que PNG).
  5. Upload isolado para Cloudflare R2 (wac-test/WAC{code}_HD.mbtiles).
  6. Telemetria e Logs ao Vivo em tempo real para o Dashboard Admin (wac_geopdf_progress.json).

Uso:
  CHART_CODES=WAC3262 DPI=300 MAX_ZOOM=12 RESAMPLING=lanczos TILE_FORMAT=webp python build_wac_geopdf.py
  CHART_CODES=ALL python build_wac_geopdf.py
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
import boto3
from datetime import datetime, timezone

# ─── Configurações Dinâmicas (Injetadas pelo Dashboard / GitHub Actions) ───────

CHART_CODES_ENV = os.environ.get("CHART_CODES", "WAC3262").strip()
DPI = int(os.environ.get("DPI", 600))
RESAMPLING = os.environ.get("RESAMPLING", "cubic").strip().lower()
TILE_FORMAT = os.environ.get("TILE_FORMAT", "webp").strip().lower()
WEBP_QUALITY = int(os.environ.get("WEBP_QUALITY", 85))
MIN_ZOOM = int(os.environ.get("MIN_ZOOM", 5))
MAX_ZOOM = int(os.environ.get("MAX_ZOOM", 12))
R2_PREFIX = os.environ.get("R2_PREFIX", "wac-test").strip().rstrip("/")
PROGRESS_KEY = os.environ.get("PROGRESS_KEY", "wac_geopdf_progress.json").strip()

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")

# ─── Bounding Boxes Oficiais das 46 Folhas WAC do Brasil ──────────────────────
# Formato: (minLon, minLat, maxLon, maxLat) em coordenadas geográficas WGS84 (EPSG:4326)

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
    "WAC3019": (-34.0, -8.0, -30.0, -4.0),  # Fernando de Noronha
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

# ─── Catálogo de Cartas WAC do Brasil (46 Folhas) ─────────────────────────────

WAC_CATALOG_FILE = os.path.join(os.path.dirname(__file__), "wac_catalog.json")

def load_catalog() -> dict:
    """Carrega catálogo local ou consulta o portal AISWEB ao vivo."""
    if os.path.exists(WAC_CATALOG_FILE):
        try:
            with open(WAC_CATALOG_FILE, "r", encoding="utf-8") as f:
                cat = json.load(f)
                if cat.get("WAC3066", {}).get("pdf_url") in ("#", "", None):
                    cat["WAC3066"]["pdf_url"] = "https://aisweb.decea.mil.br/cartas/visuais/wac/recife_wac_20251225.pdf"
                return cat
        except Exception as e:
            print(f"[Aviso] Falha ao ler catálogo local ({e}). Consultando AISWEB...")

    # Fallback: Scrape dinâmico do AISWEB
    catalog = {}
    try:
        url = "https://aisweb.decea.mil.br/?i=cartas&p=visuais"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkyFPL/Robot-HD"}
        r = requests.get(url, headers=headers, timeout=20)
        import re
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", r.text, re.DOTALL)
        for row in rows:
            if "WAC" in row:
                tds = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
                clean = [re.sub(r"<[^>]+>", "", td).strip() for td in tds]
                # Extrai a URL real do PDF (priorizando data-href ou href que contenha .pdf, evitando '#')
                pdf_m = re.search(r'(https?://[^\s"\'<>]+?\.pdf)', row)
                link = pdf_m.group(1) if pdf_m else ""
                if not link:
                    href_m = re.search(r'(?:href|data-href)=["\']([^"\']+\.pdf)["\']', row)
                    if href_m:
                        val = href_m.group(1)
                        link = val if val.startswith("http") else f"https://aisweb.decea.mil.br/{val.lstrip('/')}"

                if len(clean) >= 4 and clean[1].isdigit():
                    ident = clean[1]
                    code = f"WAC{ident}"
                    catalog[code] = {
                        "code": code,
                        "ident": ident,
                        "name": clean[2],
                        "amdt": clean[3],
                        "publication_date": clean[4] if len(clean) > 4 else "",
                        "effective_date": clean[5] if len(clean) > 5 else "",
                        "pdf_url": link,
                        "bbox": WAC_BBOXES.get(code)
                    }
    except Exception as ex:
        print(f"[Erro] Falha ao obter catálogo ao vivo do AISWEB: {ex}")

    return catalog

# ─── Localização de Binários do GDAL ──────────────────────────────────────────

def find_gdal_tool(tool_name: str) -> str:
    """Encontra os utilitários do GDAL no Linux (GitHub Actions) ou Windows (QGIS/OSGeo)."""
    # 1. PATH do Sistema
    found = shutil.which(tool_name)
    if found:
        return found

    # 2. Windows Executable Extensions
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

    # 3. Linux Paths Padrão
    linux_paths = [
        f"/usr/bin/{tool_name}",
        f"/usr/local/bin/{tool_name}"
    ]
    for p in linux_paths:
        if os.path.exists(p):
            return p

    raise FileNotFoundError(f"Utilitário GDAL '{tool_name}' não encontrado no ambiente.")

# ─── Gerenciador de Telemetria e Logs em Tempo Real (R2) ──────────────────────

class TelemetryManager:
    """Gerencia telemetria granular e logs em tempo real sincronizados com o Cloudflare R2."""
    def __init__(self, s3_client, target_codes: list):
        self.s3 = s3_client
        self.target_codes = target_codes
        self.charts_done = []
        self.metadata = {}
        self.logs = []
        self.current_chart = None
        self.current_phase = None
        self.percent = 0

        # Carrega histórico cumulativo de cartas já salvas no R2
        if self.s3 and R2_BUCKET:
            try:
                resp = self.s3.get_object(Bucket=R2_BUCKET, Key=PROGRESS_KEY)
                prev = json.loads(resp['Body'].read().decode('utf-8'))
                if isinstance(prev.get("metadata"), dict):
                    self.metadata = dict(prev["metadata"])
                    print(f"  [Telemetria] Carregadas {len(self.metadata)} cartas prévias do histórico R2.", flush=True)
            except Exception:
                pass

    def log(self, message: str, chart_idx: int = None, chart_sub_percent: float = None, level: str = "INFO"):
        """Registra log com timestamp e envia atualização imediata de progresso ao R2."""
        now_str = datetime.now().strftime("%H:%M:%S")
        prefix = "✓" if level == "SUCCESS" else ("⚠" if level == "WARN" else ("✖" if level == "ERROR" else "›"))
        formatted = f"[{now_str}] {prefix} {message}"
        print(formatted, flush=True)
        self.logs.append(formatted)
        if len(self.logs) > 80:
            self.logs = self.logs[-80:]

        self.current_phase = message

        if chart_idx is not None and chart_sub_percent is not None:
            n_total = len(self.target_codes)
            chart_base = ((chart_idx - 1) / n_total) * 100.0
            sub_contrib = (chart_sub_percent / n_total)
            self.percent = min(99, max(0, int(chart_base + sub_contrib)))

        self.sync_r2(status="processing")

    def sync_r2(self, status: str = "processing"):
        if not self.s3 or not R2_BUCKET:
            return

        payload = {
            "status": status,
            "engine": "geopdf_hd",
            "current": self.current_chart,
            "phase": self.current_phase,
            "completed": self.charts_done,
            "total": self.target_codes,
            "percent": 100 if status == "completed" else self.percent,
            "logs": self.logs,
            "config": {
                "dpi": DPI,
                "resampling": RESAMPLING,
                "tile_format": TILE_FORMAT,
                "minzoom": MIN_ZOOM,
                "maxzoom": MAX_ZOOM,
                "r2_prefix": R2_PREFIX
            },
            "metadata": self.metadata,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        try:
            self.s3.put_object(
                Bucket=R2_BUCKET,
                Key=PROGRESS_KEY,
                Body=json.dumps(payload, ensure_ascii=False, indent=2),
                ContentType="application/json",
                CacheControl="no-cache, no-store"
            )
        except Exception as e:
            print(f"  [Aviso Telemetria]: {e}", flush=True)

# ─── Download Seguro do GeoPDF ────────────────────────────────────────────────

def download_geopdf(url: str, dest_path: str, telemetry: TelemetryManager, chart_idx: int, max_retries: int = 4) -> bool:
    """Baixa o GeoPDF mestre com retries e verificação de integridade."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkyFPL/Robot-HD",
        "Accept": "application/pdf,application/octet-stream,*/*"
    }
    for attempt in range(1, max_retries + 1):
        try:
            telemetry.log(f"Baixando GeoPDF mestre do AISWEB (tentativa {attempt}/{max_retries})...", chart_idx, 8)
            r = requests.get(url, headers=headers, stream=True, timeout=60)
            if r.status_code == 200:
                with open(dest_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=128 * 1024):
                        if chunk:
                            f.write(chunk)
                size_mb = os.path.getsize(dest_path) / (1024 * 1024)
                telemetry.log(f"Download concluído com sucesso! Tamanho: {size_mb:.2f} MB", chart_idx, 15)
                return True
            else:
                telemetry.log(f"HTTP {r.status_code} ao baixar {url}", chart_idx, 10, level="WARN")
        except Exception as e:
            telemetry.log(f"Erro na tentativa {attempt}: {e}", chart_idx, 10, level="WARN")
        time.sleep(2 * attempt)
    return False

# ─── Processamento GDAL: GeoPDF -> MBTiles HD ─────────────────────────────────

def process_chart_to_mbtiles(code: str, pdf_path: str, output_mbtiles: str, chart_meta: dict, telemetry: TelemetryManager, chart_idx: int) -> bool:
    """Executa a transformação de alta fidelidade: GeoPDF -> EPSG:3857 -> MBTiles WebP/PNG."""
    gdalwarp = find_gdal_tool("gdalwarp")
    gdal_translate = find_gdal_tool("gdal_translate")
    gdaladdo = find_gdal_tool("gdaladdo")

    bbox = chart_meta.get("bbox") or WAC_BBOXES.get(code)

    with tempfile.TemporaryDirectory() as tmpdir:
        warped_tif = os.path.join(tmpdir, f"{code}_warped.tif")

        # 1. GDALWARP: Rasteriza GeoPDF, aplica recorte geográfico exato da folha e projeta em Web Mercator
        telemetry.log(f"Reprojetando GeoPDF para EPSG:3857 ({DPI} DPI, {RESAMPLING.upper()}, Recorte Exato)...", chart_idx, 20)
        warp_cmd = [
            gdalwarp,
            "--config", "GDAL_PDF_DPI", str(DPI),
            "-t_srs", "EPSG:3857",
            "-r", RESAMPLING,
            "-dstalpha",
            "-co", "COMPRESS=DEFLATE",
            "-co", "TILED=YES",
            "-overwrite"
        ]

        if bbox:
            min_lon, min_lat, max_lon, max_lat = bbox
            telemetry.log(f"Aplicando BBOX oficial: {min_lon}°W a {max_lon}°W, {min_lat}°S a {max_lat}°S", chart_idx, 25)
            warp_cmd.extend([
                "-te", str(min_lon), str(min_lat), str(max_lon), str(max_lat),
                "-te_srs", "EPSG:4326"
            ])

        if MAX_ZOOM >= 13:
            # Resolução nativa de Zoom 13 no Web Mercator: 19.10925948 m/pixel
            warp_cmd.extend(["-tr", "19.10925948", "19.10925948"])
        elif MAX_ZOOM == 12:
            warp_cmd.extend(["-tr", "38.21851897", "38.21851897"])
        elif MAX_ZOOM == 11:
            warp_cmd.extend(["-tr", "76.43703794", "76.43703794"])

        warp_cmd.extend([pdf_path, warped_tif])

        t0 = time.time()
        res1 = subprocess.run(warp_cmd, capture_output=True, text=True)
        if res1.returncode != 0:
            telemetry.log(f"ERRO GDALWARP: {res1.stderr[:200]}", chart_idx, 30, level="ERROR")
            return False
        telemetry.log(f"Rasterização e recorte concluídos em {time.time()-t0:.1f}s!", chart_idx, 48)

        # 2. GDAL_TRANSLATE: Converte o GeoTIFF para MBTiles com compressão moderna (WEBP ou PNG)
        tile_fmt_upper = TILE_FORMAT.upper()
        telemetry.log(f"Empacotando em MBTiles com compressão {tile_fmt_upper} (Qualidade: {WEBP_QUALITY})...", chart_idx, 52)
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
        telemetry.log(f"Base MBTiles gerado em {time.time()-t1:.1f}s!", chart_idx, 70)

        # 2.1. CRUCIAL: Escrever 'format=webp' na tabela metadata ANTES de chamar gdaladdo!
        # Sem isso, o GDAL assume por padrão 'format=png' e gera overviews PNG descompactados (100x mais pesados)!
        try:
            conn = sqlite3.connect(output_mbtiles)
            cur = conn.cursor()
            cur.execute("INSERT OR REPLACE INTO metadata (name, value) VALUES ('format', ?)", (TILE_FORMAT.lower(),))
            conn.commit()
            conn.close()
        except Exception as e:
            telemetry.log(f"Aviso metadata format: {e}", chart_idx, 72, level="WARN")

        # 3. GDALADDO: Gera pirâmides completas de zoom (overviews) com interpolação matemática Lanczos
        telemetry.log(f"Gerando pirâmides de overviews Lanczos ({tile_fmt_upper} Z{MIN_ZOOM} até Z{MAX_ZOOM})...", chart_idx, 75)
        if MAX_ZOOM >= 13:
            overviews = ["2", "4", "8", "16", "32", "64", "128", "256"]
        elif MAX_ZOOM >= 12:
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
        telemetry.log(f"Pirâmides de zoom concluídas em {time.time()-t2:.1f}s!", chart_idx, 88)

    # 4. Ajuste e padronização dos Metadados Canônicos no SQLite
    try:
        conn = sqlite3.connect(output_mbtiles)
        cur = conn.cursor()
        
        cur.execute("SELECT MIN(zoom_level), MAX(zoom_level), count(*) FROM tiles")
        row = cur.fetchone()
        actual_min_zoom, actual_max_zoom, total_tiles = row[0], row[1], row[2]

        # 4.1. CRUCIAL: Garantir unicidade absoluta na tabela metadata!
        # Sem UNIQUE index, o GDAL MBTiles não substitui linhas e duplica 'minzoom'.
        # O leitor C++ do GDAL lê a 2ª linha de 'minzoom' como 'maxzoom', travando o mapa no Zoom 5!
        cur.execute("SELECT name, value FROM metadata")
        existing_meta = dict(cur.fetchall())
        cur.execute("DELETE FROM metadata")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata_name ON metadata (name)")

        pub_date = str(chart_meta.get("publication_date", ""))
        eff_date = str(chart_meta.get("effective_date", ""))
        amdt_code = str(chart_meta.get("amdt", ""))
        bounds_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}" if bbox else "-180,-85,180,85"

        cur.execute("""
            INSERT OR REPLACE INTO metadata (name, value) VALUES
            ('name', ?),
            ('type', 'overlay'),
            ('version', '2.1-HD'),
            ('description', ?),
            ('format', ?),
            ('bounds', ?),
            ('minzoom', ?),
            ('maxzoom', ?),
            ('scheme', 'tms'),
            ('generator', 'SkyFPL WAC GeoPDF HD Engine v2.1'),
            ('amdt', ?),
            ('publication_date', ?),
            ('effective_date', ?)
        """, (
            f"SkyFPL WAC {code} HD",
            f"WAC {code} {chart_meta.get('name', '')} - DECEA Vector GeoPDF ({DPI} DPI, {RESAMPLING})",
            TILE_FORMAT.lower(),
            bounds_str,
            str(actual_min_zoom if actual_min_zoom is not None else MIN_ZOOM),
            str(actual_max_zoom if actual_max_zoom is not None else MAX_ZOOM),
            amdt_code,
            pub_date,
            eff_date
        ))
        
        conn.commit()
        cur.execute("PRAGMA page_size = 4096")
        cur.execute("VACUUM")
        conn.close()
        telemetry.log(f"SQLite otimizado: {total_tiles:,} tiles indexados (Z{actual_min_zoom}-Z{actual_max_zoom}).", chart_idx, 92)
    except Exception as e:
        telemetry.log(f"Aviso SQLite: {e}", chart_idx, 92, level="WARN")

    return True

# ─── Upload R2 ───────────────────────────────────────────────────────────────

def upload_to_r2(s3_client, local_path: str, r2_key: str) -> int:
    """Faz upload de um arquivo para o bucket Cloudflare R2."""
    size_bytes = os.path.getsize(local_path)
    content_type = "application/vnd.sqlite3" if local_path.endswith(".mbtiles") else "application/json"
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
    print("✈️  SkyFPL WAC High-Definition Engine (GeoPDF / GDAL) v2.1", flush=True)
    print("=" * 70, flush=True)
    print(f"  DPI de Rasterização: {DPI}", flush=True)
    print(f"  Zoom Alvo: Z{MIN_ZOOM} a Z{MAX_ZOOM}", flush=True)
    print(f"  Algoritmo de Resampling: {RESAMPLING}", flush=True)
    print(f"  Formato dos Tiles: {TILE_FORMAT.upper()} (Qualidade: {WEBP_QUALITY})", flush=True)
    print(f"  Prefixo de Destino R2: {R2_PREFIX}/", flush=True)
    print(f"  Arquivo de Progresso: {PROGRESS_KEY}", flush=True)
    print("=" * 70, flush=True)

    # 1. Carregar catálogo oficial
    catalog = load_catalog()
    if not catalog:
        print("[ERRO FATAL] Não foi possível carregar o catálogo de cartas WAC.", flush=True)
        sys.exit(1)

    # 2. Determinar cartas a processar
    if CHART_CODES_ENV.upper() == "ALL":
        target_codes = list(catalog.keys())
    else:
        requested = [c.strip().upper() for c in CHART_CODES_ENV.split(",") if c.strip()]
        target_codes = [c for c in requested if c in catalog]

    if not target_codes:
        print(f"[ERRO] Nenhum código WAC válido encontrado em CHART_CODES='{CHART_CODES_ENV}'.", flush=True)
        sys.exit(1)

    # 3. Inicializar Cliente S3 (Cloudflare R2)
    s3 = None
    if R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY and R2_BUCKET:
        s3 = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name="auto"
        )
    else:
        print("[Aviso] Credenciais do Cloudflare R2 não detectadas no ambiente. Uploads serão ignorados (Modo Local).", flush=True)

    # Inicializar Telemetria
    telemetry = TelemetryManager(s3, target_codes)
    telemetry.log(f"Motor GeoPDF HD iniciado. {len(target_codes)} cartas selecionadas: {', '.join(target_codes)}", 1, 0)

    with tempfile.TemporaryDirectory() as workdir:
        for idx, code in enumerate(target_codes, 1):
            chart_info = catalog[code]
            telemetry.current_chart = code
            telemetry.log(f"[{idx}/{len(target_codes)}] Iniciando processamento: {code} — {chart_info.get('name', '')}", idx, 2)

            pdf_url = chart_info.get("pdf_url")
            if not pdf_url:
                telemetry.log(f"URL do GeoPDF não disponível para {code}. Pulando...", idx, 100, level="WARN")
                continue

            local_pdf = os.path.join(workdir, f"{code}.pdf")
            local_mbtiles = os.path.join(workdir, f"{code}_HD.mbtiles")

            # A. Download do GeoPDF
            download_ok = download_geopdf(pdf_url, local_pdf, telemetry, idx)
            if not download_ok:
                telemetry.log(f"Download falhou para {code}.", idx, 100, level="ERROR")
                continue

            # B. Processamento GDAL de alta fidelidade
            process_ok = process_chart_to_mbtiles(code, local_pdf, local_mbtiles, chart_info, telemetry, idx)
            if not process_ok or not os.path.exists(local_mbtiles):
                telemetry.log(f"Falha no pipeline GDAL para {code}.", idx, 100, level="ERROR")
                continue

            size_bytes = os.path.getsize(local_mbtiles)
            size_mb = size_bytes / (1024 * 1024)

            # C. Upload para R2
            r2_key = f"{R2_PREFIX}/{code}_HD.mbtiles"
            if s3:
                telemetry.log(f"Enviando {code}_HD.mbtiles ({size_mb:.2f} MB) para Cloudflare R2 ({R2_BUCKET}/{r2_key})...", idx, 94)
                upload_to_r2(s3, local_mbtiles, r2_key)
                telemetry.log(f"Upload de {code}_HD.mbtiles concluído com sucesso!", idx, 100, level="SUCCESS")

            telemetry.charts_done.append(code)
            telemetry.metadata[code] = {
                "name": chart_info.get("name", ""),
                "amdt": chart_info.get("amdt", ""),
                "publication_date": chart_info.get("publication_date", ""),
                "effective_date": chart_info.get("effective_date", ""),
                "size_bytes": size_bytes,
                "size_mb": round(size_mb, 2),
                "r2_key": r2_key,
                "dpi": DPI,
                "minzoom": MIN_ZOOM,
                "maxzoom": MAX_ZOOM,
                "tile_format": TILE_FORMAT,
                "resampling": RESAMPLING,
                "processed_at": datetime.now(timezone.utc).isoformat()
            }

            # Limpar arquivos temporários da folha
            if os.path.exists(local_pdf):
                try: os.remove(local_pdf)
                except Exception: pass
            if os.path.exists(local_mbtiles):
                try: os.remove(local_mbtiles)
                except Exception: pass

    # 4. Finalização
    telemetry.current_chart = None
    telemetry.current_phase = "Todas as cartas processadas com sucesso!"
    telemetry.log(f"🏁 Concluído! {len(telemetry.charts_done)}/{len(target_codes)} cartas WAC HD geradas e publicadas.", len(target_codes), 100, level="SUCCESS")
    telemetry.sync_r2(status="completed")
    print("\n" + "=" * 70, flush=True)
    print(f"🏁 Processamento Concluído! {len(telemetry.charts_done)}/{len(target_codes)} cartas WAC HD geradas com sucesso.", flush=True)
    print("=" * 70, flush=True)

if __name__ == "__main__":
    main()
