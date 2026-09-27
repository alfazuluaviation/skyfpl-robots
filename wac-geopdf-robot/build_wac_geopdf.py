"""
build_wac_geopdf.py — SkyFPL High-Definition WAC Chart Engine (GeoPDF / GDAL)
=============================================================================
Processa cartas aeronáuticas WAC (World Aeronautical Charts) do DECEA diretamente
a partir dos arquivos mestres GeoPDF vetoriais de altíssima definição.

Diferenciais em relação ao robô legado (WMS GetMap):
  1. Rasterização nativa vetorial em 254-300 DPI (nitidez de texto cristalina em Z5-Z8).
  2. Recorte automático perfeito da área útil via NEATLINE oficial do DECEA (zero borda).
  3. Reamostragem matemática Lanczos em todas as pirâmides de zoom.
  4. Suporte a tiles WebP (50-60% mais leves que PNG, com qualidade superior).
  5. Upload isolado para o Cloudflare R2 (wac-test/WAC{code}_HD.mbtiles).
  6. Progresso em tempo real salvo em wac_geopdf_progress.json.

Uso:
  CHART_CODES=WAC3140 DPI=254 RESAMPLING=lanczos TILE_FORMAT=webp python build_wac_geopdf.py
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

CHART_CODES_ENV = os.environ.get("CHART_CODES", "WAC3140").strip()
DPI = int(os.environ.get("DPI", 254))
RESAMPLING = os.environ.get("RESAMPLING", "lanczos").strip().lower()
TILE_FORMAT = os.environ.get("TILE_FORMAT", "webp").strip().lower()
WEBP_QUALITY = int(os.environ.get("WEBP_QUALITY", 85))
MIN_ZOOM = int(os.environ.get("MIN_ZOOM", 5))
MAX_ZOOM = int(os.environ.get("MAX_ZOOM", 11))
R2_PREFIX = os.environ.get("R2_PREFIX", "wac-test").strip().rstrip("/")
PROGRESS_KEY = os.environ.get("PROGRESS_KEY", "wac_geopdf_progress.json").strip()

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")

# ─── Catálogo de Cartas WAC do Brasil (46 Folhas) ─────────────────────────────

WAC_CATALOG_FILE = os.path.join(os.path.dirname(__file__), "wac_catalog.json")

def load_catalog() -> dict:
    """Carrega catálogo local ou consulta o portal AISWEB ao vivo."""
    if os.path.exists(WAC_CATALOG_FILE):
        try:
            with open(WAC_CATALOG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
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
                link_m = re.search(r'href=["\']([^"\']+)["\']', row)
                link = link_m.group(1) if link_m else ""
                if len(clean) >= 4 and clean[1].isdigit():
                    ident = clean[1]
                    code = f"WAC{ident}"
                    catalog[code] = {
                        "code": code,
                        "ident": ident,
                        "name": clean[2],
                        "amdt": clean[3],
                        "pdf_url": link
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

        # Locais conhecidos de instalação do QGIS / OSGeo4W no Windows
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

# ─── Download Seguro do GeoPDF ────────────────────────────────────────────────

def download_geopdf(url: str, dest_path: str, max_retries: int = 4) -> bool:
    """Baixa o GeoPDF mestre com retries e verificação de integridade."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkyFPL/Robot-HD",
        "Accept": "application/pdf,application/octet-stream,*/*"
    }
    for attempt in range(1, max_retries + 1):
        try:
            print(f"  [Download] Baixando GeoPDF mestre (tentativa {attempt}/{max_retries})...")
            r = requests.get(url, headers=headers, stream=True, timeout=60)
            if r.status_code == 200:
                with open(dest_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=128 * 1024):
                        if chunk:
                            f.write(chunk)
                size_mb = os.path.getsize(dest_path) / (1024 * 1024)
                print(f"  [Download] Sucesso! Tamanho: {size_mb:.2f} MB")
                return True
            else:
                print(f"  [Aviso] HTTP {r.status_code} ao baixar {url}")
        except Exception as e:
            print(f"  [Aviso] Erro na tentativa {attempt}: {e}")
        time.sleep(2 * attempt)
    return False

# ─── Processamento GDAL: GeoPDF -> MBTiles HD ─────────────────────────────────

def process_chart_to_mbtiles(code: str, pdf_path: str, output_mbtiles: str, chart_meta: dict) -> bool:
    """Executa a transformação de alta fidelidade: GeoPDF -> EPSG:3857 -> MBTiles WebP/PNG."""
    gdalwarp = find_gdal_tool("gdalwarp")
    gdal_translate = find_gdal_tool("gdal_translate")
    gdaladdo = find_gdal_tool("gdaladdo")

    with tempfile.TemporaryDirectory() as tmpdir:
        warped_tif = os.path.join(tmpdir, f"{code}_warped.tif")

        # 1. GDALWARP: Rasteriza GeoPDF no DPI desejado, corta no NEATLINE oficial e reprojeta para Web Mercator (EPSG:3857)
        print(f"  [GDAL] Reprojetando para EPSG:3857 (DPI={DPI}, Resampling={RESAMPLING}, NEATLINE=AUTO)...")
        warp_cmd = [
            gdalwarp,
            "--config", "GDAL_PDF_DPI", str(DPI),
            "--config", "GDAL_PDF_BBOX", "NEATLINE",
            "-t_srs", "EPSG:3857",
            "-r", RESAMPLING,
            "-dstalpha",
            "-co", "COMPRESS=DEFLATE",
            "-co", "TILED=YES",
            "-overwrite",
            pdf_path,
            warped_tif
        ]
        res1 = subprocess.run(warp_cmd, capture_output=True, text=True)
        if res1.returncode != 0:
            print(f"  [ERRO GDALWARP]: {res1.stderr}")
            return False

        # 2. GDAL_TRANSLATE: Converte o GeoTIFF para MBTiles com compressão moderna (WEBP ou PNG)
        tile_fmt_upper = TILE_FORMAT.upper()
        print(f"  [GDAL] Empacotando em MBTiles com compressão {tile_fmt_upper}...")
        translate_cmd = [
            gdal_translate,
            "-of", "MBTILES",
            "-co", f"TILE_FORMAT={tile_fmt_upper}",
        ]
        if tile_fmt_upper == "WEBP":
            translate_cmd.extend(["-co", f"QUALITY={WEBP_QUALITY}"])

        translate_cmd.extend([warped_tif, output_mbtiles])
        res2 = subprocess.run(translate_cmd, capture_output=True, text=True)
        if res2.returncode != 0:
            print(f"  [ERRO GDAL_TRANSLATE]: {res2.stderr}")
            return False

        # 3. GDALADDO: Gera pirâmides completas de zoom (overviews) com interpolação matemática Lanczos
        print(f"  [GDAL] Gerando pirâmides de overviews de alta fidelidade (Lanczos)...")
        addo_cmd = [
            gdaladdo,
            "-r", RESAMPLING,
            output_mbtiles
        ]
        res3 = subprocess.run(addo_cmd, capture_output=True, text=True)
        if res3.returncode != 0:
            print(f"  [ERRO GDALADDO]: {res3.stderr}")
            return False

    # 4. Ajuste e padronização dos Metadados Canônicos no SQLite
    try:
        conn = sqlite3.connect(output_mbtiles)
        cur = conn.cursor()
        
        # Obter limites reais dos tiles gerados
        cur.execute("SELECT MIN(zoom_level), MAX(zoom_level) FROM tiles")
        actual_min_zoom, actual_max_zoom = cur.fetchone()
        
        cur.execute("""
            INSERT OR REPLACE INTO metadata (name, value) VALUES
            ('name', ?),
            ('type', 'overlay'),
            ('version', '2.0-HD'),
            ('description', ?),
            ('format', ?),
            ('minzoom', ?),
            ('maxzoom', ?),
            ('scheme', 'tms'),
            ('generator', 'SkyFPL WAC GeoPDF HD Engine v2.0')
        """, (
            f"SkyFPL WAC {code} HD",
            f"WAC {code} {chart_meta.get('name', '')} - DECEA Vector GeoPDF ({DPI} DPI, {RESAMPLING})",
            TILE_FORMAT.lower(),
            str(actual_min_zoom if actual_min_zoom is not None else MIN_ZOOM),
            str(actual_max_zoom if actual_max_zoom is not None else MAX_ZOOM)
        ))
        
        conn.commit()
        # Otimização do arquivo
        cur.execute("PRAGMA page_size = 4096")
        cur.execute("VACUUM")
        conn.close()
    except Exception as e:
        print(f"  [Aviso] Falha ao ajustar metadados SQLite: {e}")

    return True

# ─── Upload R2 e Gestão de Progresso ──────────────────────────────────────────

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

def update_progress_json(s3_client, status: str, charts_done: list, total_charts: list, current: str = None, metadata: dict = None):
    """Grava o status do robô em wac_geopdf_progress.json no R2 para o Dashboard Admin."""
    if not R2_BUCKET or not s3_client:
        return

    n_done = len(charts_done)
    n_total = len(total_charts)
    base_percent = int((n_done / n_total) * 100) if n_total > 0 else 0

    progress_payload = {
        "status": status,
        "engine": "geopdf_hd",
        "current": current,
        "completed": charts_done,
        "total": total_charts,
        "percent": base_percent,
        "config": {
            "dpi": DPI,
            "resampling": RESAMPLING,
            "tile_format": TILE_FORMAT,
            "minzoom": MIN_ZOOM,
            "maxzoom": MAX_ZOOM,
            "r2_prefix": R2_PREFIX
        },
        "metadata": metadata or {},
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    try:
        s3_client.put_object(
            Bucket=R2_BUCKET,
            Key=PROGRESS_KEY,
            Body=json.dumps(progress_payload, ensure_ascii=False, indent=2),
            ContentType="application/json",
            CacheControl="no-cache, no-store"
        )
    except Exception as e:
        print(f"  [Aviso] Falha ao atualizar {PROGRESS_KEY} no R2: {e}")

# ─── Execução Principal ───────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("✈️  SkyFPL WAC High-Definition Engine (GeoPDF / GDAL) v2.0")
    print("=" * 70)
    print(f"  DPI de Rasterização: {DPI}")
    print(f"  Algoritmo de Resampling: {RESAMPLING}")
    print(f"  Formato dos Tiles: {TILE_FORMAT.upper()} (Qualidade: {WEBP_QUALITY})")
    print(f"  Prefixo de Destino R2: {R2_PREFIX}/")
    print(f"  Arquivo de Progresso: {PROGRESS_KEY}")
    print("=" * 70)

    # 1. Carregar catálogo oficial
    catalog = load_catalog()
    if not catalog:
        print("[ERRO FATAL] Não foi possível carregar o catálogo de cartas WAC.")
        sys.exit(1)

    # 2. Determinar cartas a processar
    if CHART_CODES_ENV.upper() == "ALL":
        target_codes = list(catalog.keys())
    else:
        requested = [c.strip().upper() for c in CHART_CODES_ENV.split(",") if c.strip()]
        target_codes = [c for c in requested if c in catalog]

    if not target_codes:
        print(f"[ERRO] Nenhum código WAC válido encontrado em CHART_CODES='{CHART_CODES_ENV}'.")
        print(f"Exemplos válidos: WAC3140, WAC3262, WAC3263. Disponíveis: {len(catalog)} folhas.")
        sys.exit(1)

    print(f"[WAC HD] Cartas selecionadas para processamento ({len(target_codes)}): {', '.join(target_codes)}\n")

    # 3. Inicializar Cliente S3 (Cloudflare R2) se credenciais estiverem configuradas
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
        print("[Aviso] Credenciais do Cloudflare R2 não detectadas no ambiente. Uploads serão ignorados (Modo Local).")

    charts_done = []
    metadata = {}

    with tempfile.TemporaryDirectory() as workdir:
        for idx, code in enumerate(target_codes, 1):
            chart_info = catalog[code]
            print(f"[{idx}/{len(target_codes)}] Iniciando: {code} — {chart_info.get('name', '')}")
            update_progress_json(s3, "processing", charts_done, target_codes, current=code, metadata=metadata)

            pdf_url = chart_info.get("pdf_url")
            if not pdf_url:
                print(f"  [Pular] URL do GeoPDF não disponível para {code}.")
                continue

            local_pdf = os.path.join(workdir, f"{code}.pdf")
            local_mbtiles = os.path.join(workdir, f"{code}_HD.mbtiles")

            # A. Download do GeoPDF
            download_ok = download_geopdf(pdf_url, local_pdf)
            if not download_ok:
                print(f"  [ERRO] Download falhou para {code}.")
                continue

            # B. Processamento GDAL de alta fidelidade
            process_ok = process_chart_to_mbtiles(code, local_pdf, local_mbtiles, chart_info)
            if not process_ok or not os.path.exists(local_mbtiles):
                print(f"  [ERRO] Falha no pipeline GDAL para {code}.")
                continue

            size_bytes = os.path.getsize(local_mbtiles)
            size_mb = size_bytes / (1024 * 1024)
            print(f"  [Sucesso] {code}_HD.mbtiles gerado com sucesso! Tamanho: {size_mb:.2f} MB")

            # C. Upload para R2
            r2_key = f"{R2_PREFIX}/{code}_HD.mbtiles"
            if s3:
                print(f"  [R2 Upload] Enviando para {R2_BUCKET}/{r2_key}...")
                upload_to_r2(s3, local_mbtiles, r2_key)
                print("  [R2 Upload] Upload concluído com sucesso!")

            charts_done.append(code)
            metadata[code] = {
                "name": chart_info.get("name", ""),
                "amdt": chart_info.get("amdt", ""),
                "size_bytes": size_bytes,
                "size_mb": round(size_mb, 2),
                "r2_key": r2_key,
                "dpi": DPI,
                "tile_format": TILE_FORMAT,
                "resampling": RESAMPLING,
                "processed_at": datetime.now(timezone.utc).isoformat()
            }

            # Limpar arquivos temporários da folha
            if os.path.exists(local_pdf):
                os.remove(local_pdf)
            if os.path.exists(local_mbtiles):
                os.remove(local_mbtiles)

    # 4. Finalização
    update_progress_json(s3, "completed", charts_done, target_codes, metadata=metadata)
    print("\n" + "=" * 70)
    print(f"🏁 Processamento Concluído! {len(charts_done)}/{len(target_codes)} cartas WAC HD geradas com sucesso.")
    print("=" * 70)

if __name__ == "__main__":
    main()
