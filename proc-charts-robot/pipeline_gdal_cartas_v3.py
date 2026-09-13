"""
================================================================================
PIPELINE GDAL — Conversão GeoPDF → JPEG 200 DPI + Sidecar JSON
Versão 3.0.0 — Versão final corrigida (Antigravity + IA parceira)
================================================================================
Projeto : EFB - Cartas de Procedimento AIS Brasil
Objetivo: Converter GeoPDFs da DECEA em JPEG de alta qualidade para exibição
          instantânea no app, preservando metadados geográficos (GCPs) em
          arquivo sidecar JSON para georreferenciamento (Fase 2).

Dependências:
    pip install gdal numpy Pillow tqdm

Uso:
    python pipeline_gdal_cartas_v3.py --input ./pdfs --output ./output
    python pipeline_gdal_cartas_v3.py --input ./pdfs --output ./output --workers 8

Changelog v3.0.0 (sobre v2.0.0):
    Fix R1 — Eliminada dupla codificação JPEG lossy→lossy via vsimem (TIFF intermediário
             em memória). Pillow agora lê pixels sem compressão e salva JPEG uma única vez.
    Fix R2 — Subsampling alterado de 4:2:0 → 4:4:4 para máxima fidelidade em textos
             finos e anotações de procedimentos aeronáuticos (frequências, altitudes, etc.)
    Fix R3 — DPI ajustado de 300 → 200. Qualidade JPEG ajustada de 85 → 90.
             Estimativa real: ~0.9–1.5 MB/carta | ~1.6–2.7 GB total para 1829 cartas.

Bugs herdados da v2.0.0 (mantidos corretamente):
    Fix #1 — OpenEx com open_options por chamada (sem SetConfigOption global)
    Fix #2 — GDAL_PDF_LIB=INTERNAL forçado via os.environ
    Fix #3 — corners via GCPsToGeoTransform (transformação afim real)
================================================================================
"""

import os
import json
import logging
import argparse
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
from PIL import Image
from tqdm import tqdm

try:
    from osgeo import gdal, osr
    gdal.UseExceptions()
except ImportError:
    raise ImportError("GDAL não encontrado. Execute: pip install gdal")


# ─────────────────────────────────────────────────────────────────────────────
# FIX #2 — Driver INTERNAL definido antes de qualquer gdal.Open()
# Herdado por todos os processos filhos via os.environ (ProcessPoolExecutor)
# ─────────────────────────────────────────────────────────────────────────────
os.environ["GDAL_PDF_LIB"] = "INTERNAL"


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES GLOBAIS
# ─────────────────────────────────────────────────────────────────────────────
CONFIG = {
    # FIX R3: 200 DPI — excelente qualidade para leitura de procedimentos IFR
    # em tablets. Resolução real: ~1653x2339 px (A4 portrait)
    "dpi": 200,

    # FIX R3: q=90 — qualidade superior com tamanho controlado
    # Estimativa: ~0.9–1.5 MB/carta | ~1.6–2.7 GB total (1829 cartas)
    "jpeg_quality": 90,

    # JPEG progressivo: carta aparece imediatamente e afina — melhor UX no app
    "progressive": True,

    # FIX R2: 4:4:4 — preserva máxima fidelidade cromática em textos finos,
    # frequências, altitudes e anotações de procedimentos aeronáuticos.
    # Evita artefatos de borramento em bordas retas (que 4:2:0 causaria).
    "subsampling": "4:4:4",

    # Página do PDF (1-based no GDAL open_options)
    "pdf_page": 1,

    # Sistema de referência alvo
    "expected_crs": "EPSG:4326",

    # Mínimo de GCPs para considerar o PDF georreferenciado
    "min_gcps": 3,
}


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline_gdal_v3.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────────────────────────────────────
def calcular_md5(filepath: Path) -> str:
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def pixel_to_geo(px: float, py: float, gt: tuple) -> list:
    lon = gt[0] + px * gt[1] + py * gt[2]
    lat = gt[3] + px * gt[4] + py * gt[5]
    return [round(lon, 8), round(lat, 8)]


# ─────────────────────────────────────────────────────────────────────────────
# EXTRAÇÃO DE GCPs (FIX #3 — corners via transformação afim real)
# ─────────────────────────────────────────────────────────────────────────────
def extrair_gcps(ds: gdal.Dataset) -> dict | None:
    gcps_raw = ds.GetGCPs()
    gcp_projection = ds.GetGCPProjection()
    width  = ds.RasterXSize
    height = ds.RasterYSize

    if gcps_raw and len(gcps_raw) >= CONFIG["min_gcps"]:
        return _gcps_para_dict(gcps_raw, gcp_projection, width, height)

    gt = ds.GetGeoTransform()
    identidade = (0.0, 1.0, 0.0, 0.0, 0.0, 1.0)
    if gt and gt != identidade:
        log.info("  → Fallback: usando GeoTransform direto")
        return _geotransform_para_dict(gt, width, height)

    log.warning("  ⚠ Sem georeferenciamento detectado")
    return None


def _gcps_para_dict(gcps_raw, gcp_projection: str, width: int, height: int) -> dict:
    srs_src  = osr.SpatialReference()
    srs_wgs84 = osr.SpatialReference()
    srs_wgs84.ImportFromEPSG(4326)
    precisa_reprojetar = False

    if gcp_projection:
        srs_src.ImportFromWkt(gcp_projection)
        if not srs_src.IsSame(srs_wgs84):
            precisa_reprojetar = True

    gcps_list, lons, lats = [], [], []

    for gcp in gcps_raw:
        lon, lat = gcp.GCPX, gcp.GCPY
        if precisa_reprojetar:
            transform = osr.CoordinateTransformation(srs_src, srs_wgs84)
            lat, lon, _ = transform.TransformPoint(lon, lat)

        gcps_list.append({
            "id": gcp.Id, "pixel_x": round(gcp.GCPPixel, 2),
            "pixel_y": round(gcp.GCPLine, 2),
            "lon": round(lon, 8), "lat": round(lat, 8),
        })
        lons.append(lon)
        lats.append(lat)

    # FIX #3: Transformação afim real — correto para cartas inclinadas
    geotransform = gdal.GCPsToGeoTransform(gcps_raw, bApproxOK=1)

    if geotransform:
        corners = [
            pixel_to_geo(0,     0,      geotransform),
            pixel_to_geo(width, 0,      geotransform),
            pixel_to_geo(width, height, geotransform),
            pixel_to_geo(0,     height, geotransform),
        ]
    else:
        log.warning("  ⚠ GCPsToGeoTransform falhou — fallback min/max")
        corners = [
            [round(min(lons), 8), round(max(lats), 8)],
            [round(max(lons), 8), round(max(lats), 8)],
            [round(max(lons), 8), round(min(lats), 8)],
            [round(min(lons), 8), round(min(lats), 8)],
        ]

    return {
        "tipo": "GCPs", "crs": CONFIG["expected_crs"],
        "total_gcps": len(gcps_list), "gcps": gcps_list,
        "bbox": {
            "north": round(max(lats), 8), "south": round(min(lats), 8),
            "east":  round(max(lons), 8), "west":  round(min(lons), 8),
        },
        # Formato direto para MapLibre addSource type=image
        # Ordem: [top-left, top-right, bottom-right, bottom-left]
        "corners": corners,
    }


def _geotransform_para_dict(gt: tuple, width: int, height: int) -> dict:
    corners = [
        pixel_to_geo(0,     0,      gt),
        pixel_to_geo(width, 0,      gt),
        pixel_to_geo(width, height, gt),
        pixel_to_geo(0,     height, gt),
    ]
    lons = [c[0] for c in corners]
    lats = [c[1] for c in corners]
    return {
        "tipo": "GeoTransform", "crs": CONFIG["expected_crs"],
        "total_gcps": 4,
        "gcps": [
            {"id": "TL", "pixel_x": 0,     "pixel_y": 0,      "lon": corners[0][0], "lat": corners[0][1]},
            {"id": "TR", "pixel_x": width,  "pixel_y": 0,      "lon": corners[1][0], "lat": corners[1][1]},
            {"id": "BR", "pixel_x": width,  "pixel_y": height, "lon": corners[2][0], "lat": corners[2][1]},
            {"id": "BL", "pixel_x": 0,      "pixel_y": height, "lon": corners[3][0], "lat": corners[3][1]},
        ],
        "bbox": {
            "north": round(max(lats), 8), "south": round(min(lats), 8),
            "east":  round(max(lons), 8), "west":  round(min(lons), 8),
        },
        "corners": corners,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CONVERSÃO JPEG (FIX R1 + R2 + R3)
# ─────────────────────────────────────────────────────────────────────────────
def converter_para_jpeg(ds: gdal.Dataset, output_path: Path, carta_id: str) -> dict:
    """
    FIX R1: Usa /vsimem/ (filesystem virtual em memória do GDAL) como
    intermediário TIFF lossless. Elimina a dupla codificação lossy→lossy
    que a v2.0.0 fazia (GDAL→JPEG temp → Pillow→JPEG final).

    Pipeline correto:
      1. gdal.Translate → /vsimem/{id}.tif (TIFF sem compressão, em RAM)
      2. Numpy lê pixels do TIFF sem compressão (100% fidelidade)
      3. Pillow salva JPEG uma única vez com q=90, 4:4:4, progressive

    FIX R2: subsampling="4:4:4" → legibilidade máxima em textos aeronáuticos
    FIX R3: dpi=(200, 200), quality=90
    """
    vsimem_path = f"/vsimem/{carta_id}_intermediate.tif"

    try:
        # Etapa 1: Render do PDF → TIFF lossless na memória (sem I/O de disco)
        gdal.Translate(
            vsimem_path,
            ds,
            format="GTiff",
            creationOptions=["COMPRESS=NONE"],  # Sem compressão = leitura instantânea
        )

        # Etapa 2: Lê pixels do TIFF em memória via numpy (lossless)
        mem_ds = gdal.Open(vsimem_path, gdal.GA_ReadOnly)
        if mem_ds is None:
            raise RuntimeError("Falha ao abrir TIFF intermediário em /vsimem/")

        width  = mem_ds.RasterXSize
        height = mem_ds.RasterYSize
        bandas = mem_ds.RasterCount

        if bandas >= 3:
            r = mem_ds.GetRasterBand(1).ReadAsArray()
            g = mem_ds.GetRasterBand(2).ReadAsArray()
            b = mem_ds.GetRasterBand(3).ReadAsArray()
            img_array = np.stack([r, g, b], axis=-1)
            img = Image.fromarray(img_array.astype(np.uint8), mode="RGB")
        else:
            data = mem_ds.GetRasterBand(1).ReadAsArray()
            img = Image.fromarray(data.astype(np.uint8), mode="L").convert("RGB")

        mem_ds = None  # fecha antes de unlink

        # Etapa 3: Pillow salva JPEG uma única vez (sem geração intermediária)
        img.save(
            output_path,
            format="JPEG",
            quality=CONFIG["jpeg_quality"],       # 90
            progressive=CONFIG["progressive"],    # True
            subsampling=CONFIG["subsampling"],    # "4:4:4"
            dpi=(CONFIG["dpi"], CONFIG["dpi"]),   # (200, 200)
            optimize=True,
        )

        tamanho_kb = output_path.stat().st_size / 1024
        return {
            "largura_px":   width,
            "altura_px":    height,
            "dpi":          CONFIG["dpi"],
            "jpeg_quality": CONFIG["jpeg_quality"],
            "progressive":  CONFIG["progressive"],
            "subsampling":  CONFIG["subsampling"],
            "bandas":       bandas,
            "tamanho_kb":   round(tamanho_kb, 1),
        }

    finally:
        # Libera memória virtual GDAL sempre (mesmo em caso de erro)
        gdal.Unlink(vsimem_path)


# ─────────────────────────────────────────────────────────────────────────────
# PROCESSAMENTO POR CARTA
# ─────────────────────────────────────────────────────────────────────────────
def processar_carta(pdf_path: Path, output_dir: Path) -> dict:
    """
    FIX #1: gdal.OpenEx com open_options por chamada — sem SetConfigOption global.
    FIX #2: GDAL_PDF_LIB=INTERNAL herdado via os.environ pelo processo filho.
    """
    carta_id  = pdf_path.stem
    jpeg_path = output_dir / f"{carta_id}.jpg"
    json_path = output_dir / f"{carta_id}.json"

    if jpeg_path.exists() and json_path.exists():
        return {"status": "skipped", "carta": carta_id}

    try:
        # FIX #1: open_options isolados por chamada (sem race condition)
        ds = gdal.OpenEx(
            str(pdf_path),
            gdal.GA_ReadOnly,
            open_options=[
                f"DPI={CONFIG['dpi']}",
                f"PAGE={CONFIG['pdf_page']}",
                "RENDERING_OPTIONS=RASTER",
            ],
        )

        if ds is None:
            raise RuntimeError("GDAL não conseguiu abrir o arquivo PDF")

        georef   = extrair_gcps(ds)
        img_meta = converter_para_jpeg(ds, jpeg_path, carta_id)
        ds = None

        sidecar = {
            "carta_id":         carta_id,
            "source_pdf":       pdf_path.name,
            "source_md5":       calcular_md5(pdf_path),
            "georreferenciado": georef is not None,
            "georef":           georef,
            "imagem":           img_meta,
            "jpeg_file":        jpeg_path.name,
            "pipeline_versao":  "3.0.0",
            "gerado_em":        datetime.now(timezone.utc).isoformat(),
        }

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(sidecar, f, ensure_ascii=False, indent=2)

        log.info(
            f"  ✅ {carta_id} → {img_meta['tamanho_kb']:.0f} KB "
            f"| {img_meta['largura_px']}×{img_meta['altura_px']}px "
            f"| georef={'✓' if georef else '✗'}"
        )

        return {
            "status":           "ok",
            "carta":            carta_id,
            "tamanho_kb":       img_meta["tamanho_kb"],
            "georreferenciado": georef is not None,
        }

    except Exception as e:
        log.error(f"  ❌ {carta_id}: {e}")
        return {"status": "erro", "carta": carta_id, "erro": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# RELATÓRIO
# ─────────────────────────────────────────────────────────────────────────────
def gerar_relatorio(resultados: list, output_dir: Path):
    total   = len(resultados)
    ok      = [r for r in resultados if r["status"] == "ok"]
    erros   = [r for r in resultados if r["status"] == "erro"]
    skipped = [r for r in resultados if r["status"] == "skipped"]
    georef  = [r for r in ok if r.get("georreferenciado")]

    total_mb = sum(r.get("tamanho_kb", 0) for r in ok) / 1024
    media_kb = (sum(r.get("tamanho_kb", 0) for r in ok) / len(ok)) if ok else 0

    relatorio = {
        "data_execucao":          datetime.now(timezone.utc).isoformat(),
        "pipeline_versao":        "3.0.0",
        "total_cartas":           total,
        "convertidas":            len(ok),
        "erros":                  len(erros),
        "skipped":                len(skipped),
        "georreferenciadas":      len(georef),
        "nao_georreferenciadas":  len(ok) - len(georef),
        "total_mb":               round(total_mb, 1),
        "media_kb_por_carta":     round(media_kb, 1),
        "config":                 CONFIG,
        "erros_detalhes":         erros,
    }

    relatorio_path = output_dir / "relatorio_pipeline.json"
    with open(relatorio_path, "w", encoding="utf-8") as f:
        json.dump(relatorio, f, ensure_ascii=False, indent=2)

    print("\n" + "═" * 64)
    print("  RELATÓRIO FINAL — Pipeline GDAL v3.0.0 — Cartas AIS Brasil")
    print("═" * 64)
    print(f"  Total processado     : {total}")
    print(f"  ✅ Convertidas       : {len(ok)}")
    print(f"  ⏭  Skipped           : {len(skipped)}")
    print(f"  ❌ Erros             : {len(erros)}")
    print(f"  🗺  Georreferenciadas : {len(georef)} de {len(ok)}")
    print(f"  ⚠  Sem georef        : {len(ok) - len(georef)}")
    print(f"  📦 Total gerado      : {total_mb:.1f} MB")
    print(f"  📄 Média/carta       : {media_kb:.0f} KB (~{media_kb/1024:.2f} MB)")
    print(f"  📋 Relatório         : {relatorio_path}")
    print("═" * 64 + "\n")
    return relatorio


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Pipeline GDAL v3.0.0: GeoPDF → JPEG 200 DPI q=90 4:4:4 + Sidecar JSON"
    )
    parser.add_argument("--input",   "-i", required=True, help="Diretório com os GeoPDFs")
    parser.add_argument("--output",  "-o", required=True, help="Diretório de saída")
    parser.add_argument("--workers", "-w", type=int, default=4,
                        help="Workers paralelos (default: 4)")
    args = parser.parse_args()

    input_dir  = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(input_dir.glob("*.pdf"))
    if not pdfs:
        log.error(f"Nenhum PDF encontrado em: {input_dir}")
        return

    log.info(f"🚀 Pipeline GDAL v3.0.0 — {len(pdfs)} cartas | {args.workers} workers")
    log.info(f"   {CONFIG['dpi']} DPI | JPEG q={CONFIG['jpeg_quality']} | "
             f"Subsampling={CONFIG['subsampling']} | Progressive={CONFIG['progressive']}")
    log.info(f"   Driver PDF: INTERNAL | Intermediário: /vsimem/ (sem disco)")

    resultados = []
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(processar_carta, pdf, output_dir): pdf
            for pdf in pdfs
        }
        with tqdm(total=len(pdfs), desc="Convertendo cartas", unit="carta") as pbar:
            for future in as_completed(futures):
                resultado = future.result()
                resultados.append(resultado)
                pbar.update(1)
                if resultado["status"] == "erro":
                    pbar.set_postfix({"erro": resultado["carta"]})

    gerar_relatorio(resultados, output_dir)


if __name__ == "__main__":
    main()
