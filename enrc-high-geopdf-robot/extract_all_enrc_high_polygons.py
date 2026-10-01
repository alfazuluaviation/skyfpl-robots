"""
extract_all_enrc_high_polygons.py — SkyFPL Conic Neatline Extractor (DECEA Official GeoTIFFs ENRC HIGH)
========================================================================================================
Baixa os GeoTIFFs oficiais do DECEA (GeoAISWEB) para as cartas ENRC HIGH (H1 a H9),
vetoriza a Banda 4 (Alfa) via GDAL, calcula a curvatura cônica de Lambert exata
e gera a base canônica de corte para o Robô e para o Dashboard Admin e App Native.
"""

import os
import sys
import json
import time
import urllib.request
import tempfile
from osgeo import gdal, ogr

gdal.UseExceptions()

CODES = ["H1", "H2", "H3", "H4", "H5", "H6", "H7", "H8", "H9"]
BASE_URL = "https://geoaisweb.decea.mil.br/src/geotiffs/ENRC_{code}.tif"
OUTPUT_JSON = os.path.join(os.path.dirname(__file__), "enrc_high_official_polygons.json")

def download_file(url: str, dest_path: str):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkyFPL/Robot-HD"}
    req = urllib.request.Request(url, headers=headers)
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as resp:
        total = int(resp.headers.get("Content-Length", 0))
        downloaded = 0
        with open(dest_path, "wb") as f:
            while True:
                chunk = resp.read(512 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0 and downloaded % (4 * 1024 * 1024) < (512 * 1024):
                    pct = int(downloaded / total * 100)
                    print(f"   ↳ {downloaded / (1024*1024):.1f} MB / {total / (1024*1024):.1f} MB ({pct}%)", flush=True)
    dt = time.time() - t0
    print(f"  ✓ Download finalizado em {dt:.1f}s ({downloaded/(1024*1024):.1f} MB)", flush=True)

def extract_polygon_from_tif(tif_path: str, code: str) -> dict:
    print(f"\n[{code}] Abrindo GeoTIFF oficial: {os.path.basename(tif_path)}...")
    ds = gdal.Open(tif_path)
    w, h = ds.RasterXSize, ds.RasterYSize
    print(f"  Dimensões: {w} x {h} pixels | Bandas: {ds.RasterCount}")

    if ds.RasterCount < 4:
        raise ValueError(f"O arquivo {tif_path} não possui Banda 4 (necessário RGBA)")

    b4 = ds.GetRasterBand(4)

    # Driver de memória OGR
    mem_drv = ogr.GetDriverByName("Memory") or ogr.GetDriverByName("MEM")
    if not mem_drv:
        mem_drv = gdal.GetDriverByName("Memory")
    mem_ds = mem_drv.CreateDataSource("mem_ds")
    layer = mem_ds.CreateLayer("neatline", None, ogr.wkbPolygon)
    layer.CreateField(ogr.FieldDefn("val", ogr.OFTInteger))

    print(f"  Executando gdal.Polygonize na Banda 4...")
    t0 = time.time()
    gdal.Polygonize(b4, b4, layer, 0, [])
    print(f"  Vetorização concluída em {time.time()-t0:.2f}s ({layer.GetFeatureCount()} feições).")

    # Localizar a feição de valor 255 com maior área (o corpo principal da carta)
    best_geom = None
    max_area = 0.0
    for feat in layer:
        val = feat.GetField("val")
        if val == 255:
            geom = feat.GetGeometryRef()
            if geom:
                area = geom.GetArea()
                if area > max_area:
                    max_area = area
                    best_geom = geom.Clone()

    if not best_geom:
        raise ValueError(f"Nenhum polígono opaco (Alfa=255) encontrado em {code}")

    print(f"  Área da lâmina principal: {max_area:.4f} graus²")

    # Inset Buffer de segurança de -0.012 graus (~1.3 km de recuo interno contra borda de papel)
    inset_deg = -0.012
    buffered_geom = best_geom.Buffer(inset_deg)
    if buffered_geom and not buffered_geom.IsEmpty():
        if buffered_geom.GetGeometryType() == ogr.wkbMultiPolygon:
            max_p_area = 0.0
            chosen = None
            for p_idx in range(buffered_geom.GetGeometryCount()):
                sub_p = buffered_geom.GetGeometryRef(p_idx)
                if sub_p.GetArea() > max_p_area:
                    max_p_area = sub_p.GetArea()
                    chosen = sub_p.Clone()
            working_geom = chosen or buffered_geom
        else:
            working_geom = buffered_geom
        print(f"  Inset Buffer aplicado: {inset_deg}°")
    else:
        working_geom = best_geom

    # Calibração da curvatura cônica de Lambert (~100 a 160 vértices)
    tol = 0.0007
    simp = working_geom.SimplifyPreserveTopology(tol)
    ring = simp.GetGeometryRef(0)
    pt_count = ring.GetPointCount()

    if pt_count < 60:
        tol = 0.0005
        simp = working_geom.SimplifyPreserveTopology(tol)
        ring = simp.GetGeometryRef(0)
        pt_count = ring.GetPointCount()
    elif pt_count > 250:
        tol = 0.00085
        simp = working_geom.SimplifyPreserveTopology(tol)
        ring = simp.GetGeometryRef(0)
        pt_count = ring.GetPointCount()

    print(f"  Polígono calibrado com tolerância {tol}°: {pt_count} vértices")

    pts = []
    lons = []
    lats = []
    for i in range(pt_count):
        x = round(ring.GetX(i), 5)
        y = round(ring.GetY(i), 5)
        pts.append([x, y])
        lons.append(x)
        lats.append(y)

    if pts[0] != pts[-1]:
        pts.append(pts[0])

    bbox = [
        round(min(lons), 5),
        round(min(lats), 5),
        round(max(lons), 5),
        round(max(lats), 5)
    ]
    print(f"  BBOX Calculado ({code}): {bbox}")

    return {
        "code": code,
        "ident": f"ENRC_{code}",
        "bbox": bbox,
        "coordinates": [pts]
    }

def main():
    print("=" * 80)
    print("SkyFPL — Extração Automática de Polígonos Cônicos DECEA (ENRC H1 a H9)")
    print("=" * 80)

    results = {}
    temp_dir = tempfile.mkdtemp(prefix="decea_enrch_geotiffs_")

    try:
        for idx, code in enumerate(CODES, 1):
            print(f"\n>>> [{idx:02d}/{len(CODES):02d}] Processando Carta ENRC HIGH {code}...")
            url = BASE_URL.format(code=code)
            dest = os.path.join(temp_dir, f"ENRC_{code}.tif")

            try:
                download_file(url, dest)
                poly_data = extract_polygon_from_tif(dest, code)
                results[code] = poly_data
                print(f"  ✅ {code} extraído com sucesso ({len(poly_data['coordinates'][0])} vértices).")
            except Exception as e:
                print(f"  ❌ Falha ao processar {code}: {e}")
            finally:
                if os.path.exists(dest):
                    try:
                        os.remove(dest)
                    except Exception:
                        pass

        # Salvar JSON canônico no robô
        print("\n" + "=" * 80)
        print(f"💾 Gravando {len(results)} polígonos oficiais em: {OUTPUT_JSON}")
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        print("✅ Arquivo JSON atualizado com sucesso!")

        # Resumo final
        print("\n" + "=" * 80)
        print("TABELA RESUMO DE NEATLINES OFICIAIS ENRC HIGH (DECEA):")
        print(f"{'CÓDIGO':<8} | {'VÉRTICES':<10} | {'BBOX (W, S, E, N)'}")
        print("-" * 80)
        for code in CODES:
            if code in results:
                v_count = len(results[code]["coordinates"][0])
                bbox_str = str(results[code]["bbox"])
                print(f"{code:<8} | {v_count:<10} | {bbox_str}")
            else:
                print(f"{code:<8} | {'FALHA':<10} | -")
        print("=" * 80 + "\n")

    finally:
        if os.path.exists(temp_dir):
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

if __name__ == "__main__":
    main()
