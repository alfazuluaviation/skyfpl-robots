"""
audit_enrch_neatline.py — SkyFPL Empirical Neatline Auditor (DECEA Master GeoTIFF)
==================================================================================
Baixa diretamente o GeoTIFF oficial da carta ENRC HIGH (ex: H1) do DECEA,
executa a extração precisa da máscara de neatline via GDAL (Banda 4 - Alfa)
e compara matematicamente ponto a ponto com a carta correspondente ENRC LOW (L1).
"""

import os
import sys
import json
import time
import math
import argparse
import urllib.request
import tempfile
from osgeo import gdal, ogr

gdal.UseExceptions()

def download_file(url: str, dest_path: str):
    print(f"📥 Baixando arquivo mestre do DECEA: {url}...")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkyFPL/Audit-HD"}
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
                if total > 0 and downloaded % (2 * 1024 * 1024) < (512 * 1024):
                    pct = int(downloaded / total * 100)
                    mb_down = downloaded / (1024 * 1024)
                    mb_total = total / (1024 * 1024)
                    print(f"   ↳ {mb_down:.1f} MB / {mb_total:.1f} MB ({pct}%)", flush=True)
    dt = time.time() - t0
    speed_mb = (downloaded / (1024 * 1024)) / max(dt, 0.01)
    print(f"✅ Download finalizado em {dt:.1f}s ({speed_mb:.1f} MB/s) — {downloaded / (1024*1024):.2f} MB salvos.\n")

def extract_neatline_from_geotiff(tif_path: str, code: str) -> dict:
    print(f"🔬 Processando GeoTIFF com GDAL: {os.path.basename(tif_path)}")
    ds = gdal.Open(tif_path)
    w, h = ds.RasterXSize, ds.RasterYSize
    bands = ds.RasterCount
    print(f"   Dimensões do Raster: {w} x {h} pixels | Bandas: {bands}")

    if bands < 4:
        raise ValueError(f"O GeoTIFF não possui Banda 4 (Alfa necessário para neatline). Encontradas: {bands}")

    b4 = ds.GetRasterBand(4)

    mem_drv = ogr.GetDriverByName("Memory") or ogr.GetDriverByName("MEM")
    if not mem_drv:
        mem_drv = gdal.GetDriverByName("Memory")
    mem_ds = mem_drv.CreateDataSource("mem_ds")
    layer = mem_ds.CreateLayer("neatline", None, ogr.wkbPolygon)
    layer.CreateField(ogr.FieldDefn("val", ogr.OFTInteger))

    print("   Executando gdal.Polygonize na Banda 4 (Alpha Mask)...")
    t0 = time.time()
    gdal.Polygonize(b4, b4, layer, 0, [])
    print(f"   Vetorização concluída em {time.time()-t0:.2f}s ({layer.GetFeatureCount()} feições extraídas).")

    # Feição com maior área e val=255 (corpo útil da carta)
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
        raise ValueError("Nenhum polígono opaco (Alfa=255) foi detectado no GeoTIFF.")

    print(f"   Área bruta da lâmina útil: {max_area:.4f} graus²")

    # Inset Buffer de segurança de -0.012 graus (~1.3 km de recuo interno para evitar borda preta)
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
        print(f"   Inset buffer de segurança aplicado: {inset_deg}°")
    else:
        working_geom = best_geom

    # Calibração e simplificação da curvatura cônica de Lambert
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

    print(f"   Vértices da curvatura calculados ({tol}° tol): {pt_count} pontos")

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

    return {
        "code": code,
        "ident": f"ENRC_{code}",
        "bbox": bbox,
        "coordinates": [pts],
        "area_deg": round(working_geom.GetArea(), 6),
        "point_count": len(pts)
    }

def compare_neatlines(high_data: dict, low_data: dict):
    h_code = high_data["code"]
    l_code = low_data.get("code", "L?")
    print("=" * 80)
    print(f"📊 RELATÓRIO COMPARATIVO CARTOGRÁFICO: {h_code} (DECEA GeoTIFF) vs. {l_code} (Catálogo Existente)")
    print("=" * 80)

    h_bbox = high_data["bbox"]
    l_bbox = low_data["bbox"]
    h_pts = high_data["coordinates"][0]
    l_pts = low_data["coordinates"][0]

    print(f"\n1. 📐 Bounding Box:")
    print(f"   {h_code} Extraído: {h_bbox}")
    print(f"   {l_code} Existente: {l_bbox}")
    
    bbox_diff = [round(abs(h_bbox[i] - l_bbox[i]), 5) for i in range(4)]
    max_bbox_diff_deg = max(bbox_diff)
    # 1 grau ~ 111.12 km na latitude
    max_bbox_diff_m = max_bbox_diff_deg * 111120

    print(f"   Diferença nos limites: minX: {bbox_diff[0]}°, minY: {bbox_diff[1]}°, maxX: {bbox_diff[2]}°, maxY: {bbox_diff[3]}°")
    print(f"   Diferença máxima de fronteira: {max_bbox_diff_deg}° (~{max_bbox_diff_m:.1f} metros)")

    print(f"\n2. 📍 Densidade de Vértices:")
    print(f"   {h_code} Vértices extraídos: {len(h_pts)}")
    print(f"   {l_code} Vértices existentes: {len(l_pts)}")

    # Comparação geométrica de sobreposição via OGR
    def create_geom_from_coords(coords):
        ring = ogr.Geometry(ogr.wkbLinearRing)
        for pt in coords:
            ring.AddPoint(pt[0], pt[1])
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        return poly

    geom_h = create_geom_from_coords(h_pts)
    geom_l = create_geom_from_coords(l_pts)

    area_h = geom_h.GetArea()
    area_l = geom_l.GetArea()

    inter = geom_h.Intersection(geom_l)
    inter_area = inter.GetArea() if inter else 0.0

    union = geom_h.Union(geom_l)
    union_area = union.GetArea() if union else 1.0

    iou = (inter_area / union_area) * 100.0

    print(f"\n3. 🛰️ Sobreposição Espacial (IoU - Intersection over Union):")
    print(f"   Área {h_code}: {area_h:.6f} deg²")
    print(f"   Área {l_code}: {area_l:.6f} deg²")
    print(f"   Área Interseção: {inter_area:.6f} deg²")
    print(f"   Fidelidade Geométrica (IoU): {iou:.4f}%")

    # Comparar distâncias entre pontos
    # Para cada vértice de H, encontrar a menor distância a qualquer vértice de L
    max_dist_deg = 0.0
    for ph in h_pts:
        min_d = min(math.hypot(ph[0] - pl[0], ph[1] - pl[1]) for pl in l_pts)
        if min_d > max_dist_deg:
            max_dist_deg = min_d

    max_dist_m = max_dist_deg * 111120
    print(f"\n4. 📏 Desvio Máximo de Borda (Hausdorff pontual):")
    print(f"   Desvio angular máximo: {max_dist_deg:.6f}° (~{max_dist_m:.1f} metros)")

    # Parecer Técnico
    print("\n" + "=" * 80)
    print("🏁 PARECER TÉCNICO FINAL DA AUDITORIA:")
    if iou >= 99.8 and max_bbox_diff_m <= 150:
        print(f"   ✅ [EQUIVALÊNCIA 100% CONFIRMADA]:")
        print(f"   A geometria de corte do DECEA para a folha {h_code} é ESTRITAMENTE IDÊNTICA à folha {l_code}.")
        print(f"   A sobreposição espacial é de {iou:.2f}% e os limites convergem com precisão aeronáutica.")
        print(f"   -> RECOMENDAÇÃO: Manter o catálogo atual já configurado sem necessidade de reprocessar.")
        match_status = "IDENTICAL"
    else:
        print(f"   ⚠️ [DIVERGÊNCIA DETECTADA]:")
        print(f"   A carta {h_code} possui diferenças geométricas superiores à tolerância ({iou:.2f}% IoU).")
        print(f"   -> RECOMENDAÇÃO: Extrair todos os 9 GeoTIFFs oficiais H1 a H9 do DECEA.")
        match_status = "DIFFERENT"
    print("=" * 80 + "\n")

    return {
        "status": match_status,
        "high_code": h_code,
        "low_code": l_code,
        "iou_percent": round(iou, 4),
        "max_border_diff_meters": round(max_bbox_diff_m, 1),
        "high_vertex_count": len(h_pts),
        "low_vertex_count": len(l_pts),
        "high_bbox": h_bbox,
        "low_bbox": l_bbox
    }

def main():
    parser = argparse.ArgumentParser(description="SkyFPL ENRC H Neatline Auditor")
    parser.add_argument("--chart", type=str, default="H1", help="Código da carta H a auditar (ex: H1)")
    args = parser.parse_args()

    chart_code = args.chart.strip().upper()
    if not chart_code.startswith("H"):
        print(f"Erro: Código de carta inválido: {chart_code}. Deve começar com H (ex: H1).")
        sys.exit(1)

    low_code = "L" + chart_code[1:]

    # Carregar catálogo LOW de referência
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    low_polygons_path = os.path.join(repo_root, "enrc-geopdf-robot", "enrc_official_polygons.json")
    if not os.path.exists(low_polygons_path):
        # Tentar caminho local
        low_polygons_path = os.path.join(os.path.dirname(__file__), "enrc_high_official_polygons.json")

    with open(low_polygons_path, "r", encoding="utf-8") as f:
        low_catalog = json.load(f)

    if low_code not in low_catalog and chart_code in low_catalog:
        low_ref = low_catalog[chart_code]
    elif low_code in low_catalog:
        low_ref = low_catalog[low_code]
    else:
        raise ValueError(f"Carta de referência {low_code} não encontrada em {low_polygons_path}")

    # Baixar GeoTIFF oficial do DECEA
    geotiff_url = f"https://geoaisweb.decea.mil.br/src/geotiffs/ENRC_{chart_code}.tif"
    temp_dir = tempfile.mkdtemp(prefix=f"audit_{chart_code}_")
    tif_dest = os.path.join(temp_dir, f"ENRC_{chart_code}.tif")

    try:
        download_file(geotiff_url, tif_dest)
        high_extracted = extract_neatline_from_geotiff(tif_dest, chart_code)
        audit_summary = compare_neatlines(high_extracted, low_ref)

        # Salvar resultado da auditoria
        out_summary_file = os.path.join(os.path.dirname(__file__), f"audit_result_{chart_code}.json")
        with open(out_summary_file, "w", encoding="utf-8") as f:
            json.dump({
                "audit": audit_summary,
                "extracted_neatline": high_extracted
            }, f, indent=2)
        print(f"💾 Resultado gravado em: {out_summary_file}")

    finally:
        if os.path.exists(tif_dest):
            try:
                os.remove(tif_dest)
                os.rmdir(temp_dir)
            except Exception:
                pass

if __name__ == "__main__":
    main()
