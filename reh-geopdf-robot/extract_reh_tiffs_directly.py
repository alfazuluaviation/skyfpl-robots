"""
extract_reh_tiffs_directly.py — Extração 100% Direta dos 12 GeoTIFFs REH com Reprojeção WGS84
=============================================================================================
1. Executa gdalinfo via /vsicurl/ diretamente em cada arquivo .tif oficial do DECEA;
2. Lê wgs84Extent (reprojetado automaticamente pelo GDAL para EPSG:4326 independente de ser UTM ou Geográfico);
3. Extrai os vértices geográficos exatos e centro;
4. Aplica Inset Buffer de segurança (~80 m / 0.0008°);
5. Compara lado a lado com os BBOXes atuais do catálogo e do WMS Capabilities do DECEA;
6. Mostra a comparação detalhada de consistência.
"""

import os
import sys
import json
import subprocess
import urllib.request
import xml.etree.ElementTree as ET

GDALINFO = r"C:\Program Files\QGIS 3.40.15\bin\gdalinfo.exe"
GEOTIFF_BASE_URL = "https://geoaisweb.decea.mil.br/src/geotiffs/{filename}"
WMS_CAPABILITIES_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms?service=WMS&request=GetCapabilities"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkyFPL/REH-Auditor"}

CANONICAL_SECTORS = [
    {"code": "REH_XP2_SAO_PAULO_1", "ident": "CCV_REH_XP2_SAO_PAULO_1", "name": "São Paulo 1 (RMSP Central)", "layer": "ICA:CCV_REH_XP2_SAO_PAULO_1", "file": "CCV_REH_XP2_SAO_PAULO_1.tif", "catalog_bbox": [-47.03032, -23.83910, -46.30835, -23.30926]},
    {"code": "REH_XP2_SAO_PAULO_2", "ident": "CCV_REH_XP2_SAO_PAULO_2", "name": "São Paulo 2 (RMSP Expandido)", "layer": "ICA:CCV_REH_XP2_SAO_PAULO_2", "file": "CCV_REH_XP2_SAO_PAULO_2.tif", "catalog_bbox": [-46.78313, -23.68155, -46.58929, -23.47636]},
    {"code": "REH_WJ2_RIO_DE_JANEIRO", "ident": "CCV_REH_WJ2_RIO_DE_JANEIRO", "name": "Rio de Janeiro (WJ2)", "layer": "ICA:CCV_REH_WJ2_RIO_DE_JANEIRO", "file": "CCV_REH_WJ2_RIO_DE_JANEIRO.tif", "catalog_bbox": [-43.93131, -23.16674, -42.97307, -22.52755]},
    {"code": "REH_WJ3_RIO_DE_JANEIRO", "ident": "CCV_REH_WJ3_RIO_DE_JANEIRO", "name": "Rio de Janeiro (WJ3)", "layer": "ICA:CCV_REH_WJ3_RIO_DE_JANEIRO", "file": "CCV_REH_WJ3_RIO_DE_JANEIRO.tif", "catalog_bbox": [-43.42878, -23.04705, -43.06992, -22.80662]},
    {"code": "REH_BACIA_DE_SANTOS", "ident": "ICA:REH_BACIA_DE_SANTOS", "name": "Bacia de Santos Offshore", "layer": "ICA:REH_BACIA_DE_SANTOS", "file": "REH_BACIA_DE_SANTOS.tif", "catalog_bbox": [-43.83333, -26.50003, -41.49992, -22.66637]},
    {"code": "REH_WJ1_CABO_FRIO", "ident": "CCV_REH_WJ1_CABO_FRIO", "name": "Cabo Frio (TMA-CF)", "layer": "ICA:CCV_REH_WJ1_CABO_FRIO", "file": "CCV_REH_WJ1_CABO_FRIO.tif", "catalog_bbox": [-43.02842, -23.21096, -41.88762, -22.39236]},
    {"code": "REH_XP2_CAMPINAS", "ident": "CCV_REH_XP2_CAMPINAS", "name": "Campinas (TMA-CP)", "layer": "ICA:CCV_REH_XP2_CAMPINAS", "file": "CCV_REH_XP2_CAMPINAS.tif", "catalog_bbox": [-47.27538, -23.43792, -46.72208, -22.75112]},
    {"code": "REH_XP1_SAO_JOSE_DOS_CAMPOS", "ident": "CCV_REH_XP1_SAO_JOSE_DOS_CAMPOS", "name": "São José dos Campos", "layer": "ICA:CCV_REH_XP1_SAO_JOSE_DOS_CAMPOS", "file": "CCV_REH_XP1_SAO_JOSE_DOS_CAMPOS.tif", "catalog_bbox": [-46.36064, -23.59060, -45.81037, -22.90351]},
    {"code": "REH_XP1_SOROCABA", "ident": "CCV_REH_XP1_SOROCABA", "name": "Sorocaba", "layer": "ICA:CCV_REH_XP1_SOROCABA", "file": "CCV_REH_XP1_SOROCABA.tif", "catalog_bbox": [-47.65949, -23.78841, -46.93508, -23.25583]},
    {"code": "REH_WH_BELO_HORIZONTE", "ident": "CCV_REH_WH_BELO_HORIZONTE", "name": "Belo Horizonte (TMA-BH)", "layer": "ICA:CCV_REH_WH_BELO_HORIZONTE", "file": "CCV_REH_WH_BELO_HORIZONTE.tif", "catalog_bbox": [-44.28333, -20.20000, -43.63331, -19.38330]},
    {"code": "REH_CURITIBA", "ident": "ICA:REH_CURITIBA", "name": "Curitiba (TMA-CT)", "layer": "ICA:REH_CURITIBA", "file": "REH_CURITIBA.tif", "catalog_bbox": [-49.36667, -25.68333, -49.01662, -25.28323]},
    {"code": "REH_VITORIA", "ident": "ICA:REH_VITORIA", "name": "Vitória (TMA-VT)", "layer": "ICA:REH_VITORIA", "file": "REH_VITORIA.tif", "catalog_bbox": [-40.58333, -20.58333, -39.91661, -20.08323]}
]

def fetch_decea_capabilities():
    bboxes = {}
    try:
        req = urllib.request.Request(WMS_CAPABILITIES_URL, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=35) as resp:
            xml_bytes = resp.read()
        root = ET.fromstring(xml_bytes)
        for layer in root.iter('{http://www.opengis.net/wms}Layer'):
            name_el = layer.find('{http://www.opengis.net/wms}Name')
            ex_bbox = layer.find('{http://www.opengis.net/wms}EX_GeographicBoundingBox')
            if name_el is not None and name_el.text and ex_bbox is not None:
                n = name_el.text.strip().replace("ICA:", "")
                try:
                    w = float(ex_bbox.findtext('{http://www.opengis.net/wms}westBoundLongitude'))
                    s = float(ex_bbox.findtext('{http://www.opengis.net/wms}southBoundLatitude'))
                    e = float(ex_bbox.findtext('{http://www.opengis.net/wms}eastBoundLongitude'))
                    n_lat = float(ex_bbox.findtext('{http://www.opengis.net/wms}northBoundLatitude'))
                    bboxes[n] = [round(w, 5), round(s, 5), round(e, 5), round(n_lat, 5)]
                except Exception:
                    pass
    except Exception as e:
        print(f"Aviso WMS: {e}")
    return bboxes

def apply_inset_buffer(coords: list, inset_deg: float = 0.0008) -> list:
    ring = coords[0]
    cx = sum(p[0] for p in ring[:-1]) / (len(ring) - 1)
    cy = sum(p[1] for p in ring[:-1]) / (len(ring) - 1)
    new_ring = []
    for p in ring:
        dx = cx - p[0]
        dy = cy - p[1]
        dist = (dx*dx + dy*dy)**0.5
        if dist > 1e-6:
            nx = p[0] + (dx / dist) * inset_deg
            ny = p[1] + (dy / dist) * inset_deg
            new_ring.append([round(nx, 5), round(ny, 5)])
        else:
            new_ring.append([round(p[0], 5), round(p[1], 5)])
    if new_ring[0] != new_ring[-1]:
        new_ring[-1] = new_ring[0]
    return [new_ring]

def main():
    print("=" * 120)
    print("🚁 AUDITORIA CARTOGRÁFICA EXAUSTIVA: Extração Direta dos 12 GeoTIFFs REH DECEA via GDAL (EPSG:4326)")
    print("=" * 120)

    wms_bboxes = fetch_decea_capabilities()
    audit_results = []

    for item in CANONICAL_SECTORS:
        code = item["code"]
        name = item["name"]
        file_name = item["file"]
        catalog_bbox = item["catalog_bbox"]
        clean_layer = item["layer"].replace("ICA:", "")

        tif_url = GEOTIFF_BASE_URL.format(filename=file_name)
        vsi_url = f"/vsicurl/{tif_url}"

        cmd = [GDALINFO, "-json", vsi_url]
        p = subprocess.run(cmd, capture_output=True, text=True)

        status = "OK"
        crs_name = "N/A"
        tiff_bbox_raw = None
        tiff_bbox_buffered = None
        tiff_center = None
        inconsistency = []

        if p.returncode == 0:
            try:
                info = json.loads(p.stdout)
                crs_desc = info.get("coordinateSystem", {}).get("wkt", "")
                if "UTM" in crs_desc:
                    crs_name = "UTM (Projetado)"
                elif "EPSG\",4326" in crs_desc or "WGS 84" in crs_desc:
                    crs_name = "WGS84 (Geográfico)"
                else:
                    crs_name = "Outro CRS"

                # Lê wgs84Extent que já converte qualquer UTM/Projeção para coordenadas geográficas EPSG:4326
                extent = info.get("wgs84Extent")
                if extent and extent.get("coordinates"):
                    ring_wgs84 = extent["coordinates"][0]
                    lons = [p[0] for p in ring_wgs84]
                    lats = [p[1] for p in ring_wgs84]

                    min_lon = min(lons)
                    max_lon = max(lons)
                    min_lat = min(lats)
                    max_lat = max(lats)

                    tiff_bbox_raw = [round(min_lon, 5), round(min_lat, 5), round(max_lon, 5), round(max_lat, 5)]
                    tiff_center = [round((min_lon + max_lon) / 2.0, 5), round((min_lat + max_lat) / 2.0, 5)]

                    buf = apply_inset_buffer(extent["coordinates"], inset_deg=0.0008)
                    ring_buf = buf[0]
                    tiff_bbox_buffered = [
                        round(min(p[0] for p in ring_buf), 5),
                        round(min(p[1] for p in ring_buf), 5),
                        round(max(p[0] for p in ring_buf), 5),
                        round(max(p[1] for p in ring_buf), 5)
                    ]

                    # Compara com catalog_bbox
                    diffs = [
                        abs(catalog_bbox[0] - tiff_bbox_buffered[0]),
                        abs(catalog_bbox[1] - tiff_bbox_buffered[1]),
                        abs(catalog_bbox[2] - tiff_bbox_buffered[2]),
                        abs(catalog_bbox[3] - tiff_bbox_buffered[3]),
                    ]
                    max_diff = max(diffs)
                    if max_diff > 0.005: # > ~500 metros
                        status = "DIVERGENTE"
                        inconsistency.append(f"Divergência de {max_diff:.5f}° (~{max_diff*111:.1f} km)")
                    elif max_diff > 0.0003: # > ~30 metros
                        status = "LEVE_AJUSTE"
                        inconsistency.append(f"Ajuste fino de {max_diff:.5f}° (~{max_diff*111*1000:.0f} m)")
                    else:
                        status = "ALINHADO"
                        inconsistency.append(f"Alinhamento perfeito (delta < 30m)")

                else:
                    status = "SEM_WGS84_EXTENT"
                    inconsistency.append("wgs84Extent ausente no gdalinfo")

            except Exception as ex:
                status = "ERRO_PARSE"
                inconsistency.append(str(ex))
        else:
            status = "FALHA_TIFF"
            inconsistency.append(p.stderr[:100] if p.stderr else "HTTP ou GDAL error")

        wms_box = wms_bboxes.get(clean_layer)

        audit_results.append({
            "code": code,
            "name": name,
            "file": file_name,
            "crs": crs_name,
            "status": status,
            "catalog_bbox": catalog_bbox,
            "tiff_bbox_raw": tiff_bbox_raw,
            "tiff_bbox_buffered": tiff_bbox_buffered,
            "tiff_center": tiff_center,
            "wms_capabilities_bbox": wms_box,
            "notes": "; ".join(inconsistency)
        })

    print(f"\n{'CÓDIGO':<28} | {'CRS':<16} | {'STATUS':<12} | {'BBOX CATÁLOGO ATUAL':<38} | {'BBOX REAL TIFF (BUFFERED)':<38}")
    print("-" * 140)
    for res in audit_results:
        cat_s = str(res["catalog_bbox"])
        tif_s = str(res["tiff_bbox_buffered"]) if res["tiff_bbox_buffered"] else "N/A"
        print(f"{res['code']:<28} | {res['crs']:<16} | {res['status']:<12} | {cat_s:<38} | {tif_s:<38}")

    print("\n" + "=" * 120)
    print("DETALHAMENTO SETOR A SETOR COM DELTAS:")
    print("=" * 120)
    for res in audit_results:
        print(f"\n📌 Setor: {res['code']} — {res['name']}")
        print(f"   Arquivo DECEA: {res['file']} (CRS: {res['crs']})")
        print(f"   Diagnóstico: [{res['status']}] ➔ {res['notes']}")
        print(f"   - BBOX Catálogo Atual       : {res['catalog_bbox']}")
        print(f"   - BBOX Bruto GeoTIFF (WGS84): {res['tiff_bbox_raw']}")
        print(f"   - BBOX GeoTIFF + InsetBuffer: {res['tiff_bbox_buffered']}")
        print(f"   - BBOX WMS GetCapabilities  : {res['wms_capabilities_bbox']}")
        print(f"   - Centro Cartográfico       : {res['tiff_center']}")

    out_json = os.path.join(os.path.dirname(__file__), "reh_tiffs_audit.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(audit_results, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Relatório completo JSON salvo em: {out_json}")

if __name__ == "__main__":
    main()
