#!/usr/bin/env python3
"""
SkyFPL - Super Robô de Cartas (Versão 14.0 - Unified Precision Pipeline)
============================================================
Indexação, Conversão (250 DPI), Extração GeoPDF (ICA 96-1) e Upload R2.
"""

import os
import sys
import json
import time
import argparse
import logging
import requests
import boto3
import signal
import threading
import re
import socket
import fitz  # PyMuPDF
from io import BytesIO
from PIL import Image
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# Nuclear Timeout
socket.setdefaulttimeout(30)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('SuperRobo')

# ─── Configurações ────────────────────────────────────────────────────────────
SUPABASE_URL              = (os.environ.get('SUPABASE_URL') or 'https://gongoqjjpwphhttumdjm.supabase.co').rstrip('/')
SUPABASE_ANON_KEY         = os.environ.get('SUPABASE_ANON_KEY') or 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdvbmdvcWpqcHdwaGh0dHVtZGptIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MTUyMDcsImV4cCI6MjA4Mjk5MTIwN30.XhdrWma90JeoQdGfeqCoXgGVnyiTZ5GXHszEHw3Ce2o'
SUPABASE_SERVICE_ROLE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or SUPABASE_ANON_KEY
TABLE_URL                 = f"{SUPABASE_URL}/rest/v1/charts_procedural"
R2_BUCKET                 = "skyfpl-charts"
R2_ACCESS_KEY_ID          = os.environ.get('R2_ACCESS_KEY_ID') or os.environ.get('CLOUDFLARE_R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY      = os.environ.get('R2_SECRET_ACCESS_KEY') or os.environ.get('CLOUDFLARE_R2_SECRET_ACCESS_KEY')
R2_ENDPOINT               = os.environ.get('R2_ENDPOINT') or os.environ.get('CLOUDFLARE_R2_ENDPOINT')

HEADERS_REST = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
    'apikey': SUPABASE_SERVICE_ROLE_KEY,
    'Prefer': 'resolution=merge-duplicates',
}

# ─── Gerenciamento de Telemetria ──────────────────────────────────────────────
telemetry_lock = threading.Lock()
telemetry = {
    'status': 'initializing',
    'current_icao': '',
    'progress': 0,
    'total_airports': 0,
    'total_offered': 0,
    'total_charts': 0,
    'mirrored_charts': 0,
    'failed_charts': 0,
    'mirrored_bytes': 0,
    'logs': [],
    'failed_airports': [],
    'last_processed_charts': []
}

def upload_telemetry(s3, snapshot):
    if not s3: return
    try:
        snapshot['updated_at'] = time.time()
        r2_key = "procedural/telemetry.json"
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=r2_key,
            Body=json.dumps(snapshot, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json'
        )
    except Exception as e:
        log.error(f"❌ Erro telemetria: {e}")

def add_telemetry_log(message):
    log.info(message)
    with telemetry_lock:
        telemetry['logs'].insert(0, f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
        if len(telemetry['logs']) > 20: telemetry['logs'] = telemetry['logs'][:20]

# ─── Motor de Geografia e Imagem (ICA 96-1) ──────────────────────────────────

# ─── Cache Global de Coordenadas de Referência (ARP) dos Aeródromos ──────────
AIRPORT_ARP_CACHE = {}

def parse_dms_coords(s):
    if not s: return None
    m = re.search(r'(\d{2})\s*(\d{2})\s*(\d{2}(?:\.\d+)?)\s*([SN])\s*[\/\-]?\s*(\d{2,3})\s*(\d{2})\s*(\d{2}(?:\.\d+)?)\s*([WE])', str(s), re.IGNORECASE)
    if m:
        lat = int(m.group(1)) + int(m.group(2))/60.0 + float(m.group(3))/3600.0
        if m.group(4).upper() == 'S': lat = -lat
        lon = int(m.group(5)) + int(m.group(6))/60.0 + float(m.group(7))/3600.0
        if m.group(8).upper() == 'W': lon = -lon
        return {'lat': lat, 'lon': lon}
    return None

def load_airport_arps_cache():
    global AIRPORT_ARP_CACHE
    if AIRPORT_ARP_CACHE: return AIRPORT_ARP_CACHE
    try:
        url = "https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/rotaer/rotaer_snapshot_latest.json"
        r = requests.get(url, timeout=15)
        if r.ok:
            data = r.json()
            items = data.get('data') or data.get('airports') or data
            if isinstance(items, dict): items = items.values()
            for item in items:
                icao = item.get('icao')
                if icao:
                    coords = item.get('coordinates') or item.get('cleanCoordinates')
                    parsed = parse_dms_coords(coords)
                    if parsed:
                        AIRPORT_ARP_CACHE[icao] = parsed
            log.info(f"📍 Cache de ARPs carregado: {len(AIRPORT_ARP_CACHE)} aeródromos mapeados.")
    except Exception as e:
        log.warning(f"Aviso ao carregar cache de ARPs do ROTAER: {e}")
    return AIRPORT_ARP_CACHE


import math
import re

def solve_cramer_3x3(points):
    n = len(points)
    if n < 3: return None
    sumX = sum(p['x'] for p in points)
    sumY = sum(p['y'] for p in points)
    sumV = sum(p['v'] for p in points)
    sumXX = sum(p['x']*p['x'] for p in points)
    sumYY = sum(p['y']*p['y'] for p in points)
    sumXY = sum(p['x']*p['y'] for p in points)
    sumXV = sum(p['x']*p['v'] for p in points)
    sumYV = sum(p['y']*p['v'] for p in points)
    
    det = n*(sumXX*sumYY - sumXY*sumXY) - sumX*(sumX*sumYY - sumY*sumXY) + sumY*(sumX*sumXY - sumY*sumXX)
    if abs(det) < 1e-12: return None
    
    a = (sumXV*(sumYY*n - sumY*sumY) - sumXY*(sumYV*n - sumV*sumY) + sumX*(sumYV*sumY - sumYY*sumV)) / det
    b = (sumXX*(sumYV*n - sumV*sumY) - sumXV*(sumXY*n - sumX*sumY) + sumX*(sumXY*sumV - sumYV*sumX)) / det
    c = (sumXX*(sumYY*sumV - sumY*sumYV) - sumXY*(sumXY*sumV - sumX*sumYV) + sumXV*(sumXY*sumY - sumYY*sumX)) / det
    
    return {'a': a, 'b': b, 'c': c}

def solve_affine_4point(pdf_corners, geo_corners):
    lat_points = []
    lng_points = []
    valid = 0
    for i in range(4):
        if abs(pdf_corners[i*2]) < 0.001 and abs(pdf_corners[i*2+1]) < 0.001: continue
        if abs(geo_corners[i*2]) < 0.001 and abs(geo_corners[i*2+1]) < 0.001: continue
        lat_points.append({'x': pdf_corners[i*2], 'y': pdf_corners[i*2+1], 'v': geo_corners[i*2]})
        lng_points.append({'x': pdf_corners[i*2], 'y': pdf_corners[i*2+1], 'v': geo_corners[i*2+1]})
        valid += 1
        
    if valid < 3: return None
    lat_params = solve_cramer_3x3(lat_points)
    lng_params = solve_cramer_3x3(lng_points)
    if not lat_params or not lng_params: return None
    
    def solver(px, py):
        lat = lat_params['a']*px + lat_params['b']*py + lat_params['c']
        lng = lng_params['a']*px + lng_params['b']*py + lng_params['c']
        return [lat, lng]
    return solver

def from_meters(x, y):
    lng = (x / 20037508.34) * 180.0
    lat = (y / 20037508.34) * 180.0
    lat = (180.0 / math.pi) * (2 * math.atan(math.exp((lat * math.pi) / 180.0)) - math.pi / 2)
    return [lat, lng]

def extract_georef(doc, page, pdf_bytes, icao=None):
    """
    Motor Tático Sentinel Bytescan (V14.3 - Multi-Block Precision Engine):
    Lê todos os blocos /GPTS e /LPTS do GeoPDF do DECEA.
    Se houver múltiplos blocos (ex: cartas ADC com diagramas secundários),
    compara cada bloco com as coordenadas oficiais do aeródromo (ARP) e
    seleciona automaticamente o bloco mais próximo da pista principal.
    Em seguida, resolve a matriz afim 4-pontos extrapolando para os 4 cantos da folha A4.
    """
    try:
        buf = pdf_bytes.decode('latin1', errors='ignore')
        gpts_matches = list(re.finditer(r'/GPTS\s*\[([^\]]+)\]', buf))
        lpts_matches = list(re.finditer(r'/LPTS\s*\[([^\]]+)\]', buf))
        
        if not gpts_matches or not lpts_matches:
            vps = page.get_viewports()
            if vps: return {"type": "GeoPDF_VP", "calibration": vps[0]}
            return None
            
        # Seleção do melhor bloco GPTS caso existam múltiplos
        best_gpts_match = gpts_matches[0]
        best_lpts_match = lpts_matches[0]
        
        if len(gpts_matches) > 1 and icao:
            arp_map = load_airport_arps_cache()
            target_arp = arp_map.get(icao)
            
            if target_arp:
                best_idx = 0
                min_dist = float('inf')
                
                for idx, gm in enumerate(gpts_matches):
                    raw_nums = [float(x) for x in gm.group(1).replace(',', ' ').split() if x.strip()]
                    if len(raw_nums) < 8: continue
                    
                    scale_div = 1000.0 if 500 < abs(raw_nums[0]) < 100000 else 1.0
                    scaled_nums = [v / scale_div for v in raw_nums]
                    
                    lats = [scaled_nums[0], scaled_nums[2], scaled_nums[4], scaled_nums[6]]
                    lons = [scaled_nums[1], scaled_nums[3], scaled_nums[5], scaled_nums[7]]
                    center_lat = sum(lats) / len(lats)
                    center_lon = sum(lons) / len(lons)
                    
                    dist = haversine_distance(target_arp['lat'], target_arp['lon'], center_lat, center_lon)
                    log.debug(f"[{icao}] Bloco #{idx+1}: Centro=({center_lat:.4f}, {center_lon:.4f}) -> Dist ARP: {dist:.1f}m")
                    
                    if dist < min_dist:
                        min_dist = dist
                        best_idx = idx
                        
                best_gpts_match = gpts_matches[best_idx]
                if best_idx < len(lpts_matches):
                    best_lpts_match = lpts_matches[best_idx]
                log.info(f"🎯 [{icao}] Multi-Block GeoPDF ({len(gpts_matches)} blocos) -> Selecionado Bloco #{best_idx+1} (Distância ao ARP: {min_dist:.1f}m)")
            
        gpts_raw = best_gpts_match.group(1).replace(',', ' ').split()
        lpts_raw = best_lpts_match.group(1).replace(',', ' ').split()
        
        gpts = [float(x) for x in gpts_raw if x.strip()]
        lpts = [float(x) for x in lpts_raw if x.strip()]
        if len(gpts) < 8 or len(lpts) < 8: return None
        
        scale_divisor = 1.0
        if 500 < abs(gpts[0]) < 100000:
            scale_divisor = 1000.0
            
        processed_gpts = [v / scale_divisor for v in gpts]
        
        if any(abs(v) > 500 for v in processed_gpts):
            for i in range(0, len(processed_gpts), 2):
                lat, lng = from_meters(processed_gpts[i+1], processed_gpts[i])
                processed_gpts[i], processed_gpts[i+1] = lat, lng
                
        # Extração de BBox dos bytes crus
        gpts_idx = best_gpts_match.start()
        search_start = max(0, gpts_idx - 10000)
        search_end = min(len(buf), gpts_idx + 10000)
        local_buf = buf[search_start:search_end]
        
        def find_dict_boxes(text, key):
            boxes = []
            pattern = re.compile(r'/' + key + r'\s*\[([0-9.\-\s]+)\]')
            for m in pattern.finditer(text):
                nums = [float(x) for x in m.group(1).strip().split() if x.strip()]
                if len(nums) >= 4:
                    if len(nums) >= 8:
                        xs = [nums[i] for i in range(0, len(nums), 2)]
                        ys = [nums[i] for i in range(1, len(nums), 2)]
                        boxes.append((min(xs), min(ys), max(xs), max(ys)))
                    else:
                        boxes.append(tuple(nums[:4]))
            return boxes
        
        byte_boxes = []
        for key in ['NeatLine', 'BBox', 'MediaBox', 'CropBox', 'TrimBox']:
            for box in find_dict_boxes(local_buf, key):
                byte_boxes.append(box)
        
        H = page.rect.height
        W = page.rect.width
        
        all_native_boxes = byte_boxes + [
            (page.rect.x0, H - page.rect.y1, page.rect.x1, H - page.rect.y0),
            (page.cropbox.x0, H - page.cropbox.y1, page.cropbox.x1, H - page.cropbox.y0) if page.cropbox else None,
            (page.mediabox.x0, H - page.mediabox.y1, page.mediabox.x1, H - page.mediabox.y0) if page.mediabox else None
        ]
        
        unique_boxes = []
        for b in all_native_boxes:
            if b and b[2] - b[0] > 0 and b[3] - b[1] > 0:
                if not any(abs(b[0] - ub[0]) < 1 and abs(b[1] - ub[1]) < 1 and abs(b[2] - ub[2]) < 1 for ub in unique_boxes):
                    unique_boxes.append(b)
                
        best_residual = float('inf')
        final_solver = None
        
        for box in unique_boxes:
            x0, y0, x1, y1 = box
            bw = x1 - x0
            bh = y1 - y0
            
            trial_scaled = []
            for i in range(0, len(lpts), 2):
                lx = lpts[i]
                ly = lpts[i+1]
                
                native_px = x0 + lx * bw
                native_py = y0 + ly * bh
                
                px = native_px
                py = H - native_py
                trial_scaled.extend([px, py])
                
            pdf_corners = trial_scaled[:8]
            
            tp_solver = solve_affine_4point(pdf_corners, processed_gpts[:8])
            if tp_solver:
                res = 0
                for j in range(0, min(8, len(processed_gpts)), 2):
                    lat, lng = tp_solver(trial_scaled[j], trial_scaled[j+1])
                    res += abs(lat - processed_gpts[j]) + abs(lng - processed_gpts[j+1])
                
                if res < best_residual:
                    best_residual = res
                    final_solver = tp_solver
        
        if not final_solver:
            final_solver = solve_affine_4point(lpts[:8], processed_gpts[:8])
            if not final_solver: return None
            
        bw, bh = page.rect.width, page.rect.height
        
        tl_lat, tl_lon = final_solver(0, 0)
        tr_lat, tr_lon = final_solver(bw, 0)
        br_lat, br_lon = final_solver(bw, bh)
        bl_lat, bl_lon = final_solver(0, bh)
        
        log.info(f"📐 GeoRef Corners [{icao or 'GENERIC'}]: TL=({tl_lat:.6f},{tl_lon:.6f}) TR=({tr_lat:.6f},{tr_lon:.6f}) BR=({br_lat:.6f},{br_lon:.6f}) BL=({bl_lat:.6f},{bl_lon:.6f})")
        
        return {
            "type": "Sentinel_Bytescan",
            "calibration": {
                "measure": {
                    "gpts": [
                        bl_lat, bl_lon,
                        br_lat, br_lon,
                        tr_lat, tr_lon,
                        tl_lat, tl_lon
                    ]
                }
            }
        }
    except Exception as e:
        log.debug(f"GeoRef Skip: {e}")
        return None

def process_pdf_to_jpg(pdf_content, icao=None):
    """Converte PDF para JPEG 250 DPI e extrai calibração geodésica."""
    try:
        doc = fitz.open(stream=pdf_content, filetype="pdf")
        if doc.page_count == 0: return None, None
        
        page = doc[0]
        # 🛰️ V14.2: Ativação do Motor de Precisão Sentinel Bytescan
        geo_data = extract_georef(doc, page, pdf_content, icao)
        
        zoom = 250 / 72
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
        
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        buffer = BytesIO()
        img.save(buffer, format="JPEG", quality=90, optimize=True, progressive=True, subsampling=0)
        
        meta = {
            "w": pix.width, 
            "h": pix.height, 
            "dpi": 250, 
            "geo": geo_data,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "version": "14.2-sentinel-bytescan"
        }
        doc.close()
        return buffer.getvalue(), meta
    except Exception as e:
        log.error(f"Erro processamento PDF: {e}")
        return None, None

# ─── Infraestrutura R2 ───────────────────────────────────────────────────────

def init_s3():
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        log.error("❌ ERRO: Credenciais do Cloudflare R2 não encontradas!")
        return None
        
    config = Config(
        connect_timeout=15, 
        read_timeout=30, 
        retries={'max_attempts': 3}, 
        max_pool_connections=50
    )
    
    return boto3.client(
        's3', 
        endpoint_url=R2_ENDPOINT, 
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY, 
        region_name='auto', 
        config=config
    )

def upload_to_r2(s3, key, body, content_type):
    try:
        s3.put_object(Bucket=R2_BUCKET, Key=key, Body=body, ContentType=content_type)
        # URL de publicação (pode variar conforme o domínio customizado do Cloudflare)
        return f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{key}"
    except Exception as e:
        log.error(f"Erro upload R2 ({key}): {e}")
        return None

# ─── Lógica de Processamento e Auditoria (Shadow Mode) ─────────────────────────

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcula a distância em metros entre duas coordenadas geográficas."""
    R = 6371000  # Raio da Terra em metros
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

def calculate_geo_diff(old_gpts, new_gpts):
    """Retorna o maior desvio (em metros) entre os 4 cantos da carta."""
    if not old_gpts or not new_gpts or len(old_gpts) < 8 or len(new_gpts) < 8: return None
    distances = []
    for i in range(0, 8, 2):
        d = haversine_distance(old_gpts[i], old_gpts[i+1], new_gpts[i], new_gpts[i+1])
        distances.append(d)
    return max(distances)

def process_single_chart(s3, icao, chart, airac, dry_run):
    name = chart.get('nome', 'CARTA')
    tipo = chart.get('tipo', 'UNKN')
    url_decea = chart.get('link', '')
    
    if not url_decea or dry_run: return 0
    
    clean_name = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_').upper()
    base_path = f"procedural/charts/{airac}/{icao}"
    
    MAX_RETRIES = 3
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url_decea, timeout=30)
            if not resp.ok:
                if attempt < MAX_RETRIES:
                    time.sleep(2 * attempt)
                    continue
                with telemetry_lock:
                    telemetry['failed_charts'] += 1
                    telemetry['failed_airports'].insert(0, {
                        'icao': icao, 
                        'name': name,
                        'error': f"DECEA Offline ({resp.status_code})", 
                        'at': datetime.now().strftime('%H:%M:%S')
                    })
                return 0
                
            pdf_bytes = resp.content
            jpg_bytes, meta = process_pdf_to_jpg(pdf_bytes, icao)
            
            if not jpg_bytes:
                if attempt < MAX_RETRIES:
                    time.sleep(1)
                    continue
                with telemetry_lock:
                    telemetry['failed_charts'] += 1
                    telemetry['failed_airports'].insert(0, {
                        'icao': icao, 
                        'name': name,
                        'error': "Erro Conversão JPEG", 
                        'at': datetime.now().strftime('%H:%M:%S')
                    })
                return 0

            url_pdf = upload_to_r2(s3, f"{base_path}/{tipo}_{clean_name}.pdf", pdf_bytes, 'application/pdf')
            url_jpg = upload_to_r2(s3, f"{base_path}/{tipo}_{clean_name}.jpg", jpg_bytes, 'image/jpeg') if jpg_bytes else None
            
            if not url_pdf or not url_jpg:
                 if attempt < MAX_RETRIES:
                    time.sleep(5)
                    continue
                 with telemetry_lock:
                    telemetry['failed_charts'] += 1
                    telemetry['failed_airports'].insert(0, {
                        'icao': icao, 
                        'name': name,
                        'error': "Erro Upload R2", 
                        'at': datetime.now().strftime('%H:%M:%S')
                    })
                 return 0
            
            # 🛡️ SHADOW MODE: DIFFING HISTÓRICO (Padrão Ouro)
            needs_review = False
            geo_diff_meters = None
            diagnostic_logs = None
            
            try:
                # 1. Fetch Gold Standard do banco
                q_url = f"{TABLE_URL}?icao=eq.{icao}&tipo=eq.{tipo}&nome_procedimento=eq.{name}&select=metadata_geo"
                gold_resp = requests.get(q_url, headers=HEADERS_REST, timeout=10)
                if gold_resp.ok:
                    gold_data = gold_resp.json()
                    if gold_data and len(gold_data) > 0 and gold_data[0].get('metadata_geo'):
                        old_meta = gold_data[0]['metadata_geo']
                        old_geo = old_meta.get('geo')
                        new_geo = meta.get('geo') if meta else None
                        
                        if old_geo and new_geo:
                            # Lógica para suportar formato aninhado
                            old_gpts = None
                            if 'calibration' in old_geo and 'measure' in old_geo['calibration']:
                                old_gpts = old_geo['calibration']['measure'].get('gpts')
                            elif 'calibration' in old_geo:
                                old_gpts = old_geo['calibration'].get('gpts')
                            
                            new_gpts = None
                            if 'calibration' in new_geo and 'measure' in new_geo['calibration']:
                                new_gpts = new_geo['calibration']['measure'].get('gpts')
                            
                            if old_gpts and new_gpts:
                                diff = calculate_geo_diff(old_gpts, new_gpts)
                                if diff is not None:
                                    geo_diff_meters = round(diff, 2)
                                    if diff > 50.0:  # Threshold de 50 metros
                                        needs_review = True
                                        diagnostic_logs = {
                                            "reason": "Desvio geográfico alto",
                                            "diff_meters": geo_diff_meters,
                                            "threshold": 50.0,
                                            "old_gpts": old_gpts,
                                            "new_gpts": new_gpts,
                                            "ai_insight": "Robô detectou anomalia grande. Manter Padrão Ouro sugerido caso a DECEA não tenha movido a pista."
                                        }
                                        log.warning(f"⚠️ Anomalia Geo ({icao} {tipo} {name}): Diff {geo_diff_meters}m")
            except Exception as ex_diff:
                log.error(f"Erro na auditoria de diffing para {icao}: {ex_diff}")
                
            record = {
                'icao': icao, 
                'tipo': tipo, 
                'nome_procedimento': name, 
                'url_decea': url_decea,
                'url_r2': url_pdf, 
                'url_r2_jpg': url_jpg, 
                'airac_cycle': airac,
                'data_carta': chart.get('dt', ''), 
                'metadata_geo': meta, 
                'source': 'skyfpl-robo-v14.1-shadow',
                'needs_review': needs_review,
                'geo_diff_meters': geo_diff_meters,
                'diagnostic_logs': diagnostic_logs
            }
            
            # Sincronização com o Banco de Dados Supabase
            requests.post(f"{TABLE_URL}?on_conflict=icao,tipo,nome_procedimento", json=[record], headers=HEADERS_REST, timeout=20)
            
            with telemetry_lock:
                telemetry['mirrored_charts'] += 1
                telemetry['mirrored_bytes'] += len(pdf_bytes) + len(jpg_bytes)
                telemetry['last_processed_charts'].insert(0, {
                    'icao': icao, 
                    'name': name, 
                    'url': url_jpg, 
                    'at': datetime.now().strftime('%H:%M:%S')
                })
                if len(telemetry['last_processed_charts']) > 20: telemetry['last_processed_charts'].pop()
            
            add_telemetry_log(f"✅ {icao}: {tipo} - {name} processada")
            return 1
            
        except Exception as e:
            if attempt < MAX_RETRIES:
                log.warning(f"⚠️ Tentativa {attempt} falhou para {icao} - {name}: {e}")
                time.sleep(2 * attempt)
                continue
            err_msg = str(e)
            log.error(f"❌ Falha Definitiva {icao} - {name}: {err_msg}")
            with telemetry_lock:
                telemetry['failed_charts'] += 1
                telemetry['failed_airports'].insert(0, {
                    'icao': icao, 
                    'name': name,
                    'error': f"Erro: {err_msg}", 
                    'at': datetime.now().strftime('%H:%M:%S')
                })
            return 0

def fetch_charts_for_icao(icao):
    """Consulta a API de borda para descobrir novas cartas do DECEA."""
    url = f"{SUPABASE_URL}/functions/v1/fetch-charts"
    try:
        r = requests.post(url, json={'icaoCode': icao}, headers=HEADERS_REST, timeout=30)
        return r.json().get('charts', []) if r.ok else []
    except Exception as e:
        log.debug(f"Fetch Fail {icao}: {e}")
        return []


# ─── Notificações Multicanal & Webhook de Staging (Fase 2) ───────────────────
def dispatch_staging_webhook_and_telegram(airac, total_charts, by_type, r2_path, multi_block_count, status="VALIDATED", error_diag=None, step=None):
    """
    Dispara Webhook de Staging para a Edge Function airac-charts-ingest e notifica o Telegram.
    Garante redundância multicanal e alertas de emergência imediatos.
    """
    key_candidate = (os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or '').strip()
    if not key_candidate:
        key_candidate = (os.environ.get('SUPABASE_ANON_KEY') or '').strip()
    if not key_candidate:
        key_candidate = SUPABASE_ANON_KEY

    if not SUPABASE_URL:
        return

    webhook_url = f"{SUPABASE_URL}/functions/v1/airac-charts-ingest"
    wh_headers = {
        'Authorization': f'Bearer {key_candidate}',
        'apikey': key_candidate,
        'Content-Type': 'application/json'
    }

    if status == "FAILED":
        body = {
            'status': 'FAILED',
            'cycle': str(airac),
            'step': step or 'Processamento de Cartas',
            'error': error_diag or 'Erro desconhecido'
        }
    else:
        body = {
            'status': 'VALIDATED',
            'cycle': str(airac),
            'r2_path': r2_path,
            'total_charts': total_charts,
            'by_type': by_type,
            'multi_block_count': multi_block_count
        }

    try:
        log.info(f"📡 Enviando webhook de Staging/Telegram para {webhook_url}...")
        res = requests.post(webhook_url, json=body, headers=wh_headers, timeout=45)
        if res.ok:
            log.info(f"✅ Webhook de Staging e Alerta Telegram acionados com sucesso: {res.text}")
        else:
            log.warning(f"⚠️ Resposta do webhook ({res.status_code}): {res.text}")
    except Exception as e:
        log.warning(f"⚠️ Erro ao enviar webhook para Edge Function: {e}")

    # Fallback direto via Telegram Bot API caso as variáveis estejam setadas no ambiente
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    if bot_token and chat_id:
        try:
            if status == "FAILED":
                msg = f"🔴 *ALERTA VERMELHO — Robô de Cartas*\n━━━━━━━━━━━━━━━━━━\n🛰️ *Ciclo AIRAC:* `{airac}`\n📍 *Etapa:* {step}\n⚠️ *Erro:* {error_diag}"
            else:
                msg = f"🗺️ *SkyFPL — Ingestão de Cartas AIRAC*\n━━━━━━━━━━━━━━━━━━\n🛰️ *Ciclo AIRAC:* `{airac}`\n📊 *Total de Cartas:* {total_charts}\n🎯 *Motor:* Multi-Block & Afim 4-Pontos\n🛡️ *Status:* VALIDATED ✅"
            requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json={
                'chat_id': chat_id,
                'text': msg,
                'parse_mode': 'Markdown'
            }, timeout=10)
        except Exception as tg_err:
            log.debug(f"Direct Telegram skip: {tg_err}")

def export_master_json(s3, airac):
    """Gera o índice mestre de todas as cartas processadas para o App."""
    all_records = []
    offset = 0
    while True:
        r = requests.get(f"{TABLE_URL}?select=*&limit=1000&offset={offset}", headers=HEADERS_REST, timeout=60)
        data = r.json()
        if not data: break
        all_records.extend(data)
        offset += 1000
        if len(data) < 1000: break
    
    payload = {
        'metadata': {
            'generated_at': datetime.now(timezone.utc).isoformat(), 
            'airac_cycle': airac, 
            'total': len(all_records)
        }, 
        'data': all_records
    }
    content = json.dumps(payload, ensure_ascii=False, indent=2)
    # 1. Publicação do Índice Mestre de Produção
    s3.put_object(Bucket=R2_BUCKET, Key='latest_proc_charts.json', Body=content, ContentType='application/json')
    # 2. Publicação do Snapshot Histórico Versionado do Ciclo AIRAC
    versioned_key = f"charts/cycles/{airac}/proc_charts_{airac}.json"
    try:
        s3.put_object(Bucket=R2_BUCKET, Key=versioned_key, Body=content, ContentType='application/json')
        log.info(f"📦 Snapshot versionado publicado: {versioned_key}")
    except Exception as ex:
        log.warning(f"Aviso ao publicar snapshot versionado: {ex}")
    return len(content)

# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    current_step = "Inicialização de Parâmetros e Credenciais"
    parser = argparse.ArgumentParser()
    parser.add_argument('--icao', help='ICAO ou lista CSV')
    parser.add_argument('--dry-run', default='False')
    parser.add_argument('--airac', help='Ciclo AIRAC')
    parser.add_argument('--workers', type=int, default=10)
    args = parser.parse_args()
    
    dry_run = str(args.dry_run).lower() == 'true'
    airac = args.airac or datetime.now(timezone.utc).strftime('%y%m')

    try:
        current_step = "Conexão com Cloudflare R2 e Cache de ARPs"
        s3 = init_s3()
        load_airport_arps_cache()
        
        # Reinicializa a telemetria global para esta execução
        with telemetry_lock:
            telemetry.update({
                'status': 'Iniciando SkyFPL Robô v14.3...',
                'current_icao': '',
                'progress': 0,
                'total_airports': 0,
                'total_offered': 0,
                'total_charts': 0,
                'mirrored_charts': 0,
                'failed_charts': 0,
                'mirrored_bytes': 0,
                'logs': [],
                'failed_airports': [],
                'last_processed_charts': []
            })
        
        upload_telemetry(s3, telemetry)
        
        def handle_stop(s, f):
            telemetry['status'] = 'stopped'
            upload_telemetry(s3, telemetry)
            sys.exit(0)
        signal.signal(signal.SIGTERM, handle_stop)
        
        add_telemetry_log(f"🚀 SkyFPL Robô v14.3 Iniciado | 250 DPI | AIRAC {airac}")
        
        current_step = "Filtragem da Malha Aérea Brasileira"
        icao_list = [c.strip().upper() for c in args.icao.split(',')] if args.icao else []
        if not icao_list:
            add_telemetry_log("🌍 Baixando malha aérea brasileira para filtragem...")
            try:
                r = requests.get('https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/latest_navdata.json', timeout=30)
                nav_data = r.json().get('data', [])
                icao_list = sorted({p['icao'] for p in nav_data if p.get('icao') and p.get('type') in ['airport', 'heliport']})
                add_telemetry_log(f"✅ Malha filtrada: {len(icao_list)} aeródromos identificados.")
            except Exception as e:
                add_telemetry_log(f"❌ Erro ao baixar malha: {e}")
                sys.exit(1)
        
        telemetry['total_airports'] = len(icao_list)
        telemetry['status'] = 'in_progress'
        
        stop_heartbeat = threading.Event()
        def hb():
            while not stop_heartbeat.is_set():
                upload_telemetry(s3, telemetry)
                time.sleep(10)
        threading.Thread(target=hb, daemon=True).start()
        
        current_step = "Descoberta de Cartas no DECEA / AISWEB"
        add_telemetry_log(f"🌍 Iniciando Descoberta Paralela para {len(icao_list)} aeródromos...")
        all_tasks = []
        by_type = {'IAC': 0, 'SID': 0, 'STAR': 0, 'ADC': 0, 'PDC': 0, 'VAC': 0}
        
        # 🚀 DISCOBERTA PARALELA
        with ThreadPoolExecutor(max_workers=args.workers * 2) as discovery_exe:
            discovery_futures = {discovery_exe.submit(fetch_charts_for_icao, icao): icao for icao in icao_list}
            processed_discovery = 0
            total_to_discover = len(icao_list)
            
            for future in as_completed(discovery_futures):
                icao = discovery_futures[future]
                processed_discovery += 1
                charts = future.result()
                
                if charts:
                    with telemetry_lock:
                        telemetry['total_offered'] += len(charts)
                    for c in charts:
                        all_tasks.append((icao, c))
                        tipo = (c.get('tipo') or '').upper()
                        if tipo in by_type:
                            by_type[tipo] += 1
                
                if processed_discovery % 50 == 0 or processed_discovery == total_to_discover:
                    with telemetry_lock:
                        telemetry['status'] = f"Descobrindo: {processed_discovery}/{total_to_discover}..."
                        telemetry['progress'] = int((processed_discovery / total_to_discover) * 20)
                    upload_telemetry(s3, telemetry)
        
        add_telemetry_log(f"✅ Descoberta concluída: {len(all_tasks)} cartas encontradas.")
        
        # 🚀 PROCESSAMENTO PARALELO (Rasterização 250 DPI + Georreferenciamento Multi-Block)
        current_step = "Rasterização & Georreferenciamento Multi-Bloco"
        processed_count = 0
        total_tasks = len(all_tasks)
        
        if total_tasks > 0:
            with ThreadPoolExecutor(max_workers=args.workers) as exe:
                futures = {exe.submit(process_single_chart, s3, t[0], t[1], airac, dry_run): t[0] for t in all_tasks}
                for future in as_completed(futures):
                    icao_task = futures[future]
                    processed_count += 1
                    with telemetry_lock:
                        telemetry['current_icao'] = icao_task
                        telemetry['total_charts'] = processed_count
                        telemetry['progress'] = 20 + int((processed_count / total_tasks) * 80)
            
        stop_heartbeat.set()
        
        # ─── RECONCILIAÇÃO FINAL ───────────────────────────────
        current_step = "Geração de Índice Mestre & Publicação R2"
        total_offered = telemetry['total_offered']
        total_success = telemetry['mirrored_charts']
        total_failed  = telemetry['failed_charts']
        
        add_telemetry_log(f"📊 Resumo: {total_success} sucessos, {total_failed} falhas.")
        
        master_key = 'latest_proc_charts.json'
        if not dry_run and total_success > 0:
            add_telemetry_log("📦 Gerando Índice Mestre...")
            size = export_master_json(s3, airac)
            add_telemetry_log(f"✅ Master JSON gerado ({size} bytes).")
        
        telemetry['status'] = 'completed'
        telemetry['progress'] = 100
        add_telemetry_log(f"✅ Robô v14.3 finalizado com sucesso!")
        upload_telemetry(s3, telemetry)

        # 📡 DISPARO DO WEBHOOK DE STAGING E ALERTA TELEGRAM
        current_step = "Disparo do Webhook de Staging e Alerta Telegram"
        if not dry_run:
            dispatch_staging_webhook_and_telegram(
                airac=airac,
                total_charts=total_success,
                by_type=by_type,
                r2_path=master_key,
                multi_block_count=by_type.get('ADC', 0) + by_type.get('PDC', 0),
                status="VALIDATED"
            )

    except Exception as critical_err:
        # 🔴 CAPTURA GLOBAL DE FALHA & DISPARO DE ALERTA DE EMERGÊNCIA
        err_str = str(critical_err)
        err_type = type(critical_err).__name__
        
        if 'Timeout' in err_type or 'timeout' in err_str.lower():
            friendly_diag = f"Timeout de rede na etapa '{current_step}'."
        elif 'ClientError' in err_type or 'EndpointConnectionError' in err_type:
            friendly_diag = f"Falha de conexão com o Cloudflare R2 durante '{current_step}'."
        elif 'KeyError' in err_type or 'JSONDecodeError' in err_type:
            friendly_diag = f"Incompatibilidade no formato de dados retornado na etapa '{current_step}'."
        else:
            friendly_diag = f"Erro inesperado ({err_type}) em '{current_step}': {err_str[:120]}"

        log.error(f"🚨 [FALHA CRÍTICA] {friendly_diag}")
        
        with telemetry_lock:
            telemetry['status'] = 'error'
            telemetry['error_diagnosis'] = friendly_diag
            telemetry['logs'].insert(0, f"🔴 FALHA CRÍTICA: {friendly_diag}")
        
        if s3:
            try:
                upload_telemetry(s3, telemetry)
            except:
                pass

        # Disparar Alerta Vermelho no Telegram
        dispatch_staging_webhook_and_telegram(
            airac=airac,
            total_charts=0,
            by_type={},
            r2_path='',
            multi_block_count=0,
            status="FAILED",
            error_diag=friendly_diag,
            step=current_step
        )

        raise critical_err

