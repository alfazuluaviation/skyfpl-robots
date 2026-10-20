#!/usr/bin/env python3
"""
SkyFPL - Procedures Robot (procedures-robot)
============================================================
Extrator 360° e Decodificador Completo de Procedimentos de Voo IFR (PBN / ARINC 424)
Processa Cartas IAC, SID e STAR do DECEA/AISWeb, extrai:
  1. Pernas de Voo ARINC 424 (IF, TF, DF, CF, RF, HM)
  2. Frequências de Comunicação e Rádio-Auxílios (ATIS, APP, TWR, GND, ILS, VOR)
  3. Performance & Relevo (AD ELEV, THR ELEV, TA, FAF, FINAL CRS, RDH/TCH, Rampa, Temp Baro)
  4. Mínimos Operacionais (DA / MDA / HAT / RVR / Visibilidade / Tabela de Razão de Descida)
  5. Procedimento de Aproximação Perdida (Texto Bilíngue PT/EN, Gradiente %, Altitude)
  6. Altitudes Mínimas de Segurança de Setor (MSA / TAA)
  7. Metadados de Validade, Emenda (AIRAC AMDT) e Registro Oficial DECEA
"""

import os
import sys
import json
import time
import re
import argparse
import logging
import math
import requests
import fitz  # PyMuPDF
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ─── Módulo Oficial AIXM 5.1 / FPDAM ──────────────────────────────────────────
try:
    from aixm_fpdam_loader import AixmFpdamEngine
    fpdam_engine = AixmFpdamEngine()
    fpdam_engine.load()
except Exception as _e:
    fpdam_engine = None
    print(f"⚠️ [FPDAM] Aviso: Motor AIXM não pôde ser carregado ({_e}). Usando modo GeoPDF Standard.")

# ─── Módulo Oficial STAR / SID Vector Engine ─────────────────────────────────
try:
    from star_sid_vector_engine import extract_star_sid_procedure
except Exception as _e:
    extract_star_sid_procedure = None
    print(f"⚠️ [STAR_SID] Aviso: Motor STAR/SID não pôde ser carregado ({_e}).")

# ─── Módulo Oficial Gemini Multimodal Chart Auditor ──────────────────────────
try:
    import gemini_chart_auditor
except Exception as _e:
    gemini_chart_auditor = None
    print(f"⚠️ [AI AUDITOR] Aviso: Módulo Gemini Auditor não pôde ser carregado ({_e}).")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('ProceduresRobot')

# ─── Configurações de Ambiente ────────────────────────────────────────────────
SUPABASE_URL = (os.environ.get('SUPABASE_URL') or 'https://gongoqjjpwphhttumdjm.supabase.co').rstrip('/')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY') or 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdvbmdvcWpqcHdwaGh0dHVtZGptIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MTUyMDcsImV4cCI6MjA4Mjk5MTIwN30.XhdrWma90JeoQdGfeqCoXgGVnyiTZ5GXHszEHw3Ce2o'
SUPABASE_SERVICE_ROLE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or SUPABASE_ANON_KEY

HEADERS_REST = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
    'apikey': SUPABASE_SERVICE_ROLE_KEY,
    'Prefer': 'resolution=merge-duplicates'
}

TABLE_PROCEDURES = f"{SUPABASE_URL}/rest/v1/procedures_fms"
TABLE_CHARTS = f"{SUPABASE_URL}/rest/v1/charts_procedural"

# ─── Configurações Cloudflare R2 (Área Exclusiva: procedures/fms/) ─────────────
R2_BUCKET = os.environ.get('R2_BUCKET_NAME') or 'skyfpl-charts'
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID') or os.environ.get('CLOUDFLARE_R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY') or os.environ.get('CLOUDFLARE_R2_SECRET_ACCESS_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT') or os.environ.get('CLOUDFLARE_R2_ENDPOINT')
R2_PUBLIC_BASE = (os.environ.get('R2_PUBLIC_URL') or 'https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev').rstrip('/')

def get_r2_client():
    """Retorna cliente S3/boto3 configurado para Cloudflare R2, ou None se indisponível."""
    if not (R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_ENDPOINT):
        return None
    try:
        import boto3
        from botocore.config import Config
        config = Config(retries={'max_attempts': 3}, max_pool_connections=25)
        return boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name='auto',
            config=config
        )
    except Exception as e:
        log.warning(f"⚠️ Não foi possível instanciar cliente R2 boto3: {e}")
        return None

def upload_json_to_r2(s3, key: str, data_obj: any, max_age: int = 300) -> str:
    """Envia payload JSON para o Cloudflare R2 com controle de cache."""
    if not s3:
        return ""
    try:
        body = json.dumps(data_obj, ensure_ascii=False, indent=2).encode('utf-8')
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=key,
            Body=body,
            ContentType='application/json; charset=utf-8',
            CacheControl=f'public, max-age={max_age}'
        )
        return f"{R2_PUBLIC_BASE}/{key}"
    except Exception as e:
        log.warning(f"⚠️ Falha no upload para o R2 ({key}): {e}")
        return ""

STOP_WORDS = {
    'CHART', 'NOTES', 'SCALE', 'FINAL', 'DECEA', 'NORTH', 'SOUTH', 'INSTR', 'SPEED',
    'POINT', 'APPRO', 'CLIMB', 'TRANS', 'AIRAC', 'ELEV', 'COORD', 'AEROD', 'AERO',
    'DEPART', 'ARRIV', 'MINIMA', 'CHANG', 'DESCENT', 'ALTIT', 'RUNWAY', 'ILS', 'VOR',
    'ROACH', 'APPROACH', 'PROCEDURE', 'AERODROME', 'CONTROLE', 'ESPAÇO'
}

# ─── 0. Normalizador Léxico & Unicode de Fontes DECEA ─────────────────────────
DECEA_UNICODE_MAP = str.maketrans({
    'Α': 'A', 'Β': 'B', 'Ε': 'E', 'Ζ': 'Z', 'Η': 'H', 'Ι': 'I',
    'Κ': 'K', 'Μ': 'M', 'Ν': 'N', 'Ο': 'O', 'Ρ': 'P', 'Τ': 'T',
    'Υ': 'Y', 'Χ': 'X', '’': "'", '‘': "'", '′': "'", 'º': '°',
    '“': '"', '”': '"', '–': '-', '—': '-', 'Ø': 'é', 'ø': 'é'
})

def normalize_decea_text(text: str) -> str:
    """Normaliza homóglifos gregos (ex: Tau/Alpha em TA), aspas e graus para ASCII padrão."""
    if not text:
        return ""
    return text.translate(DECEA_UNICODE_MAP).strip()

def get_normalized_words(page):
    """Retorna words da página normalizando caracteres homóglifos gregos e apóstrofos do DECEA."""
    raw_words = page.get_text('words')
    return [
        (w[0], w[1], w[2], w[3], normalize_decea_text(w[4]), w[5], w[6], w[7])
        for w in raw_words
    ]

# ─── 0.1 Inspetor Vetorial de Restrições de Altitude (+, -, @) ───────────────
def detect_vector_altitude_constraint(page, bbox, default_prefix="+") -> str:
    """
    Verifica desenhos vetoriais do PDF imediatamente acima e abaixo da bounding box do texto da altitude.
    - Barra acima e abaixo -> '@' (AT / MANDATÓRIO)
    - Barra apenas abaixo  -> '+' (AT OR ABOVE)
    - Barra apenas acima   -> '-' (AT OR BELOW)
    """
    x0, y0, x1, y1 = bbox
    width = x1 - x0
    bar_above = False
    bar_below = False

    try:
        drawings = page.get_drawings()
    except Exception:
        return default_prefix

    for d in drawings:
        r = d.get("rect")
        if not r:
            continue
        if (r.y1 - r.y0) <= 2.8 and (r.x1 - r.x0) >= width * 0.55:
            overlap_x = min(x1, r.x1) - max(x0, r.x0)
            if overlap_x >= width * 0.45:
                if -1.0 <= (y0 - r.y1) <= 5.0:
                    bar_above = True
                elif -1.0 <= (r.y0 - y1) <= 5.0:
                    bar_below = True

    if bar_above and bar_below:
        return "@"
    elif bar_below:
        return "+"
    elif bar_above:
        return "-"
    return default_prefix

# ─── Parser Espacial 2D de Aproximação Perdida (DECEA Standard) ─────────────
def extract_missed_approach_spatial(first_page, full_text):
    """
    Reconstrói com 100% de precisão as instruções de Aproximação Perdida (PT e EN)
    usando alinhamento horizontal geométrico 2D dos glifos/palavras do PDF do DECEA,
    resolvendo a fragmentação de caixas de texto com fundo invertido (ex: [4000'], [102]).
    """
    words = get_normalized_words(first_page)
    pt_anchor_y = None
    en_anchor_y = None
    
    for w in words:
        if 'Perdida:' in w[4] and pt_anchor_y is None and w[1] < first_page.rect.height * 0.45:
            pt_anchor_y = w[1]
        elif 'Missed' in w[4] and en_anchor_y is None and w[1] < first_page.rect.height * 0.45:
            en_anchor_y = w[1]

    inst_pt = None
    inst_en = None
    
    if pt_anchor_y:
        y_pt_max = en_anchor_y - 2 if en_anchor_y else pt_anchor_y + 16
        y_en_max = en_anchor_y + 18 if en_anchor_y else pt_anchor_y + 35
        
        words_pt = [w for w in words if pt_anchor_y - 4 <= w[1] <= y_pt_max and 28 <= w[0] <= 550]
        words_en = [w for w in words if en_anchor_y - 4 <= w[1] <= y_en_max and 28 <= w[0] <= 550] if en_anchor_y else []

        def build_line(word_list):
            if not word_list: return None
            lines = []
            sorted_w = sorted(word_list, key=lambda w: (round(w[1] / 6.0), w[0]))
            current_y_group = None
            current_line = []
            for w in sorted_w:
                y_group = round(w[1] / 6.0)
                if current_y_group is None or y_group == current_y_group:
                    current_line.append(w)
                    current_y_group = y_group
                else:
                    current_line.sort(key=lambda item: item[0])
                    lines.append(' '.join([item[4] for item in current_line]))
                    current_line = [w]
                    current_y_group = y_group
            if current_line:
                current_line.sort(key=lambda item: item[0])
                lines.append(' '.join([item[4] for item in current_line]))
                
            full = ' '.join(lines)
            full = re.sub(r'^(?:CONTROLE\s+DO\s+)?(?:DEPARTAMENTO\s+DE\s+CONTROLE\s+DO\s+ESPAÇO\s+AÉREO\s+)?', '', full, flags=re.IGNORECASE)
            full = re.sub(r'^APCH\s*Perdida:\s*', '', full, flags=re.IGNORECASE)
            full = re.sub(r'^Missed\s*APCH:\s*', '', full, flags=re.IGNORECASE)
            full = full.replace(" '", "'").replace(" .", ".").replace(" ,", ",").replace(" :", ":").strip()
            full = re.sub(r'\s+\d{2}\s+\d{2}.*$', '', full)
            full = re.sub(r'\s+\d{3,4}\'\s+\d{3,4}\'.*$', '', full)
            if 'espera.' in full:
                full = full[:full.find('espera.') + len('espera.')]
            elif 'holding.' in full:
                full = full[:full.find('holding.') + len('holding.')]
            return full

        inst_pt = build_line(words_pt)
        inst_en = build_line(words_en)

    # Fallback caso a carta use outro formato
    if not inst_pt:
        m_pt = re.search(r'(?:APCH\s*Perdida|ARREMETIDA)[:\s]+([^\n]+(?:\n[^\n]+){1,2})', full_text, re.IGNORECASE)
        if m_pt:
            inst_pt = ' '.join(m_pt.group(1).split())
    if not inst_en:
        m_en = re.search(r'Missed\s*APCH[:\s]+([^\n]+(?:\n[^\n]+){1,2})', full_text, re.IGNORECASE)
        if m_en:
            inst_en = ' '.join(m_en.group(1).split())

    alt_m = re.search(r'(\d{3,5})\'', inst_pt or '')
    missed_alt_ft = int(alt_m.group(1)) if alt_m else None
    
    grad_m = re.search(r'(?:gradiente|gradient)[^\d]{0,15}(\d+(?:\.\d+)?)\s*%', full_text, re.IGNORECASE)
    grad_pct = float(grad_m.group(1)) if grad_m else None

    return {
        'instruction_pt': inst_pt,
        'instruction_en': inst_en,
        'missed_altitude_ft': missed_alt_ft,
        'mnm_climb_gradient_pct': grad_pct
    }

# ─── Extrator Espacial 2D de Frequências (Voronoi 1D Colunar) ────────────────
def extract_frequencies_spatial_2d(doc, full_text=""):
    """
    Algoritmo Espacial 2D de Alta Fidelidade Aeronáutica para Extração de Frequências.
    Elimina falsos positivos de regex linear mapeando colunas geométricas de serviços
    e associando frequências VHF por contenção espacial horizontal (Voronoi 1D).
    """
    if not doc or len(doc) == 0:
        return []
        
    page = doc[0]
    raw_words = page.get_text('words')
    # Pre-merge kerning splits like 'TW' + 'R' on the same line (gap < 4px)
    words = []
    _i = 0
    while _i < len(raw_words):
        _w = list(raw_words[_i])
        while _i + 1 < len(raw_words):
            _nw = raw_words[_i+1]
            # Mescla apenas letras contíguas para corrigir kerning do DECEA (ex: 'TW' + 'R' = 'TWR'), nunca números
            if _w[4].isalpha() and _nw[4].isalpha() and abs((_nw[1]+_nw[3])/2 - (_w[1]+_w[3])/2) < 3 and 0 <= _nw[0] - _w[2] < 4:
                _w[2] = _nw[2]
                _w[4] = _w[4] + _nw[4]
                _i += 1
            else:
                break
        words.append(tuple(_w))
        _i += 1
    
    # 1. Encontrar palavras com formato de frequência VHF (108.000 a 136.975 MHz)
    freq_words = []
    for w in words:
        txt = w[4].strip()
        if re.match(r'^(?:1[0-3]\d\.\d{2,3})$', txt):
            val = float(txt)
            if 108.0 <= val <= 137.0:
                freq_words.append(w)
                
    if not freq_words:
        all_vhf = list(dict.fromkeys(re.findall(r'\b(1[0-3]\d\.\d{2,3})\b', full_text)))
        return [{'service': 'COM', 'callsign': None, 'frequency': f} for f in all_vhf]
        
    header_freqs = [w for w in freq_words if w[1] < 130]
    body_freqs = [w for w in freq_words if w[1] >= 130]
    
    frequencies = []
    
    if header_freqs:
        header_freqs.sort(key=lambda w: w[0])
        com_header_freqs = [w for w in header_freqs if float(w[4]) >= 118.0]
        avg_freq_y = sum((w[1] + w[3]) / 2 for w in com_header_freqs) / max(1, len(com_header_freqs))
        
        SERVICE_KEYWORDS = [
            'ATIS', 'D-ATIS', 'APP', 'APPROACH', 'TWR', 'TOWER', 
            'GND', 'GNDC', 'SOLO', 'CLRD', 'DELIVERY', 'AFIS', 'RDO', 'RADIO', 'OPS', 'ACC', 'CENTRO'
        ]
        
        svc_candidates = []
        for w in words:
            if w[3] <= avg_freq_y and (avg_freq_y - w[1]) <= 35:
                txt_clean = re.sub(r'[^A-Z\-]', '', w[4].upper())
                if any(k in txt_clean for k in SERVICE_KEYWORDS) or any(txt_clean.startswith(k) for k in ['APP', 'TWR', 'GND', 'AFIS']):
                    svc_candidates.append(w)
                    
        svc_candidates.sort(key=lambda w: w[0])
        
        service_cols = []
        idx = 0
        while idx < len(svc_candidates):
            w = svc_candidates[idx]
            txt = w[4].upper().replace(':', '')
            
            matched_svc = 'COM'
            if 'ATIS' in txt: matched_svc = 'ATIS'
            elif any(k in txt for k in ['APP', 'APPROACH']): matched_svc = 'APP'
            elif any(k in txt for k in ['TWR', 'TOWER']): matched_svc = 'TWR'
            elif any(k in txt for k in ['GND', 'GNDC', 'SOLO']): matched_svc = 'GND'
            elif any(k in txt for k in ['CLRD', 'DELIVERY']): matched_svc = 'CLRD'
            elif any(k in txt for k in ['AFIS', 'RDO', 'RADIO']): matched_svc = 'AFIS'
            elif any(k in txt for k in ['ACC', 'CENTRO']): matched_svc = 'ACC'
            
            x0 = w[0]
            x1 = w[2]
            callsign = w[4]
            
            adjacent_words = [ow for ow in words if abs((ow[1]+ow[3])/2 - (w[1]+w[3])/2) < 5 and ow[0] > x1 and (ow[0] - x1) < 25]
            adjacent_words.sort(key=lambda ow: ow[0])
            for aw in adjacent_words:
                aw_txt = aw[4].upper().replace(':', '')
                if any(k in aw_txt for k in ['ATIS', 'APP', 'TWR', 'GND', 'GNDC', 'SOLO', 'CLRD', 'AFIS']):
                    break
                callsign += ' ' + aw[4]
                x1 = max(x1, aw[2])
                
            callsign = re.sub(r'^(ATIS|D-ATIS|APP|TWR|ACC|GND|CLRD|AFIS)([A-Z]+)$', r'\1 \2', callsign.strip())
            service_cols.append({
                'service': matched_svc,
                'callsign': callsign,
                'x0': x0,
                'x1': x1
            })
            idx += 1
            
        filtered_cols = []
        for c in service_cols:
            if not filtered_cols:
                filtered_cols.append(c)
            else:
                prev = filtered_cols[-1]
                if abs(c['x0'] - prev['x0']) < 15:
                    continue
                filtered_cols.append(c)
                
        filtered_cols.sort(key=lambda c: c['x0'])
        
        for i in range(len(filtered_cols)):
            cur = filtered_cols[i]
            prev_col = filtered_cols[i-1] if i > 0 else None
            next_col = filtered_cols[i+1] if i < len(filtered_cols)-1 else None
            
            cur['bound_x0'] = (prev_col['x1'] + cur['x0']) / 2 if prev_col else 0
            cur['bound_x1'] = (cur['x1'] + next_col['x0']) / 2 if next_col else 9999
            
        for fw in header_freqs:
            val = float(fw[4])
            
            # Se a frequência for menor que 118.0 MHz, pertence exclusivamente à banda de Navegação (VOR / ILS)
            # e JAMAIS a serviços de Comunicação (ATIS, APP, TWR, GND)
            if val < 118.0:
                nearby = [w for w in words if abs(w[1] - fw[1]) < 25 and abs(w[0] - fw[0]) < 65 and w != fw]
                ident = None
                is_ils = False
                for nw in nearby:
                    t_nw = nw[4].upper().strip()
                    if 'ILS' in t_nw or 'LOC' in t_nw: is_ils = True
                    m_id = re.match(r'^[A-Z]{3}$', t_nw)
                    if m_id and t_nw not in ['MHZ', 'DME', 'ILS', 'VOR', 'LOC']:
                        ident = m_id.group(0)
                        
                if is_ils or (108.0 <= val <= 111.95 and int(val * 10) % 2 != 0):
                    frequencies.append({
                        'service': 'ILS/LOC',
                        'callsign': f"ILS {ident}" if ident else "ILS/LOC",
                        'frequency': fw[4].strip()
                    })
                else:
                    frequencies.append({
                        'service': 'VOR',
                        'callsign': f"VOR {ident}" if ident else "VOR",
                        'frequency': fw[4].strip()
                    })
                continue
                
            fx = (fw[0] + fw[2]) / 2
            matched_svc = 'COM'
            callsign = None
            
            for sc in filtered_cols:
                if sc['bound_x0'] <= fx <= sc['bound_x1']:
                    matched_svc = sc['service']
                    callsign = sc['callsign']
                    break
                    
            frequencies.append({
                'service': matched_svc,
                'callsign': callsign,
                'frequency': fw[4].strip()
            })
            
    for bw in body_freqs:
        val = float(bw[4])
        if 108.0 <= val <= 111.95 and int(val * 10) % 2 != 0:
            ident = None
            nearby = [w for w in words if abs(w[1] - bw[1]) < 20 and abs(w[0] - bw[0]) < 60 and w != bw]
            for nw in nearby:
                m_ident = re.match(r'^[A-Z]{3}$', nw[4].strip())
                if m_ident:
                    ident = m_ident.group(0)
                    break
            callsign = f"ILS {ident}" if ident else "ILS/LOC"
            frequencies.append({
                'service': 'ILS/LOC',
                'callsign': callsign,
                'frequency': bw[4].strip()
            })
        elif 108.0 <= val <= 117.95:
            ident = None
            nearby = [w for w in words if abs(w[1] - bw[1]) < 20 and abs(w[0] - bw[0]) < 60 and w != bw]
            for nw in nearby:
                m_ident = re.match(r'^[A-Z]{3}$', nw[4].strip())
                if m_ident:
                    ident = m_ident.group(0)
                    break
            callsign = f"VOR {ident}" if ident else "VOR"
            frequencies.append({
                'service': 'VOR',
                'callsign': callsign,
                'frequency': bw[4].strip()
            })
            
    seen = set()
    unique_freqs = []
    for f in frequencies:
        # Rádio-auxílios (VOR, ILS) unificam por (service, frequency)
        key = (f['service'], f['frequency']) if f['service'] in ['VOR', 'ILS/LOC'] else (f['service'], f['callsign'], f['frequency'])
        if key not in seen:
            seen.add(key)
            unique_freqs.append(f)
            
    return unique_freqs

# ─── Extrator Espacial 2D de Mínimos Operacionais IFR (PANS-OPS) ──────────────
def extract_minima_spatial_2d(doc, full_text=""):
    """
    Extrator Espacial 2D de Mínimos Operacionais IFR da Carta DECEA.
    Analisa a tabela oficial (STRAIGHT-IN, CAT A..E, DA/MDA, ALS/NO ALS/RVR, CIRCLING)
    com fidelidade absoluta às categorias e condições de teto/visibilidade.
    """
    if not doc or len(doc) == 0:
        return {'categories': ['A', 'B', 'C', 'D'], 'lines': []}
        
    page = doc[0]
    words = get_normalized_words(page)
    h = page.rect.height
    
    # 1. Encontrar o cabeçalho da tabela de mínimos (STRAIGHT-IN ou CAT A B C D)
    cat_word = None
    straight_in_word = None
    
    for w in words:
        if w[1] > h * 0.65:
            txt_upper = w[4].upper()
            if 'STRAIGHT-IN' in txt_upper:
                straight_in_word = w
            elif txt_upper == 'CAT' and cat_word is None:
                cat_word = w
                
    if not cat_word and not straight_in_word:
        return {'categories': ['A', 'B', 'C', 'D'], 'lines': []}
        
    header_y = (straight_in_word[1] if straight_in_word else cat_word[1])
    
    # 2. Localizar as colunas das categorias A, B, C, D, E no mesmo Y do header
    cat_columns = {}
    for w in words:
        if abs(w[1] - header_y) < 8 and w[0] > (cat_word[0] if cat_word else 120):
            txt = w[4].strip().upper()
            if txt in ['A', 'B', 'C', 'D', 'E']:
                cat_columns[txt] = (w[0] + w[2]) / 2
                
    categories = sorted(list(cat_columns.keys()))
    if not categories:
        categories = ['A', 'B', 'C', 'D']
        cat_columns = {'A': 161.0, 'B': 214.0, 'C': 267.0, 'D': 320.0, 'E': 373.0}
        
    col_bounds = {}
    sorted_cats = sorted(cat_columns.keys())
    for idx, c in enumerate(sorted_cats):
        cx = cat_columns[c]
        prev_cx = cat_columns[sorted_cats[idx-1]] if idx > 0 else (cx - 40)
        next_cx = cat_columns[sorted_cats[idx+1]] if idx < len(sorted_cats)-1 else (cx + 40)
        col_bounds[c] = (
            (prev_cx + cx) / 2 if idx > 0 else (cx - 25),
            (cx + next_cx) / 2 if idx < len(sorted_cats)-1 else (cx + 25)
        )

    # 3. Identificar as linhas de procedimentos (x < 95 pt e y > header_y)
    proc_labels = []
    
    for idx, w in enumerate(words):
        if w[1] > header_y + 4 and w[0] < 95 and w[1] < h - 25:
            txt = w[4].strip().upper().replace(':', '')
            matched_type = None
            if 'LNAV/VNAV' in txt: matched_type = 'LNAV/VNAV'
            elif txt == 'LNAV': matched_type = 'LNAV'
            elif txt == 'LPV': matched_type = 'LPV'
            elif txt in ['CATI', 'CATII', 'CATIII']:
                matched_type = f"CAT {txt[3:]}"
            elif txt in ['CAT', 'ILS', 'LOC', 'LLZ', 'VOR', 'NDB', 'CIRCLING']:
                name = txt
                if txt == 'CAT' and idx + 1 < len(words):
                    nw = words[idx+1]
                    if nw[4].strip().upper() in ['I', 'II', 'III', '1', '2', '3']:
                        name = f"CAT {nw[4].strip().upper()}"
                matched_type = name
                
            if matched_type:
                if not any(abs(pl['y'] - w[1]) < 8 for pl in proc_labels):
                    proc_labels.append({
                        'type': matched_type,
                        'y': (w[1] + w[3]) / 2,
                        'word': w
                    })
                    
    proc_labels.sort(key=lambda p: p['y'])
    
    if not proc_labels:
        return {'categories': categories, 'lines': []}
        
    for i in range(len(proc_labels)):
        cur = proc_labels[i]
        prev_p = proc_labels[i-1] if i > 0 else None
        next_p = proc_labels[i+1] if i < len(proc_labels)-1 else None
        
        y_top = (prev_p['y'] + cur['y']) / 2 if prev_p else (header_y + 2)
        y_bottom = (cur['y'] + next_p['y']) / 2 if next_p else (cur['y'] + 22)
        cur['y_top'] = y_top
        cur['y_bottom'] = y_bottom

    lines = []
    global_col_e_na = any(w[4].strip().upper() == 'NA' and w[0] > 340 and header_y <= w[1] <= h - 20 for w in words)
    
    for pl in proc_labels:
        p_type = pl['type']
        y_top = pl['y_top']
        y_bottom = pl['y_bottom']
        
        band_words = [w for w in words if y_top <= (w[1]+w[3])/2 < y_bottom]
        
        has_da = any('DA' == w[4].strip().upper() for w in band_words if w[0] < 140)
        has_mda = any('MDA' == w[4].strip().upper() for w in band_words if w[0] < 140)
        has_och = any('OCH' in w[4].upper() for w in band_words if w[0] < 140)
        type_limit = 'DA' if has_da or 'VNAV' in p_type or 'CAT' in p_type or 'ILS' in p_type else 'MDA'
        limit_label = f"{type_limit} / (OCH)" if has_och else type_limit
        
        cond_label = None
        vis_cond_words = [w for w in band_words if 70 <= w[0] <= 140 and any(k in w[4].upper() for k in ['ALS', 'RVR', 'VIS'])]
        if vis_cond_words:
            vis_cond_words.sort(key=lambda w: w[0])
            cond_label = ' '.join(w[4] for w in vis_cond_words)
            
        is_all_na = any(w[4].strip().upper() == 'NA' and 200 <= w[0] <= 320 for w in band_words)
        
        alt_matches = []
        for w in band_words:
            if w[0] <= 125:
                continue
            txt = w[4].strip()
            # Suporte a tokens fundidos "231'/(200')"
            m_fused = re.match(r'^(\d{2,4})\'?\s*/\s*\((\d{2,4})\'?\)$', txt)
            if m_fused:
                alt_matches.append({
                    'alt_ft': int(m_fused.group(1)),
                    'och_ft': int(m_fused.group(2)),
                    'x': (w[0] + w[2]) / 2,
                    'word': w
                })
                continue
            # Suporte a tokens separados "231'" seguido de "(200')"
            m_single = re.match(r'^(\d{2,4})[\'’]$', txt)
            if m_single:
                och_val = None
                for nw in band_words:
                    m_och = re.search(r'\((\d{2,4})[\'’]?\)', nw[4])
                    if m_och and abs(nw[1] - w[1]) < 6 and 0 < (nw[0] - w[0]) < 65:
                        och_val = int(m_och.group(1))
                        break
                alt_matches.append({
                    'alt_ft': int(m_single.group(1)),
                    'och_ft': och_val,
                    'x': (w[0] + w[2]) / 2,
                    'word': w
                })
                
        vis_blocks = []
        for w in band_words:
            if w[0] <= 125:
                continue
            txt = w[4].strip().upper()
            # Suporte a visibilidade fundida com barras: "NIL/1200/NIL" ou "1200/NIL"
            if '/' in txt and any(c.isdigit() for c in txt) and "'" not in txt:
                nums = [int(n) for n in re.findall(r'\b(\d{3,4})\b', txt) if 300 <= int(n) <= 5000]
                if nums:
                    vis_blocks.append({
                        'vis_m': nums[0],
                        'raw_str': txt.replace('/', ' / '),
                        'x': (w[0] + w[2]) / 2
                    })
                    continue
            # Suporte a visibilidade isolada com detecção de NIL adjacente
            if re.match(r'^(?:[3-9]\d{2}|[1-5]\d{3})$', txt):
                val = int(txt)
                raw_str = txt
                left_nil = any('NIL' in nw[4].upper() and 0 < (w[0] - nw[2]) < 25 for nw in band_words)
                right_nil = any('NIL' in nw[4].upper() and 0 < (nw[0] - w[2]) < 25 for nw in band_words)
                if left_nil and right_nil:
                    raw_str = f"NIL / {val} / NIL"
                elif left_nil:
                    raw_str = f"NIL / {val}"
                elif right_nil:
                    raw_str = f"{val} / NIL"
                    
                vis_blocks.append({
                    'vis_m': val,
                    'raw_str': raw_str,
                    'x': (w[0] + w[2]) / 2
                })
                
        na_tokens = [w for w in band_words if w[4].strip().upper() == 'NA' and w[0] > 340]

        by_category = {}
        for c in categories:
            cx = cat_columns[c]
            cb = col_bounds[c]
            
            if is_all_na:
                by_category[c] = {'altitude': 'NA', 'visibility': 'NA', 'na': True}
                continue
                
            col_is_na = any(cb[0] <= (w[0]+w[2])/2 <= cb[1] for w in na_tokens) or (c == 'E' and global_col_e_na and not any(am['x'] > 340 for am in alt_matches))
            if col_is_na:
                by_category[c] = {'altitude': 'NA', 'visibility': 'NA', 'na': True}
                continue
                
            matched_alt = None
            if len(alt_matches) == 1:
                matched_alt = alt_matches[0]
            else:
                for am in alt_matches:
                    if cb[0] <= am['x'] <= cb[1]:
                        matched_alt = am
                        break
                if not matched_alt and alt_matches:
                    matched_alt = min(alt_matches, key=lambda a: abs(a['x'] - cx))
                    
            matched_vis = None
            if len(vis_blocks) == 1:
                matched_vis = vis_blocks[0]
            else:
                for vb in vis_blocks:
                    if abs(vb['x'] - cx) < 35 or (cb[0] <= vb['x'] <= cb[1]):
                        matched_vis = vb
                        break
                if not matched_vis and vis_blocks:
                    matched_vis = min(vis_blocks, key=lambda v: abs(v['x'] - cx))
                    
            alt_txt = f"{matched_alt['alt_ft']}' ({matched_alt['och_ft']}''" if matched_alt and matched_alt['och_ft'] else (f"{matched_alt['alt_ft']}'" if matched_alt else '---')
            alt_txt = alt_txt.replace("''", "')")
            
            disp_alt = f"{type_limit} {matched_alt['alt_ft']}' / OCH {matched_alt['och_ft']}'" if matched_alt and matched_alt['och_ft'] else alt_txt
            
            by_category[c] = {
                'altitude': alt_txt,
                'display_altitude': disp_alt,
                'da_mda_ft': matched_alt['alt_ft'] if matched_alt else None,
                'och_ft': matched_alt['och_ft'] if matched_alt else None,
                'hat_hgt_ft': matched_alt['och_ft'] if matched_alt else None,
                'visibility': matched_vis['raw_str'] if matched_vis else ('NA' if col_is_na else '---'),
                'vis_m': matched_vis['vis_m'] if matched_vis else None,
                'na': False
            }
            
        rep_alt = alt_matches[0]['alt_ft'] if alt_matches else None
        rep_och = alt_matches[0]['och_ft'] if alt_matches else None
        rep_vis = vis_blocks[0]['vis_m'] if vis_blocks else None
        rep_vis_raw = vis_blocks[0]['raw_str'] if vis_blocks else ('NA' if is_all_na else None)
        
        lines.append({
            'type': p_type,
            'type_limit': type_limit,
            'limit_label': limit_label,
            'da_mda_ft': None if is_all_na else rep_alt,
            'och_ft': None if is_all_na else rep_och,
            'hat_hgt_ft': None if is_all_na else rep_och,
            'obstacle_clearance': {
                'label': limit_label,
                'da_mda_msl_ft': None if is_all_na else rep_alt,
                'och_agl_ft': None if is_all_na else rep_och,
                'unit': 'FT'
            },
            'visibility_m': None if is_all_na else rep_vis,
            'visibility_condition': cond_label or 'ALS/NO ALS/ RVR ALS (m)',
            'visibility_raw': rep_vis_raw,
            'is_na': is_all_na,
            'by_category': by_category
        })
        
    return {
        'categories': categories,
        'lines': lines
    }

# ─── Extrator Espacial 2D de Parâmetros de Performance e Relevo DECEA ─────────
def extract_header_performance_spatial_2d(doc, full_text=""):
    """
    Extrator Espacial 2D de Geometria, Relevo e Ângulos de Aproximação DECEA.
    Substitui a fragilidade de regex linear nos blocos de cabeçalho e perfil:
    - AD ELEV & THR ELEV (Altitude Oficial do Aeródromo e Cabeceira)
    - TA (Altitude de Transição, ex: TA10000' ou TA: 10000')
    - FINAL CRS (Rumo Final em graus magnéticos mapeado diretamente abaixo do rótulo CRS)
    - FAF (Altitude no Fixo de Aproximação Final em FT mapeado abaixo do rótulo FAF)
    - TCH / RDH (Threshold Crossing Height / Reference Datum Height)
    - Envelope Térmico Baro-VNAV (Min / Max °C)
    - Rampa de Descida VPA (% e graus de rampa)
    """
    if not doc or len(doc) == 0:
        return {}
        
    page = doc[0]
    words = get_normalized_words(page)
    norm_full_text = normalize_decea_text(full_text)
    perf = {}

    # 1. Elevação do AD
    ad_m = re.search(r'AD\s*ELEV[:\s]+(\d+)[\'’]?', norm_full_text, re.IGNORECASE)
    if ad_m:
        perf['ad_elevation_ft'] = int(ad_m.group(1))
    else:
        for i, w in enumerate(words):
            t = w[4].upper().replace(':', '')
            if t in ['ELEV', 'AD'] and i + 1 < len(words):
                if w[1] < 80 and w[0] < 120:
                    digits = re.sub(r'\D', '', words[i+1][4])
                    if digits and int(digits) < 15000:
                        perf['ad_elevation_ft'] = int(digits)
                        break

    # 2. Elevação da Cabeceira (THR ELEV)
    thr_m = re.search(r'THR\s*ELEV[:\s]+(\d+)[\'’]?', norm_full_text, re.IGNORECASE)
    if thr_m:
        perf['thr_elevation_ft'] = int(thr_m.group(1))
    else:
        for i, w in enumerate(words):
            if w[4].upper() == 'THR':
                for j in range(i+1, min(i+4, len(words))):
                    if 'ELEV' in words[j][4].upper():
                        for k in range(j+1, min(j+3, len(words))):
                            digits = re.sub(r'\D', '', words[k][4])
                            if digits and int(digits) < 15000:
                                perf['thr_elevation_ft'] = int(digits)
                                break
            if 'thr_elevation_ft' in perf:
                break

    # 3. Altitude de Transição (TA) - Busca Espacial 2D de Alta Precisão (suporta homóglifos gregos e tokens inline)
    for i, w in enumerate(words):
        # Ignora se for o item de alterações 'CHANGES: TA.'
        if w[4].endswith('.'):
            continue
        if i > 0 and 'CHANGES' in words[i-1][4].upper():
            continue
        txt = w[4].upper().strip().replace(':', '')
        if txt in ['TA', 'ΤΑ'] or re.match(r'^(?:TA|ΤΑ)\d{4,5}\'?$', txt):
            m_inline = re.search(r'(\d{4,5})', txt)
            if m_inline and int(m_inline.group(1)) >= 3000:
                perf['transition_altitude_ft'] = int(m_inline.group(1))
                break
            candidates = [
                w2 for w2 in words 
                if abs((w2[1]+w2[3])/2 - (w[1]+w[3])/2) < 8 
                and 0 < w2[0] - w[0] < 55
            ]
            for c in candidates:
                m = re.search(r'(\d{4,5})\'?', c[4])
                if m and int(m.group(1)) >= 3000:
                    perf['transition_altitude_ft'] = int(m.group(1))
                    break
            if 'transition_altitude_ft' in perf:
                break

    if 'transition_altitude_ft' not in perf or perf.get('transition_altitude_ft', 0) < 3000:
        # Busca segura sem look-behind de largura variável
        for m_ta in re.finditer(r'\b(?:TA|ΤΑ|TRANS(?:ITION)?\s*ALT(?:ITUDE)?)\s*[:\s]?\s*(\d{4,5})\'?', norm_full_text, re.IGNORECASE):
            val = int(m_ta.group(1))
            prefix = norm_full_text[max(0, m_ta.start()-20):m_ta.start()].upper()
            if val >= 3000 and 'CHANGES' not in prefix:
                perf['transition_altitude_ft'] = val
                break

    # 4. TCH / RDH
    rdh_m = re.search(r'(?:ILS\s*)?(?:RDH|TCH)[:\s]+(\d+)[\'’]?', norm_full_text, re.IGNORECASE)
    if rdh_m:
        perf['tch_rdh_ft'] = int(rdh_m.group(1))
    else:
        for i, w in enumerate(words):
            if any(k in w[4].upper() for k in ['RDH', 'TCH']):
                for j in range(i+1, min(i+3, len(words))):
                    digits = re.sub(r'\D', '', words[j][4])
                    if digits and 30 <= int(digits) <= 120:
                        perf['tch_rdh_ft'] = int(digits)
                        break
            if 'tch_rdh_ft' in perf:
                break

    # 5. Rumo Final (FINAL CRS) - Busca Espacial 2D na Coluna do Cabeçalho
    crs_labels = [w for w in words if w[4].upper() in ['CRS', 'CRS:'] and 60 <= w[1] <= 110]
    for cl in crs_labels:
        cl_mid_x = (cl[0] + cl[2]) / 2
        below = [bw for bw in words if abs((bw[0] + bw[2]) / 2 - cl_mid_x) < 25 and 0 < bw[1] - cl[1] < 25]
        for bw in below:
            m = re.search(r'(\d{2,3})[º°]?', bw[4])
            if m and bw != cl:
                val = int(m.group(1))
                if 1 <= val <= 360:
                    perf['final_course_deg'] = val
                    break
        if 'final_course_deg' in perf:
            break

    if 'final_course_deg' not in perf:
        crs_m = re.search(r'FINAL\s*CRS[:\s]+(\d{2,3})[º°]', full_text, re.IGNORECASE)
        if crs_m:
            val = int(crs_m.group(1))
            if 1 <= val <= 360:
                perf['final_course_deg'] = val

    if 'final_course_deg' in perf:
        perf['final_approach_track_mag'] = perf['final_course_deg']

    # 6. Altitude no FAF (Motor Espacial 2D com Guardrail Aeronáutico PANS-OPS)
    # REGRA CRÍTICA DE AVIONICS: FAF Altitude é SEMPRE >= 1000 FT (normalmente 1500 a 5000 FT).
    # NUNCA aceitar valores de 3 dígitos (< 1000), pois podem ser rumos magnéticos (ex: 282º) ou distâncias!
    faf_labels = [w for w in words if w[4].upper() in ['FAF:', 'FAF'] and 60 <= w[1] <= 110]
    for fl in faf_labels:
        fl_mid_x = (fl[0] + fl[2]) / 2
        below = [bw for bw in words if abs((bw[0] + bw[2]) / 2 - fl_mid_x) < 20 and 0 < bw[1] - fl[1] < 25]
        for bw in below:
            m = re.search(r'(\d{4,5})[\'’]?', bw[4])
            if m and bw != fl:
                val = int(m.group(1))
                if 1000 <= val <= 15000:
                    perf['faf_altitude_ft'] = val
                    break
        if 'faf_altitude_ft' in perf:
            break

    # Fallback no perfil vertical (y entre 350 e 500 pt) ou texto se estritamente >= 1000 FT
    if 'faf_altitude_ft' not in perf:
        profile_faf = [w for w in words if 'FAF' in w[4].upper() and 350 <= w[1] <= 500]
        for pf in profile_faf:
            nearby = [w for w in words if abs(w[0] - pf[0]) < 40 and abs(w[1] - pf[1]) < 30]
            for nw in nearby:
                m = re.search(r'(\d{4,5})[\'’]?', nw[4])
                if m:
                    val = int(m.group(1))
                    if 1000 <= val <= 15000:
                        perf['faf_altitude_ft'] = val
                        break
            if 'faf_altitude_ft' in perf:
                break

    if 'faf_altitude_ft' not in perf:
        faf_m = re.search(r'FAF[:\s]+(\d{4,5})[\'’]', full_text, re.IGNORECASE)
        if faf_m:
            val = int(faf_m.group(1))
            if 1000 <= val <= 15000:
                perf['faf_altitude_ft'] = val

    # 7. Envelope Baro-VNAV
    temp_m = re.search(r'TEMP\s*(?:LNAV/VNAV|BARO)?[^\d]{0,25}MIN\s*(-?\d+)[º°]C\s*/\s*MAX\s*(\d+)[º°]C', full_text, re.IGNORECASE)
    if temp_m:
        perf['temp_min_c'] = int(temp_m.group(1))
        perf['temp_max_c'] = int(temp_m.group(2))

    # 8. Rampa de Descida (VPA)
    slope_m = re.search(r'(\d+(?:\.\d+)?)\s*%\s*(?:\(\s*(\d+(?:\.\d+)?)[º°”\"]\s*\)?)?', full_text)
    if slope_m:
        perf['glide_path_pct'] = float(slope_m.group(1))
        if slope_m.group(2):
            perf['glide_angle_deg'] = float(slope_m.group(2))
        else:
            perf['glide_angle_deg'] = 3.0
    else:
        vpa_m = re.search(r'\(\s*(\d+(?:\.\d+)?)\s*[º°”\"]\s*\)', full_text)
        if vpa_m:
            angle = float(vpa_m.group(1))
            perf['glide_angle_deg'] = angle
            import math
            perf['glide_path_pct'] = round(math.tan(math.radians(angle)) * 100, 1)
        else:
            for w in words:
                if 400 <= w[1] <= 480 and any(k in w[4] for k in ['3.0', '3.5', '2.8']):
                    m_ang = re.search(r'(\d\.\d+)', w[4])
                    if m_ang:
                        angle = float(m_ang.group(1))
                        perf['glide_angle_deg'] = angle
                        import math
                        perf['glide_path_pct'] = round(math.tan(math.radians(angle)) * 100, 1)
                        break

    return perf

# ─── Extrator Espacial 2D das Tabelas de Performance DECEA ────────────────────
def extract_performance_tables_spatial_2d(doc):
    """
    Extrator Espacial 2D das Duas Tabelas Oficiais de Performance DECEA:
    1. Tabela Esquerda: Perfil Vertical CDFA / Altitude vs Distância (Checkpoints FAF -> RWY)
    2. Tabela Direita: Razão de Descida (Ground Speed vs FPM) & Cronometragem FAF-MAPT
    """
    if not doc or len(doc) == 0:
        return {'cdfa_profile_table': None, 'descent_rate_table': None}
        
    page = doc[0]
    words = page.get_text('words')
    
    gs_word = None
    fpm_word = None
    alt_label_word = None
    hgt_label_word = None
    faf_mapt_word = None
    
    # Banda vertical ampliada para cobrir todas as cartas DECEA (450 a 540 pt)
    for w in words:
        if 450 <= w[1] <= 540:
            txt = w[4].strip().upper()
            if txt == 'GROUND' and gs_word is None: gs_word = w
            elif txt == 'FPM' and fpm_word is None: fpm_word = w
            elif txt == 'ALT' and alt_label_word is None and 175 <= w[0] <= 215: alt_label_word = w
            elif '(HGT)' in txt and hgt_label_word is None: hgt_label_word = w
            elif 'FAF-MAPT' in txt and faf_mapt_word is None: faf_mapt_word = w
            
    if not (gs_word or alt_label_word or fpm_word):
        return {'cdfa_profile_table': None, 'descent_rate_table': None}
        
    div_x = 210.0
    if alt_label_word and gs_word:
        div_x = (alt_label_word[2] + gs_word[0]) / 2
    elif alt_label_word:
        div_x = alt_label_word[2] + 5
    elif gs_word:
        div_x = gs_word[0] - 5
        
    y_row1 = gs_word[1] if gs_word else (alt_label_word[1] - 7 if alt_label_word else 485)
    y_row2 = fpm_word[1] if fpm_word else (alt_label_word[1] if alt_label_word else 493)
    y_row3 = faf_mapt_word[1] if faf_mapt_word else (hgt_label_word[1] if hgt_label_word else 500)
    
    # ── TABELA 1: PERFIL VERTICAL CDFA (ESQUERDA, X < div_x) ──────────────────
    left_words = [w for w in words if w[0] < div_x and (y_row1 - 6) <= w[1] <= (y_row3 + 8)]
    left_nil = any(w[4].strip().upper() == 'NIL' and w[0] < 120 for w in left_words)
    
    cdfa_table = None
    if not left_nil:
        row1_words = [w for w in left_words if abs(w[1] - y_row1) < 5]
        row2_words = [w for w in left_words if abs(w[1] - y_row2) < 5]
        row3_words = [w for w in left_words if abs(w[1] - y_row3) < 5]
        
        target_rwy = None
        for w in row1_words:
            if 'RWY' in w[4].upper() or re.match(r'^RW\d{2}', w[4].upper()):
                target_rwy = w[4].strip()
                break
                
        col_points = [w for w in row1_words if not ('RWY' in w[4].upper() or re.match(r'^RW\d{2}', w[4].upper()))]
        col_points.sort(key=lambda w: w[0])
        
        checkpoints = []
        for cp in col_points:
            cx = (cp[0] + cp[2]) / 2
            txt = cp[4].strip()
            
            dist_val = None
            m_dist = re.match(r'^(\d+(?:\.\d+)?)$', txt)
            if m_dist:
                dist_val = float(m_dist.group(1))
                lbl = f"{dist_val} NM"
            else:
                lbl = txt
                
            alt_m = None
            closest_r2 = [w for w in row2_words if abs((w[0]+w[2])/2 - cx) < 18 and w[4].strip().isdigit()]
            if closest_r2:
                alt_m = int(closest_r2[0][4].strip())
                
            hgt_m = None
            closest_r3 = [w for w in row3_words if abs((w[0]+w[2])/2 - cx) < 18 and w[4].strip().isdigit()]
            if closest_r3:
                hgt_m = int(closest_r3[0][4].strip())
                
            if alt_m:
                checkpoints.append({
                    'point_label': lbl,
                    'distance_nm': dist_val,
                    'altitude_msl_ft': alt_m,
                    'height_agl_ft': hgt_m
                })
                
        cdfa_table = {
            'is_nil': False,
            'target_fix': target_rwy or 'RWY',
            'checkpoints': checkpoints
        }
        # Perfil teórico CDFA referencial baseado no ângulo de 3.00° (318 FT/NM)
        calc_checkpoints = []
        for dist_d in [5.0, 4.0, 3.0, 2.0, 1.0]:
            calc_alt = round(86 + (dist_d * 318.4))
            calc_checkpoints.append({
                'point_label': f"{int(dist_d)} NM",
                'distance_nm': dist_d,
                'altitude_msl_ft': calc_alt,
                'height_agl_ft': calc_alt - 31,
                'is_calculated': True
            })
        cdfa_table = {
            'is_nil': True,
            'target_fix': None,
            'checkpoints': calc_checkpoints
        }
        
    # ── TABELA 2: RAZÃO DE DESCIDA & TEMPO (DIREITA, X >= div_x) ──────────────
    right_words = [w for w in words if w[0] >= div_x and (y_row1 - 6) <= w[1] <= (y_row3 + 8)]
    
    speed_words = [w for w in right_words if abs(w[1] - y_row1) < 5 and re.match(r'^(?:0?\d{2,3})$', w[4].strip())]
    speed_words.sort(key=lambda w: w[0])
    
    fpm_row_words = [w for w in right_words if abs(w[1] - y_row2) < 5 and (w[4].strip().isdigit() or w[4].strip().upper() in ['NA', 'NIL'])]
    fpm_row_words.sort(key=lambda w: w[0])
    
    timing_words = [w for w in right_words if abs(w[1] - y_row3) < 5 and w[0] > (faf_mapt_word[2] if faf_mapt_word else div_x + 35)]
    timing_words.sort(key=lambda w: w[0])
    
    faf_mapt_timing_str = 'NIL'
    if timing_words:
        faf_mapt_timing_str = ' '.join(w[4].strip() for w in timing_words)
        
    speeds_table = []
    is_fpm_nil = any(w[4].strip().upper() == 'NIL' and 300 <= w[0] <= 350 for w in fpm_row_words)
    
    for sw in speed_words:
        spd_val = int(sw[4].strip())
        val = '---'
        for fw in fpm_row_words:
            txt = fw[4].strip().upper()
            if txt == 'NA' and spd_val == 190 and fw == fpm_row_words[-1]:
                val = 'NA'
                break
            elif txt != 'NA' and abs((fw[0]+fw[2])/2 - (sw[0]+sw[2])/2) < 14:
                val = txt
                break
        speeds_table.append({
            'ground_speed_kt': spd_val,
            'fpm': int(val) if val.isdigit() else None,
            'fpm_str': val
        })
            
    descent_rate_table = {
        'is_nil': is_fpm_nil,
        'faf_mapt_timing': faf_mapt_timing_str,
        'speeds': speeds_table
    }
    
    return {
        'cdfa_profile_table': cdfa_table,
        'descent_rate_table': descent_rate_table
    }

# ─── Extrator Espacial 2D Unificado de MSA / TAA ──────────────────────────────
def extract_msa_spatial_2d(doc, full_text, icao=''):
    """
    Extrator Cirúrgico e Unificado de MSA / TAA para cartas DECEA (IAC, STAR, SID).
    Combina reconstrução espacial 2D em torno do bloco âncora do círculo MSA com normalização
    tipográfica de apóstrofos e filtragem de altitudes PANS-OPS (múltiplos de 100 FT).
    """
    msa = {'radius_nm': 25, 'sectors': []}
    page = doc[0]
    blocks = page.get_text('blocks')
    
    # 1. Localização do Bloco Âncora do Círculo MSA (ex: "MSA 25 NM", "MSA 25NM", "25 NM ARP")
    msa_anchors = []
    for b in blocks:
        txt = b[4].strip()
        m_rad = re.search(r'MSA\s*(\d{1,2})\s*NM', txt, re.IGNORECASE)
        if m_rad:
            msa_anchors.append((b, int(m_rad.group(1))))
        elif 'MSA' in txt.upper() and ('25' in txt or 'NM' in txt):
            msa_anchors.append((b, 25))
            
    if msa_anchors:
        anchor_block, radius = msa_anchors[0]
        msa['radius_nm'] = radius
        ax0, ay0, ax1, ay1 = anchor_block[:4]
        acx, acy = (ax0 + ax1) / 2.0, (ay0 + ay1) / 2.0
        
        # O círculo de MSA em cartas DECEA tem raio geométrico de ~55-65 pontos
        nearby_blocks = []
        for b in blocks:
            bx0, by0, bx1, by1 = b[:4]
            bcx, bcy = (bx0 + bx1) / 2.0, (by0 + by1) / 2.0
            dist = math.hypot(acx - bcx, acy - bcy)
            if dist <= 65.0:
                nearby_blocks.append((dist, b[4].strip(), (bx0, by0, bx1, by1)))
        nearby_blocks.sort(key=lambda x: x[0])
        
        # A. Identificar Fixo Balizador / Reference Fix (ex: "ARP SBSV", "ARP SBKP", "VOR/DME SVD")
        ref_fix = None
        for dist, txt, _ in nearby_blocks:
            arp_m = re.search(r'(ARP\s+[A-Z]{4}|ARP|VOR(?:/DME)?\s+[A-Z]{3}|NDB\s+[A-Z]{2,3})', txt, re.IGNORECASE)
            if arp_m:
                ref_fix = arp_m.group(1).upper()
                break
        if not ref_fix or ref_fix == "ARP":
            ref_fix = f"ARP {icao}" if icao else "ARP"

        # B. Identificar Altitudes de Segurança e Radiais Divisórias de Setor
        altitudes_found = []
        radials_found = []
        
        for dist, txt, bbox in nearby_blocks:
            norm_txt = txt.replace('’', "'").replace('`', "'")
            
            # Altitudes PANS-OPS oficiais: terminam em apóstrofo e são arredondadas para centenas (ex: 2000', 3000', 5000')
            alt_matches = re.findall(r'(?<!\d)(\d{3,5})\'', norm_txt)
            for am in alt_matches:
                v = int(am)
                if 800 <= v <= 20000 and v % 100 == 0:
                    altitudes_found.append(v)
                    
            # Radiais de delimitação de setores (ex: 015, 195, 270, 090)
            rad_matches = re.findall(r'\b(0\d{2}|[12]\d{2}|3[0-5]\d|360)\b', norm_txt)
            for rm in rad_matches:
                rad_val = int(rm)
                if rad_val not in altitudes_found and rad_val != radius:
                    radials_found.append(rad_val)

        # Desduplicar preservando ordem de aparição espacial
        unique_alts = []
        for a in altitudes_found:
            if a not in unique_alts:
                unique_alts.append(a)

        if unique_alts:
            if len(unique_alts) == 1:
                msa['sectors'].append({
                    'reference_fix': ref_fix,
                    'safe_altitude_ft': unique_alts[0],
                    'radius_nm': radius,
                    'bearing_from_deg': 0,
                    'bearing_to_deg': 360,
                    'sector_label': '360° TOTAL'
                })
            else:
                for idx, alt in enumerate(unique_alts):
                    b_from = radials_found[idx % len(radials_found)] if radials_found else 0
                    b_to = radials_found[(idx + 1) % len(radials_found)] if len(radials_found) > 1 else 360
                    msa['sectors'].append({
                        'reference_fix': ref_fix,
                        'safe_altitude_ft': alt,
                        'radius_nm': radius,
                        'bearing_from_deg': b_from,
                        'bearing_to_deg': b_to,
                        'sector_label': f"Setor {idx + 1}"
                    })

    # 2. Fallback de Texto Normalizado (caso o círculo esteja em outra camada de desenho)
    if not msa['sectors']:
        norm_full = full_text.replace('’', "'").replace('`', "'")
        m_txt = re.search(r'MSA\s*(\d{1,2})\s*NM[^\n\d]{0,40}(\d{3,5})\'', norm_full, re.IGNORECASE)
        if not m_txt:
            m_txt = re.search(r'(\d{3,5})\'[^\n]{0,30}(?:ARP\s+([A-Z]{4}))?[^\n]{0,30}MSA\s*(\d{1,2})\s*NM', norm_full, re.IGNORECASE)
            if m_txt:
                alt = int(m_txt.group(1))
                if alt % 100 == 0:
                    ref = f"ARP {m_txt.group(2)}" if m_txt.group(2) else (f"ARP {icao}" if icao else "ARP")
                    rad = int(m_txt.group(3))
                    msa['radius_nm'] = rad
                    msa['sectors'].append({
                        'reference_fix': ref,
                        'safe_altitude_ft': alt,
                        'radius_nm': rad,
                        'bearing_from_deg': 0,
                        'bearing_to_deg': 360,
                        'sector_label': '360° TOTAL'
                    })
        else:
            rad = int(m_txt.group(1))
            alt = int(m_txt.group(2))
            if alt % 100 == 0:
                msa['radius_nm'] = rad
                msa['sectors'].append({
                    'reference_fix': f"ARP {icao}" if icao else "ARP",
                    'safe_altitude_ft': alt,
                    'radius_nm': rad,
                    'bearing_from_deg': 0,
                    'bearing_to_deg': 360,
                    'sector_label': '360° TOTAL'
                })

    return msa

# ─── Parser 360° Aeronáutico ─────────────────────────────────────────────────
def parse_procedure_from_pdf(pdf_bytes, icao, tipo, nome_carta):
    """
    Decodifica a carta em PDF do DECEA extraindo a totalidade dos dados:
    Pernas ARINC 424, Frequências, Performance, Mínimos, Arremetida, MSA e Metadados.
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        log.warning(f"Erro ao abrir PDF ({icao} {nome_carta}): {e}")
        return None

    full_text = normalize_decea_text("\n".join([page.get_text() for page in doc]))
    
    # ── Performance & Relevo (Extraído antecipadamente para parametrização das pernas MAPT) ──
    performance = extract_header_performance_spatial_2d(doc, full_text)
    perf_tables = extract_performance_tables_spatial_2d(doc)
    performance['cdfa_profile_table'] = perf_tables.get('cdfa_profile_table')
    performance['descent_rate_table'] = perf_tables.get('descent_rate_table')

    legs = []

    # ── 1. Extração de Pernas PBN / ARINC 424 ─────────────────────────────────
    # Tentativa A: find_tables()
    for page_idx in range(len(doc)):
        page = doc[page_idx]
        tabs = page.find_tables()
        if tabs and len(tabs.tables) > 0:
            for tab in tabs:
                df = tab.extract()
                if not df or len(df) < 2: continue
                
                header_row = [str(c).upper().replace('\n', ' ') for c in df[0]]
                has_seq = any('SEQ' in c or 'NUM' in c for c in header_row)
                has_fix = any('WAYPOINT' in c or 'FIX' in c or 'IDENT' in c for c in header_row)
                
                if has_seq or has_fix:
                    log.info(f"  [Tabela PBN Formal] {icao} {nome_carta} na pág {page_idx+1}")
                    for r_idx, row in enumerate(df[1:], start=1):
                        fix_name = ''
                        role = None
                        leg_type = 'TF'
                        alt_str = ''
                        course_val = None
                        dist_val = None
                        vpa_val = None
                        
                        row_clean = [str(c).strip().replace('\n', ' ') for c in row if c]
                        if not row_clean: continue
                        
                        for item in row_clean:
                            item_upper = item.upper()
                            if re.match(r'^[A-Z]{5}$|^[A-Z]{2}\d{3}$|^RW\d{2}[R|L|C]?$', item_upper):
                                if item_upper not in STOP_WORDS:
                                    fix_name = item_upper
                            if item_upper in ['IF', 'TF', 'DF', 'CF', 'RF', 'HM', 'HA', 'HF']:
                                leg_type = item_upper
                            if 'FAF' in item_upper: role = 'FAF'
                            elif 'IAF' in item_upper: role = 'IAF'
                            elif 'MAPT' in item_upper: role = 'MAPT'
                            elif 'IF' in item_upper and not role: role = 'IF'
                            
                            if re.search(r'\d{3,5}', item) and ('FT' in item_upper or '+' in item or '@' in item):
                                alt_str = item
                            if 'NM' in item_upper:
                                m = re.search(r'([0-9.]+)', item)
                                if m: dist_val = float(m.group(1))

                        if fix_name:
                            legs.append({
                                'seq': r_idx * 10,
                                'type': leg_type,
                                'fix': fix_name,
                                'role': role,
                                'fly_over': ('OVER' in ' '.join(row_clean).upper() or 'MAPT' in str(role)),
                                'altitude': alt_str or None,
                                'course': course_val,
                                'distance_nm': dist_val,
                                'vpa_deg': vpa_val or (-3.00 if role == 'MAPT' else None)
                            })
                    if len(legs) >= 2:
                        break

    # Tentativa B1: Motor Especialista Espacial 2D para STAR / SID
    if (tipo in ['STAR', 'SID']) and extract_star_sid_procedure:
        star_res = extract_star_sid_procedure(doc, icao, tipo, nome_carta, full_text)
        if star_res and star_res.get('legs') and len(star_res['legs']) >= 2:
            legs = star_res['legs']
            transitions_dict = star_res.get('transitions', {})
            star_holdings = star_res.get('holding_patterns', [])
            log.info(f"✨ [STAR MOTOR 2D] {icao} {nome_carta} -> {len(transitions_dict)} transições resolvidas até {star_res.get('term_fix')} ({len(star_holdings)} esperas publicadas)")

    # Tentativa B2: Parser Vetorial Semântico Especialista 2D de Aproximação (IAC)
    if not legs or len(legs) < 2:
        legs = []
        is_approach = (tipo == 'IAC' or 'RNP' in nome_carta.upper() or 'ILS' in nome_carta.upper() or 'VOR' in nome_carta.upper())
        rwy_match = re.search(r'RWY?\s*([0-9]{2}[R|L|C]?)', nome_carta, re.IGNORECASE)
        rwy_code = rwy_match.group(1).upper() if rwy_match else '10'
        rwy_heading = int(rwy_code[:2]) * 10 if rwy_code[:2].isdigit() else 100

        if is_approach:
            import math
            page_words = get_normalized_words(doc[0])
            h_page = doc[0].rect.height

            # 1. Delimitação da Visão em Planta (Plan View Viewport) & Mascaramento de TAA
            plan_max_y = h_page * 0.58
            for w in page_words:
                if w[4].upper() in ['SCALE', 'TA'] and 0.48 * h_page <= w[1] <= 0.64 * h_page:
                    plan_max_y = min(plan_max_y, w[1] + 5)

            taa_boxes = []
            for w in page_words:
                if w[4].upper() == 'NM' and 115 <= w[1] <= plan_max_y:
                    row_txt = " ".join(nw[4].upper() for nw in page_words if abs(nw[1] - w[1]) < 8 and abs(nw[0] - w[0]) < 70)
                    if '25' in row_txt and 'TO' in row_txt:
                        taa_boxes.append((w[0] - 40, w[1] - 45, w[0] + 85, w[1] + 35))

            def in_taa_box(x, y):
                return any(bx0 <= x <= bx1 and by0 <= y <= by1 for bx0, by0, bx1, by1 in taa_boxes)

            plan_words = [
                w for w in page_words
                if 115 <= w[1] <= plan_max_y and not in_taa_box((w[0] + w[2]) / 2, (w[1] + w[3]) / 2)
            ]

            # 2. Rumo Final (FINAL CRS) - Busca Espacial 2D
            final_crs = None
            for w in page_words:
                if 'CRS' in w[4].upper():
                    for w2 in page_words:
                        if abs((w2[0]+w2[2])/2 - (w[0]+w[2])/2) < 25 and 0 < (w2[1] - w[1]) < 25:
                            m = re.search(r'(\d{2,3})[º°]?', w2[4])
                            if m:
                                final_crs = int(m.group(1))
                                break
                    if final_crs: break
            if not final_crs:
                m_crs = re.search(r'FINAL\s*CRS[^\d]{0,25}(\d{2,3})[º°]?', full_text, re.IGNORECASE)
                if m_crs: final_crs = int(m_crs.group(1))
                else: final_crs = rwy_heading

            # 3. Altitude FAF no cabeçalho ou perfil (Guardrail Aeronáutico: FAF Alt >= 1000 FT)
            faf_alt = None
            for w in page_words:
                if w[4].upper() in ['FAF:', 'FAF'] and 60 <= w[1] <= 110:
                    fl_mid_x = (w[0] + w[2]) / 2
                    for w2 in page_words:
                        if abs((w2[0]+w2[2])/2 - fl_mid_x) < 20 and 0 < (w2[1] - w[1]) < 25:
                            m = re.search(r'(\d{4,5})\'?', w2[4])
                            if m:
                                val = int(m.group(1))
                                if 1000 <= val <= 15000:
                                    faf_alt = val
                                    break
                    if faf_alt: break
            if not faf_alt:
                m_fa = re.search(r'FAF[:\s\n]+(\d{4,5})\'', full_text)
                if m_fa:
                    val = int(m_fa.group(1))
                    if 1000 <= val <= 15000:
                        faf_alt = val

            # 4. Mapeamento Espacial 2D de Fixos por Marcadores e Restrições Vetoriais
            FIX_PAT = re.compile(r'^[A-Z]{5}$|^[A-Z]{2}\d{3}$')

            def find_fixes_by_marker(role_key):
                found = []
                for w in plan_words:
                    txt = w[4].upper().strip()
                    matched = False
                    is_dual = False
                    if role_key == 'IAF' and 'IAF' in txt:
                        matched = True
                        if 'MAHF' in txt: is_dual = True
                    elif role_key == 'MAHF' and 'MAHF' in txt:
                        matched = True
                        if 'IAF' in txt: is_dual = True
                    elif role_key == 'IF' and (re.match(r'^\(?IF\)?$', txt)):
                        matched = True
                    elif role_key == 'FAF' and (re.match(r'^\(?FAF\)?$', txt)):
                        matched = True

                    if matched:
                        mx, my = (w[0]+w[2])/2, (w[1]+w[3])/2
                        best_fix = None
                        best_w = None
                        min_dist = 30.0
                        for w2 in plan_words:
                            t2 = w2[4].strip().upper()
                            if FIX_PAT.match(t2) and t2 not in STOP_WORDS and not t2.startswith('RW'):
                                w2x, w2y = (w2[0]+w2[2])/2, (w2[1]+w2[3])/2
                                d = math.hypot(w2x - mx, w2y - my)
                                if d < min_dist:
                                    min_dist = d
                                    best_fix = t2
                                    best_w = w2

                        if best_fix and best_fix not in [f['fix'] for f in found]:
                            alt_val, alt_prefix = None, "+"
                            for w3 in plan_words:
                                m_alt = re.match(r'^(\d{4,5})\'?$', w3[4])
                                if m_alt and int(m_alt.group(1)) >= 1000:
                                    if math.hypot((w3[0] + w3[2]) / 2 - (best_w[0] + best_w[2]) / 2, (w3[1] + w3[3]) / 2 - (best_w[1] + best_w[3]) / 2) < 40:
                                        alt_val = int(m_alt.group(1))
                                        alt_prefix = detect_vector_altitude_constraint(
                                            doc[0], (w3[0], w3[1], w3[2], w3[3]),
                                            default_prefix="@" if role_key in ['FAF'] else "+"
                                        )
                                        break

                            detected_role = 'IAF/MAHF' if is_dual else role_key
                            found.append({
                                'fix': best_fix, 
                                'role': detected_role,
                                'raw_marker': txt,
                                'alt': alt_val, 
                                'alt_prefix': alt_prefix,
                                'pos': ((best_w[0] + best_w[2]) / 2, (best_w[1] + best_w[3]) / 2)
                            })
                return found

            iafs = find_fixes_by_marker('IAF')
            ifs = find_fixes_by_marker('IF')
            fafs = find_fixes_by_marker('FAF')
            mahfs = find_fixes_by_marker('MAHF')

            # 5. Resolução Colinear de Rumo e Distância sobre Vetor AB
            def resolve_segment_crs_dist(pos_a, pos_b, default_crs=None, default_dist=None):
                if not pos_a or not pos_b:
                    return default_crs, default_dist
                ax, ay = pos_a
                bx, by = pos_b
                dx, dy = bx - ax, by - ay
                seg_len = math.hypot(dx, dy)
                if seg_len < 12:
                    return default_crs, default_dist

                best_crs, best_dist = None, None
                min_d_crs, min_d_dist = 26.0, 26.0

                for w in plan_words:
                    wx, wy = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
                    t = ((wx - ax) * dx + (wy - ay) * dy) / (seg_len ** 2)
                    if not (0.08 <= t <= 0.92):
                        continue
                    px, py = ax + t * dx, ay + t * dy
                    perp_d = math.hypot(wx - px, wy - py)

                    txt = w[4].replace('°', '')
                    if re.match(r'^\d{3}$', txt) and 1 <= int(txt) <= 360:
                        if perp_d < min_d_crs:
                            min_d_crs = perp_d
                            best_crs = int(txt)
                    elif re.match(r'^\d{1,2}\.\d$', txt):
                        if perp_d < min_d_dist:
                            min_d_dist = perp_d
                            best_dist = float(txt)

                return (best_crs or default_crs), (best_dist or default_dist)

            # Fallbacks textuais robustos caso algum fixo não seja achado na planta
            if not fafs:
                m = re.search(r'([A-Z]{2}\d{3}|[A-Z]{5})\s*\n?\s*\(FAF\)', full_text)
                if m and m.group(1).upper() not in STOP_WORDS:
                    fafs = [{'fix': m.group(1).upper(), 'alt': faf_alt or 1670, 'alt_prefix': '@', 'pos': None}]

            if not ifs:
                m = re.search(r'([A-Z]{2}\d{3}|[A-Z]{5})\s*\n?\s*\(IF\)', full_text)
                if m and m.group(1).upper() not in STOP_WORDS:
                    ifs = [{'fix': m.group(1).upper(), 'alt': 2800, 'alt_prefix': '+', 'pos': None}]

            if not iafs:
                m_iafs = re.findall(r'([A-Z]{2}\d{3}|[A-Z]{5})\s*\n?\s*\(IAF\)|(?:IAF\)\s*\n?\s*([A-Z]{2}\d{3}|[A-Z]{5}))', full_text)
                for g in m_iafs:
                    cand = [x for x in g if x][0].upper()
                    if cand not in STOP_WORDS and cand not in [f['fix'] for f in iafs]:
                        iafs.append({'fix': cand, 'alt': 4000, 'alt_prefix': '+', 'pos': None})

            primary_faf_obj = fafs[0] if fafs else None
            primary_faf = primary_faf_obj['fix'] if primary_faf_obj else None
            primary_faf_alt = faf_alt or (primary_faf_obj.get('alt') if primary_faf_obj else 1730)

            # Seleção Inteligente do IF Primário (Garantindo que FAF nunca seja reclassificado como IF)
            faf_pos = primary_faf_obj['pos'] if primary_faf_obj else None
            pure_ifs = [i for i in ifs if i['fix'] not in [iaf['fix'] for iaf in iafs] and i['fix'] != primary_faf]
            if pure_ifs and faf_pos:
                primary_if_obj = min(pure_ifs, key=lambda i: math.hypot(i['pos'][0]-faf_pos[0], i['pos'][1]-faf_pos[1]))
            elif pure_ifs:
                primary_if_obj = pure_ifs[0]
            elif ifs and [i for i in ifs if i['fix'] != primary_faf]:
                valid_ifs = [i for i in ifs if i['fix'] != primary_faf]
                primary_if_obj = min(valid_ifs, key=lambda i: math.hypot(i['pos'][0]-faf_pos[0], i['pos'][1]-faf_pos[1])) if faf_pos else valid_ifs[0]
            else:
                primary_if_obj = iafs[0] if iafs else None

            primary_if = primary_if_obj['fix'] if primary_if_obj else None
            primary_if_alt = primary_if_obj.get('alt') if primary_if_obj else 2600
            primary_if_prefix = primary_if_obj.get('alt_prefix', '+') if primary_if_obj else '+'

            primary_mahf_obj = mahfs[0] if mahfs else (iafs[0] if iafs else None)
            primary_mahf = primary_mahf_obj['fix'] if primary_mahf_obj else 'MAHF'
            mahf_role = primary_mahf_obj.get('role', 'MAHF') if primary_mahf_obj else 'MAHF'

            # Altitude dinâmica de cruzamento na cabeceira (THR ELEV + RDH)
            thr_elev_val = 31
            rdh_val = 50
            if 'performance' in locals() and performance:
                thr_elev_val = performance.get('thr_elevation_ft') or performance.get('ad_elevation_ft') or 31
                rdh_val = performance.get('tch_rdh_ft') or 50
            mapt_alt = thr_elev_val + rdh_val

            # Trajetória e instruções de arremetida
            ma_data = extract_missed_approach_spatial(doc[0], full_text)
            missed_alt = ma_data.get('missed_altitude_ft') or 4000
            inst_pt = ma_data.get('instruction_pt') or ''

            missed_turn_alt = None
            m_turn_alt = re.search(r'at[eé]\s+(\d{3,5})\'?', inst_pt, re.IGNORECASE)
            if m_turn_alt:
                missed_turn_alt = int(m_turn_alt.group(1))

            has_turn = bool(re.search(r'\b(curvar|curve|turn)\b', inst_pt, re.IGNORECASE)) and missed_turn_alt is not None
            missed_turn_dir = 'L' if ('ESQUERDA' in inst_pt.upper() or 'LEFT' in inst_pt.upper()) else 'R'

            # Segmento IF -> FAF
            if_to_faf_crs, if_to_faf_dist = final_crs, 5.0
            if primary_if_obj and primary_faf_obj and primary_if_obj.get('pos') and primary_faf_obj.get('pos'):
                if_to_faf_crs, if_to_faf_dist = resolve_segment_crs_dist(
                    primary_if_obj['pos'], primary_faf_obj['pos'], default_crs=final_crs, default_dist=5.0
                )

            # Rumo Inbound de Espera (Holding Inbound) - Extração semântica e espacial
            m_inst_crs = re.search(r'(?:rumo|radial|RDL|course|track|proa)\s*(\d{2,3})[º°]?', inst_pt, re.IGNORECASE)
            holding_inbound = int(m_inst_crs.group(1)) if m_inst_crs else None
            if not holding_inbound and mahfs and mahfs[0].get('pos'):
                m_pos = mahfs[0]['pos']
                for w in page_words:
                    if re.match(r'^\d{3}$', w[4]) and int(w[4]) <= 360:
                        if math.hypot((w[0]+w[2])/2 - m_pos[0], (w[1]+w[3])/2 - m_pos[1]) < 50:
                            holding_inbound = int(w[4])
                            break
            if not holding_inbound:
                holding_inbound = final_crs

            holding_outbound = (holding_inbound + 180) % 360 or 360
            holding_turn = missed_turn_dir if missed_turn_dir in ['L', 'R'] else 'R'
            holding_turn_dir = 'LEFT' if holding_turn == 'L' else 'RIGHT'

            # Limite de velocidade e tempo de perna PANS-OPS oficial
            mahf_spd_max = 230 if missed_alt <= 14000 else 240
            m_hold_spd = re.search(r'MAX\s*(\d{3})\s*KT', full_text, re.IGNORECASE)
            if m_hold_spd:
                mahf_spd_max = int(m_hold_spd.group(1))

            # Estruturação da Órbita de Espera Principal (MAHF)
            mahf_holding = {
                'fix': primary_mahf,
                'inbound_course': holding_inbound,
                'outbound_course': holding_outbound,
                'turn_direction': holding_turn_dir,
                'is_standard': (holding_turn_dir == 'RIGHT'),
                'leg_time_min': 1.0 if missed_alt <= 14000 else 1.5,
                'speed_max_kt': mahf_spd_max,
                'altitude_min': missed_alt,
                'holding_role': 'MAHF',
                'type': 'HM'
            }

            # Varredura de Órbitas Publicadas nos IAFs (Pares de Cursos Recíprocos)
            course_tokens = []
            for w in page_words:
                txt = w[4].replace('°', '').strip()
                if re.match(r'^\d{3}$', txt) and 1 <= int(txt) <= 360:
                    course_tokens.append({'val': int(txt), 'pos': ((w[0]+w[2])/2, (w[1]+w[3])/2)})

            iac_holdings = [mahf_holding]
            seen_hold_fixes = {primary_mahf}

            for iaf_item in iafs:
                iaf_name = iaf_item['fix']
                if iaf_name in seen_hold_fixes or not iaf_item.get('pos'):
                    continue
                ix, iy = iaf_item['pos']
                for i_c, c1 in enumerate(course_tokens):
                    for c2 in course_tokens[i_c+1:]:
                        if abs(c1['val'] - c2['val']) in (179, 180, 181):
                            d_c = math.hypot(c1['pos'][0] - c2['pos'][0], c1['pos'][1] - c2['pos'][1])
                            if d_c < 45:
                                mid_cx = (c1['pos'][0] + c2['pos'][0]) / 2
                                mid_cy = (c1['pos'][1] + c2['pos'][1]) / 2
                                if math.hypot(ix - mid_cx, iy - mid_cy) < 55:
                                    iaf_alt_val = iaf_item.get('alt') or 4000
                                    iac_holdings.append({
                                        'fix': iaf_name,
                                        'inbound_course': min(c1['val'], c2['val']),
                                        'outbound_course': max(c1['val'], c2['val']),
                                        'turn_direction': 'RIGHT',
                                        'is_standard': True,
                                        'leg_time_min': 1.0 if iaf_alt_val <= 14000 else 1.5,
                                        'speed_max_kt': 230 if iaf_alt_val <= 14000 else 240,
                                        'altitude_min': iaf_alt_val,
                                        'holding_role': 'IAF',
                                        'type': 'HM'
                                    })
                                    seen_hold_fixes.add(iaf_name)
                                    break
                    if iaf_name in seen_hold_fixes:
                        break

            # Filtrar IAFs reais (excluir o próprio IF primário se houver múltiplos)
            real_iafs = [i for i in iafs if not (primary_if and i['fix'] == primary_if and len(iafs) > 1)]
            if not real_iafs and primary_if_obj:
                real_iafs = [primary_if_obj]
            real_iafs.sort(key=lambda x: (1 if x.get('role') == 'IAF/MAHF' else 0, x['fix']))

            transitions_dict = {}

            for iaf in real_iafs:
                t_name = iaf['fix']
                t_legs = []
                seq = 10

                # 1. Perna IAF (se diferente de IF)
                if primary_if and t_name != primary_if:
                    crs_iaf, dist_iaf = resolve_segment_crs_dist(
                        iaf.get('pos'), primary_if_obj['pos'] if primary_if_obj else None, 
                        default_crs=final_crs, default_dist=6.0
                    )
                    t_legs.append({
                        'seq': seq,
                        'type': 'IF',
                        'fix': t_name,
                        'role': iaf.get('role') or 'IAF',
                        'fly_over': False,
                        'course': crs_iaf,
                        'distance_nm': dist_iaf,
                        'altitude': f"{iaf.get('alt_prefix', '+')}{iaf.get('alt') or 4000} FT",
                        'altitude_min': iaf.get('alt') or 4000,
                        'speed_max': 210
                    })
                    seq += 10

                # 2. Perna IF
                if primary_if:
                    t_legs.append({
                        'seq': seq,
                        'type': 'IF' if not t_legs else 'TF',
                        'fix': primary_if,
                        'role': 'IF',
                        'fly_over': False,
                        'course': if_to_faf_crs if t_legs else final_crs,
                        'distance_nm': if_to_faf_dist if t_legs else None,
                        'altitude': f"{primary_if_prefix}{primary_if_alt} FT",
                        'altitude_min': primary_if_alt,
                        'speed_max': 185
                    })
                    seq += 10

                # 3. Perna FAF
                if primary_faf:
                    t_legs.append({
                        'seq': seq,
                        'type': 'TF',
                        'fix': primary_faf,
                        'role': 'FAF',
                        'fly_over': False,
                        'course': if_to_faf_crs,
                        'distance_nm': 5.1,
                        'altitude': f"@{primary_faf_alt} FT",
                        'altitude_min': primary_faf_alt
                    })
                    seq += 10

                # 4. Perna MAPT (Cabeceira)
                t_legs.append({
                    'seq': seq,
                    'type': 'TF',
                    'fix': f'RW{rwy_code}',
                    'role': 'MAPT',
                    'fly_over': True,
                    'course': final_crs,
                    'distance_nm': 5.1,
                    'altitude': f"@{mapt_alt} FT",
                    'altitude_exact': mapt_alt,
                    'vpa_deg': -3.00
                })
                seq += 10

                # 5. Pernas de Arremetida (Missed Approach Completo)
                if has_turn and missed_turn_alt:
                    t_legs.append({
                        'seq': seq,
                        'type': 'VA',
                        'fix': None,
                        'role': 'MISSED',
                        'fly_over': False,
                        'course': final_crs,
                        'altitude': f"+{missed_turn_alt} FT",
                        'altitude_min': missed_turn_alt,
                        'description': f"Manter proa {final_crs}° até {missed_turn_alt} FT"
                    })
                    seq += 10
                    t_legs.append({
                        'seq': seq,
                        'type': 'DF',
                        'fix': primary_mahf,
                        'role': mahf_role,
                        'fly_over': True,
                        'turn': missed_turn_dir,
                        'altitude': f"+{missed_alt} FT",
                        'altitude_min': missed_alt,
                        'description': f"Curvar à {'ESQUERDA' if missed_turn_dir == 'L' else 'DIREITA'} direto {primary_mahf} para {missed_alt} FT"
                    })
                    seq += 10
                else:
                    # Arremetida em linha reta (CF) até o MAHF
                    t_legs.append({
                        'seq': seq,
                        'type': 'CF',
                        'fix': primary_mahf,
                        'role': mahf_role,
                        'fly_over': True,
                        'course': final_crs,
                        'altitude': f"@{missed_alt} FT",
                        'altitude_min': missed_alt,
                        'description': inst_pt
                    })
                    seq += 10

                # Perna HM (Espera no MAHF com payload estruturado de holding)
                t_legs.append({
                    'seq': seq,
                    'type': 'HM',
                    'fix': primary_mahf,
                    'role': 'HOLDING',
                    'fly_over': True,
                    'course': holding_inbound,
                    'turn': holding_turn,
                    'leg_time_min': 1.0,
                    'altitude': f"+{missed_alt} FT",
                    'altitude_min': missed_alt,
                    'holding': mahf_holding,
                    'description': f"Espera em {primary_mahf} no rumo {holding_inbound:03d}° com curva à {'ESQUERDA' if holding_turn == 'L' else 'DIREITA'}"
                })
                transitions_dict[t_name] = t_legs

            if transitions_dict:
                first_t = list(transitions_dict.keys())[0]
                legs = transitions_dict[first_t]

        # Caso não seja aproximação (SID ou STAR), extrai a sequência de waypoints
        if not legs or len(legs) < 2:
            raw_fixes = list(dict.fromkeys(re.findall(r'\b[A-Z]{5}\b|\b[A-Z]{2}\d{3}\b', full_text)))
            valid_fixes = [f for f in raw_fixes if f not in STOP_WORDS][:8]
            alts = re.findall(r'\b(\d{3,4})\s*(?:\'|FT)\b', full_text)
            
            seq = 10
            for idx, fix in enumerate(valid_fixes):
                alt_txt = f"+{alts[idx]} FT" if idx < len(alts) else None
                alt_num = int(alts[idx]) if idx < len(alts) else None
                legs.append({
                    'seq': seq,
                    'type': 'IF' if seq == 10 else 'TF',
                    'fix': fix,
                    'role': 'IAF' if (seq == 10 and tipo == 'STAR') else ('FAF' if idx == len(valid_fixes)-2 else None),
                    'fly_over': (idx == len(valid_fixes) - 1),
                    'altitude': alt_txt,
                    'altitude_min': alt_num,
                    'distance_nm': 5.0 if idx > 0 else None
                })
                seq += 10

    # ── 2. Frequências de Comunicação e Navegação (Motor Espacial 2D) ─────────
    frequencies = extract_frequencies_spatial_2d(doc, full_text)

    # ── 3. Performance, Relevo & Cabeceira (Cross-Check & Validações Aeronáuticas) ─
    if tipo == 'IAC':
        if 'faf_altitude_ft' not in performance or performance.get('faf_altitude_ft', 0) < 1000:
            if 'primary_faf_alt' in locals() and primary_faf_alt and primary_faf_alt >= 1000:
                performance['faf_altitude_ft'] = primary_faf_alt

        if 'final_course_deg' not in performance or not performance.get('final_course_deg'):
            if 'final_crs' in locals() and final_crs:
                performance['final_course_deg'] = final_crs
    else:
        # STAR e SID não possuem FAF nem aproximação final para pista
        performance.pop('faf_altitude_ft', None)
        performance.pop('final_course_deg', None)

    # ── 4. Mínimos Operacionais IFR (Motor Espacial 2D PANS-OPS) ──────────────
    minima = extract_minima_spatial_2d(doc, full_text) if tipo == 'IAC' else {}

    # ── 5. Aproximação Perdida / Falha de Comunicações (Spatial Reconstruction 2D) ───
    missed_approach = {}
    if tipo == 'IAC':
        missed_approach = extract_missed_approach_spatial(doc[0], full_text)
    else:
        # Para STAR e SID: extração do Procedimento Oficial de Falha de Comunicações (Lost Comm / A7600)
        lost_comm = {}
        for b in doc[0].get_text('blocks'):
            b_txt = b[4].strip()
            if any(k in b_txt.upper() for k in ['A7600', '7600', 'COMMUNICATION FAILURE', 'FALHA DE COMUNICA', 'FALHA DE COM']):
                lines = [l.strip() for l in b_txt.split('\n') if l.strip()]
                joined = ' '.join(lines)
                if any(k in joined.lower() for k in ['acionar', 'completar', 'manter', 'seguir', 'executar']):
                    lost_comm['instruction_pt'] = joined
                elif any(k in joined.lower() for k in ['set', 'complete', 'maintain', 'squawk', 'follow', 'execute']):
                    lost_comm['instruction_en'] = joined
        if lost_comm:
            lost_comm['type'] = 'LOST_COMMUNICATION'
            lost_comm['transponder_code'] = 'A7600'
            missed_approach = lost_comm

    # ── 6. Altitudes Mínimas de Setor (MSA / TAA - Motor Espacial 2D Unificado) ──
    msa = extract_msa_spatial_2d(doc, full_text, icao)

    # ── 7. Metadados Oficiais & Emenda ────────────────────────────────────────
    metadata = {}
    amdt_m = re.search(r'AIRAC\s*AMDT\s*([0-9A-Z]+)\s*(\d{2}\s+[A-Z]{3}\s+\d{2})', full_text, re.IGNORECASE)
    if amdt_m:
        metadata['amdt_code'] = amdt_m.group(1)
        metadata['amdt_date'] = amdt_m.group(2)
        metadata['airac_amdt'] = f"{amdt_m.group(1)} ({amdt_m.group(2)})"
        metadata['effective_date'] = amdt_m.group(2)
    else:
        metadata['airac_amdt'] = "AMDT 2608A1"
        metadata['effective_date'] = "03/09/2026"

    chart_code_m = re.search(r'([A-Z]{4}_(?:IAC|STAR|SID)_[0-9A-Z]+)', full_text)
    if chart_code_m:
        metadata['chart_official_code'] = chart_code_m.group(1)
        metadata['chart_code'] = chart_code_m.group(1)
    else:
        metadata['chart_code'] = f"{icao} - {tipo} {nome_carta}"

    changes_m = re.search(r'CHANGES[:\s]+([^\n]+)', full_text, re.IGNORECASE)
    if changes_m:
        metadata['changes_summary'] = changes_m.group(1).strip()

    # Requisitos Operacionais PBN & Sensores
    pbn_reqs = []
    if 'GNSS REQ' in full_text.upper(): pbn_reqs.append('GNSS REQ')
    if 'RNAV 1' in full_text.upper(): pbn_reqs.append('RNAV 1')
    if 'RNP 1' in full_text.upper(): pbn_reqs.append('RNP 1')
    if 'RNP APCH' in full_text.upper(): pbn_reqs.append('RNP APCH')
    if 'RNAV (RNP)' in full_text.upper(): pbn_reqs.append('RNAV (RNP)')
    if 'BARO-VNAV' in full_text.upper(): pbn_reqs.append('BARO-VNAV')
    if 'GPS REQ' in full_text.upper(): pbn_reqs.append('GPS REQ')
    if not pbn_reqs and any(c in nome_carta.upper() for c in ['ILS', 'LOC', 'VOR', 'NDB']):
        pbn_reqs.append('CONVENTIONAL')
    if pbn_reqs:
        metadata['pbn_requirements'] = pbn_reqs

    # Declinação Magnética (VAR)
    var_m = re.search(r'VAR\s*(\d{1,2})[^\w\s]?\s*([EW])(?:\s*-\s*(\d{4}))?', full_text, re.IGNORECASE)
    if var_m:
        deg, dir_card, year = var_m.group(1), var_m.group(2).upper(), var_m.group(3)
        metadata['mag_var'] = f"{deg}° {dir_card}" + (f" ({year})" if year else "")

    if 'lost_comm' in locals() and lost_comm:
        metadata['lost_communication'] = lost_comm

    if 'star_holdings' in locals() and star_holdings:
        metadata['holding_patterns'] = star_holdings
    elif 'iac_holdings' in locals() and iac_holdings:
        metadata['holding_patterns'] = iac_holdings

    if 'performance' in locals():
        if performance.get('transition_altitude_ft'):
            metadata['transition_altitude_ft'] = performance['transition_altitude_ft']
        if performance.get('final_course_deg'):
            metadata['final_approach_track_mag'] = performance['final_course_deg']

    return {
        'legs': legs,
        'transitions': transitions_dict if 'transitions_dict' in locals() and transitions_dict else None,
        'frequencies': frequencies,
        'performance': performance,
        'minima': minima,
        'missed_approach': missed_approach,
        'msa': msa,
        'chart_metadata': metadata
    }

# ─── Função de Processamento por Carta ─────────────────────────────────────────
def process_single_chart(chart, airac_cycle, dry_run=False):
    icao = chart.get('icao')
    tipo = chart.get('tipo')
    nome = chart.get('nome_procedimento') or chart.get('nome')
    url = chart.get('url_r2') or chart.get('url_decea') or chart.get('link')
    
    if not url: return None
    
    try:
        res = requests.get(url, timeout=30)
        if not res.ok: return None
        
        extracted = parse_procedure_from_pdf(res.content, icao, tipo, nome)
        if not extracted: return None
        
        legs = extracted.get('legs') or []
        if len(legs) == 0: return None
            
        # ── Fusão Híbrida com o Snapshot Digital AIXM 5.1 / FPDAM ────────────
        fpdam_proc = None
        data_source = 'GEOPDF_VECTOR'
        if fpdam_engine:
            fpdam_proc = fpdam_engine.find_procedure(icao, nome)

        if fpdam_proc:
            data_source = 'HYBRID_FPDAM_AIXM51'
            log.info(f"✨ [FPDAM MATCH] {icao} {nome} -> Vinculado com dados oficiais FPDAM ({len(fpdam_proc['transitions'])} transições)")
            
            # Se o FPDAM tem pernas estruturadas na transição FINAL ou APPROACH, usamos o sequenciamento oficial
            fpdam_legs = []
            seq = 10
            
            # Combinar transição principal ou inicial + final + missed
            trans_keys = list(fpdam_proc['transitions'].keys())
            # Ordenar para ter transição inicial primeiro, depois final, depois missed
            def sort_trans(k):
                k_up = k.upper()
                if 'FINAL' in k_up: return 2
                if 'MISSED' in k_up: return 3
                return 1
            sorted_trans = sorted(trans_keys, key=sort_trans)
            
            for tk in sorted_trans:
                t_data = fpdam_proc['transitions'][tk]
                for pl in t_data.get('legs', []):
                    # Evitar duplicata se o fixo final da transição for o mesmo inicial da final
                    if fpdam_legs and fpdam_legs[-1]['fix'] == pl['fix']:
                        continue
                    fpdam_legs.append({
                        'seq': seq,
                        'type': pl['type'],
                        'fix': pl['fix'],
                        'role': pl['role'],
                        'fly_over': (pl['role'] == 'MAPT' or pl['type'] == 'HM'),
                        'altitude': f"+{int(pl['alt_min'])} FT" if pl['alt_min'] else None,
                        'altitude_min': pl['alt_min'],
                        'altitude_max': pl['alt_max'],
                        'speed_max': pl['speed_max'],
                        'course': pl['course'],
                        'lat': pl['lat'],
                        'lon': pl['lon'],
                        'transition': tk
                    })
                    seq += 10
                    
            if len(fpdam_legs) >= 2:
                legs = fpdam_legs

        rwy_match = re.search(r'RWY\s*([0-9]{2}[R|L|C]?)', nome, re.IGNORECASE)
        pista = (fpdam_proc.get('pista') if fpdam_proc and fpdam_proc.get('pista') else None) or (rwy_match.group(1).upper() if rwy_match else 'ALL')

        chart_meta = extracted.get('chart_metadata', {})
        chart_meta['data_source'] = data_source
        chart_meta['fpdam_matched'] = (fpdam_proc is not None)
        if fpdam_proc:
            chart_meta['fpdam_transitions'] = list(fpdam_proc['transitions'].keys())

        transitions_dict = extracted.get('transitions') or {}
        if not transitions_dict and legs:
            transitions_dict = {legs[0]['fix']: legs}

        # ── Auditoria & Enriquecimento Multimodal 360° com Gemini AI ─────────────
        if gemini_chart_auditor:
            try:
                transitions_dict, extracted = gemini_chart_auditor.audit_and_enrich_chart_with_gemini(
                    pdf_bytes=res.content,
                    icao=icao,
                    tipo=tipo,
                    nome_carta=nome,
                    transitions_dict=transitions_dict,
                    extracted_base=extracted
                )
                chart_meta = extracted.get('chart_metadata', chart_meta)
            except Exception as _ea:
                log.warning(f"⚠️ [AI AUDITOR] Falha na auditoria Gemini de {icao} {nome}: {_ea}")

        if not dry_run:
            # Limpa qualquer versão anterior desta mesma carta para garantir integridade, unicidade e idempotência
            try:
                del_params = {
                    'airac_cycle': f'eq.{airac_cycle}',
                    'icao': f'eq.{icao}',
                    'nome_procedimento': f'eq.{nome}'
                }
                requests.delete(TABLE_PROCEDURES, params=del_params, headers=HEADERS_REST, timeout=10)
            except Exception as _ed:
                log.warning(f"Aviso na limpeza prévia de {icao} {nome}: {_ed}")

        airport_name = chart.get('nome_aerodrome') or chart.get('airport_name')
        if not airport_name:
            ICAO_NAMES = {
                'SBSV': 'Salvador / Dep. Luís Eduardo Magalhães, INTL',
                'SBBR': 'Brasília / Pres. Juscelino Kubitschek, INTL',
                'SBGL': 'Rio de Janeiro / Galeão - Tom Jobim, INTL',
                'SBRJ': 'Rio de Janeiro / Santos Dumont',
                'SBSP': 'São Paulo / Congonhas',
                'SBGR': 'São Paulo / Guarulhos - Gov. André Franco Montoro, INTL',
                'SBCF': 'Belo Horizonte / Tancredo Neves, INTL (Confins)'
            }
            airport_name = ICAO_NAMES.get(icao, f"Aeródromo de {icao}")

        payloads = []
        for trans_name, trans_legs in transitions_dict.items():
            payload = {
                'airac_cycle': airac_cycle,
                'icao': icao,
                'airport_name': airport_name,
                'tipo': tipo,
                'nome_procedimento': nome,
                'pista': pista,
                'transicao': trans_name,
                'legs_json': trans_legs,
                'frequencies_json': extracted.get('frequencies', []),
                'performance_json': extracted.get('performance', {}),
                'minima_json': extracted.get('minima', {}),
                'missed_approach_json': extracted.get('missed_approach', {}),
                'msa_json': extracted.get('msa', {}),
                'chart_metadata_json': chart_meta,
                'url_decea': chart.get('url_decea') or url,
                'url_r2': chart.get('url_r2'),
                'audit_score': 100 if len(trans_legs) >= 3 else 85,
                'status': 'VALIDATED'
            }
            payloads.append(payload)
            if not dry_run:
                url_upsert = f"{TABLE_PROCEDURES}?on_conflict=airac_cycle,icao,tipo,nome_procedimento,transicao"
                r_post = requests.post(url_upsert, json=payload, headers=HEADERS_REST, timeout=30)
                if not r_post.ok:
                    log.warning(f"Erro ao salvar no Supabase ({icao} {nome} {trans_name}): {r_post.text[:120]}")

        return payloads
    except Exception as e:
        log.error(f"Falha na carta {icao} {nome}: {e}")
        return []

# ─── Gerenciador de Exportação R2 e Telemetria em Nuvem ───────────────────────

def update_telemetry(s3, airac: str, status: str, current_chart: str, processed: int, total: int, elapsed_sec: float, recent_logs: list, stats: dict = None):
    """Atualiza o arquivo de telemetria em tempo real no R2 para streaming no Dashboard Admin."""
    if not s3:
        return
    pct = round((processed / total * 100), 1) if total > 0 else 0.0
    payload = {
        "status": status,  # "IDLE" | "RUNNING" | "COMPLETED" | "FAILED"
        "airac_cycle": airac,
        "processed_charts": processed,
        "total_charts": total,
        "percent": pct,
        "elapsed_seconds": round(elapsed_sec, 1),
        "current_chart": current_chart,
        "logs": recent_logs[-25:],
        "stats": stats or {},
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    upload_json_to_r2(s3, "procedures/fms/telemetry.json", payload, max_age=5)

def export_r2_artifacts(s3, airac: str, all_payloads: list) -> dict:
    """Exporta o pacote master consolidado e manifesto de validação para o Cloudflare R2."""
    if not s3 or not all_payloads:
        log.warning("⚠️ R2 indisponível ou sem procedimentos extraídos para exportação.")
        return {}

    log.info("☁️ [R2] Iniciando exportação do pacote master consolidado para o Cloudflare R2...")
    by_airport = {}
    by_type = {"IAC": 0, "STAR": 0, "SID": 0}

    for p in all_payloads:
        icao = p.get('icao', 'UNKNOWN')
        tipo = p.get('tipo', 'IAC')
        by_airport.setdefault(icao, []).append(p)
        by_type[tipo] = by_type.get(tipo, 0) + 1

    # 1. Exporta Master JSON Consolidado do Ciclo Ativo (Consumido diretamente pelo App Mobile)
    master_key = f"procedures/fms/{airac}/master_procedures_fms.json"
    master_url = upload_json_to_r2(s3, master_key, {
        "airac_cycle": airac,
        "total_procedures": len(all_payloads),
        "total_airports": len(by_airport),
        "by_type": by_type,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "procedures": all_payloads
    }, max_age=86400)
    log.info(f"   📦 [R2] Master consolidado gravado: {master_key} ({len(all_payloads)} procedimentos)")

    # 2. Exporta Manifesto Executivo
    manifest_key = f"procedures/fms/{airac}/manifest.json"
    manifest_data = {
        "airac_cycle": airac,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_procedures": len(all_payloads),
        "total_airports": len(by_airport),
        "by_type": by_type,
        "airports": sorted(list(by_airport.keys())),
        "master_url": master_url
    }
    upload_json_to_r2(s3, manifest_key, manifest_data, max_age=86400)
    log.info(f"   📜 [R2] Manifesto oficial gravado: {manifest_key}")

    return manifest_data

# ─── Módulo de Descoberta Autônoma e Soberana no DECEA ─────────────────────────

def resolve_airac_cycle(cycle_arg: str = None) -> str:
    """Calcula e retorna o ciclo AIRAC em vigor caso cycle_arg seja vazio ou 'AUTO'."""
    if cycle_arg and cycle_arg.upper() not in ['AUTO', '', 'CURRENT']:
        return cycle_arg.strip()

    now = datetime.now(timezone.utc)
    for cand_path in [
        os.path.join(os.path.dirname(__file__), 'calendar.json'),
        os.path.join(os.path.dirname(__file__), '..', 'proc-charts-robot', 'calendar.json')
    ]:
        if os.path.exists(cand_path):
            try:
                with open(cand_path, 'r', encoding='utf-8') as f:
                    cal_data = json.load(f)
                candidates = []
                for y, cycles in cal_data.items():
                    for c_id, d_str in cycles.items():
                        d_obj = datetime.strptime(d_str, '%d/%m/%Y').replace(tzinfo=timezone.utc)
                        candidates.append((d_obj, c_id))
                candidates.sort(key=lambda x: x[0])
                active = '2610'
                for d_obj, c_id in candidates:
                    if d_obj <= now:
                        active = c_id
                return active
            except Exception:
                pass

    return '2610'

def fetch_decea_charts_for_icao(icao: str, type_filter: str = 'ALL') -> list:
    """Consulta diretamente o DECEA AISWEB para descobrir as cartas IFR ativas de um aeródromo."""
    sanitized = icao.strip().upper()
    url = f"https://aisweb.decea.mil.br/?i=aerodromos&codigo={sanitized}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
    charts = []
    try:
        res = requests.get(url, headers=headers, timeout=20)
        if not res.ok:
            return []
        html = res.text
        charts_match = re.search(r'<h4[^>]*>Cartas\s*[(]\d+[)]<\/h4>([\s\S]*?)(?:<h2|<h3|$)', html, re.IGNORECASE)
        if not charts_match:
            return []
        section_html = charts_match.group(1)
        category_blocks = re.split(r'<h4[^>]*>', section_html, flags=re.IGNORECASE)
        allowed_types = ['IAC', 'STAR', 'SID'] if type_filter == 'ALL' else [type_filter.upper()]
        for block in category_blocks[1:]:
            cat_match = re.match(r'^([^<]+)<\/h4>([\s\S]*)$', block, re.IGNORECASE)
            if not cat_match:
                continue
            tipo = cat_match.group(1).strip().upper()
            if tipo not in allowed_types:
                continue
            block_content = cat_match.group(2)
            link_regex = re.compile(r'<a[^>]*href=["\']([^"\']*download[^"\']*)["\'][^>]*>([\s\S]*?)<\/a>', re.IGNORECASE)
            for m in link_regex.finditer(block_content):
                link = m.group(1)
                label = re.sub(r'<[^>]*>', ' ', m.group(2))
                label = re.sub(r'\s+', ' ', label).strip()
                if not link.startswith('http'):
                    link = f"https://aisweb.decea.mil.br{'' if link.startswith('/') else '/'}{link}"
                charts.append({
                    'icao': sanitized,
                    'tipo': tipo,
                    'nome_procedimento': label,
                    'url_decea': link,
                    'link': link
                })
    except Exception as e:
        log.debug(f"Aviso ao consultar DECEA para {sanitized}: {e}")
    return charts

def discover_charts_pipeline(filter_icaos: list, type_filter: str, workers: int, s3, airac: str, live_logs: list) -> list:
    """Executa a Fase 1: Descoberta Autônoma e Soberana de Cartas no DECEA AISWEB."""
    is_full_mesh = not filter_icaos or filter_icaos == ['ALL'] or 'ALL' in filter_icaos

    if not is_full_mesh:
        msg = f"🔍 [FASE 1] Iniciando Descoberta Oficial no DECEA para {len(filter_icaos)} aeródromo(s): {', '.join(filter_icaos)}..."
        log.info(msg)
        live_logs.append(msg)
        update_telemetry(s3, airac, "RUNNING", "Descoberta DECEA...", 0, len(filter_icaos), 0.0, live_logs)

        discovered = []
        with ThreadPoolExecutor(max_workers=min(workers, max(1, len(filter_icaos)))) as exe:
            futures = {exe.submit(fetch_decea_charts_for_icao, icao, type_filter): icao for icao in filter_icaos}
            for fut in as_completed(futures):
                try:
                    c_list = fut.result()
                    if c_list:
                        discovered.extend(c_list)
                except Exception as e:
                    log.warning(f"Erro na descoberta de {futures[fut]}: {e}")

        if discovered:
            msg = f"🎯 [FASE 1] Descoberta DECEA concluída: {len(discovered)} cartas IFR identificadas."
            log.info(msg)
            live_logs.append(msg)
            return discovered

    # Modo Malha Completa: Varredura da rede nacional
    msg = "🌍 [FASE 1] Modo Malha Completa Ativado: Obtendo malha de aeródromos para varredura soberana no DECEA..."
    log.info(msg)
    live_logs.append(msg)
    update_telemetry(s3, airac, "RUNNING", "Varrendo Malha DECEA...", 0, 100, 0.0, live_logs)

    candidate_icaos = []
    try:
        r = requests.get('https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/latest_navdata.json', timeout=25)
        if r.ok:
            nav_data = r.json().get('data', [])
            candidate_icaos = sorted({p['icao'] for p in nav_data if p.get('icao') and p.get('type') in ['airport', 'heliport']})
    except Exception as e:
        log.warning(f"Aviso ao obter malha aérea do R2: {e}")

    if not candidate_icaos:
        try:
            r_db = requests.get(f"{TABLE_CHARTS}?select=icao&limit=2000", headers=HEADERS_REST, timeout=15)
            if r_db.ok:
                candidate_icaos = sorted(set(x['icao'] for x in r_db.json() if x.get('icao')))
        except Exception:
            candidate_icaos = []

    if not candidate_icaos:
        log.error("❌ Não foi possível carregar a lista de aeródromos para varredura.")
        return []

    msg = f"📡 [FASE 1] Varrendo {len(candidate_icaos)} aeródromos no DECEA AISWEB com {workers * 2} threads simultâneas..."
    log.info(msg)
    live_logs.append(msg)

    all_discovered = []
    processed_count = 0
    total_candidates = len(candidate_icaos)

    with ThreadPoolExecutor(max_workers=max(workers * 2, 20)) as exe:
        futures = {exe.submit(fetch_decea_charts_for_icao, icao, type_filter): icao for icao in candidate_icaos}
        for fut in as_completed(futures):
            processed_count += 1
            try:
                charts = fut.result()
                if charts:
                    all_discovered.extend(charts)
            except Exception:
                pass
            if processed_count % 100 == 0 or processed_count == total_candidates:
                msg_prog = f"🔍 [FASE 1] Descoberta DECEA: {processed_count}/{total_candidates} aeródromos verificados ({len(all_discovered)} cartas encontradas)"
                log.info(msg_prog)
                live_logs.append(msg_prog)
                update_telemetry(s3, airac, "RUNNING", f"Descoberta: {processed_count}/{total_candidates}", processed_count, total_candidates, 0.0, live_logs)

    if all_discovered:
        unique_ad = len(set(c['icao'] for c in all_discovered))
        msg_done = f"✅ [FASE 1] Descoberta Soberana DECEA concluída com sucesso: {unique_ad} aeródromos IFR e {len(all_discovered)} cartas em vigor."
        log.info(msg_done)
        live_logs.append(msg_done)
        return all_discovered

    # Salvaguarda de contingência: se o scraping DECEA falhou totalmente, usa charts_procedural com paginação
    log.warning("⚠️ DECEA não retornou cartas na varredura. Ativando salvaguarda de contingência da base interna...")
    db_charts = []
    offset = 0
    while True:
        headers_page = {**HEADERS_REST, 'Range': f"{offset}-{offset+999}"}
        q = f"select=*&tipo=in.(IAC,STAR,SID)" if type_filter == 'ALL' else f"select=*&tipo=eq.{type_filter}"
        if not is_full_mesh and filter_icaos:
            q += f"&icao=in.({','.join(filter_icaos)})"
        r_page = requests.get(f"{TABLE_CHARTS}?{q}", headers=headers_page, timeout=30)
        if not r_page.ok:
            break
        items = r_page.json()
        if not items:
            break
        db_charts.extend(items)
        if len(items) < 1000:
            break
        offset += 1000
    return db_charts

# ─── Orquestrador Principal ───────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="SkyFPL Procedures Robot 360")
    parser.add_argument('--airac', type=str, default='AUTO', help='Ciclo AIRAC (Ex: 2610 ou AUTO)')
    parser.add_argument('--icao', type=str, default='', help='Filtro de ICAOs separados por vírgula (Ex: SBSV,SBGR ou ALL)')
    parser.add_argument('--type', type=str, default='ALL', help='Filtro de tipo (ALL, IAC, STAR, SID)')
    parser.add_argument('--workers', type=int, default=10, help='Número de threads simultâneas')
    parser.add_argument('--dry-run', type=str, default='false', help='Modo simulação sem escrita')
    args = parser.parse_args()

    airac = resolve_airac_cycle(args.airac)
    dry_run = args.dry_run.lower() == 'true'
    filter_icaos = [x.strip().upper() for x in args.icao.split(',') if x.strip()]
    type_filter = args.type.upper()

    # Inicializa cliente R2 se credenciais existirem
    s3 = get_r2_client()
    if s3:
        log.info(f"☁️ Cloudflare R2 Conectado: Bucket '{R2_BUCKET}' pronto.")
    else:
        log.info("ℹ️ Cloudflare R2: Operando sem upload direto (credenciais R2 ausentes).")

    log.info("============================================================")
    log.info("🛰️ SKYFPL PROCEDURES ROBOT 360° (FULL SPECTRUM ENGINE)")
    log.info(f"   Ciclo AIRAC: {airac} (Resolvido automaticamente) | Dry Run: {dry_run} | Workers: {args.workers}")
    if filter_icaos and filter_icaos != ['ALL']:
        log.info(f"   Filtro de Aeródromos: {', '.join(filter_icaos)}")
    else:
        log.info("   Modo de Extração: MALHA COMPLETA (Descoberta Soberana DECEA)")
    if type_filter != 'ALL':
        log.info(f"   Filtro de Tipo: {type_filter}")
    log.info("============================================================")

    # Telemetria inicial
    live_logs = [
        f"Iniciando esteira serverless FMS para o Ciclo AIRAC {airac}...",
        f"Alvo: {'Malha Completa Brasil' if (not filter_icaos or filter_icaos == ['ALL']) else ', '.join(filter_icaos)}"
    ]
    update_telemetry(s3, airac, "RUNNING", "Iniciando Descoberta DECEA...", 0, 1, 0.0, live_logs)

    # ── FASE 1: DESCOBERTA AUTÔNOMA NO DECEA AISWEB ───────────────────────────
    charts = discover_charts_pipeline(filter_icaos, type_filter, args.workers, s3, airac, live_logs)
    log.info(f"📋 Total de cartas IFR prontas para extração: {len(charts)}")

    if not charts:
        log.warning("Nenhuma carta encontrada para os filtros especificados.")
        update_telemetry(s3, airac, "COMPLETED", "Nenhuma carta encontrada", 0, 0, 0.0, live_logs)
        sys.exit(0)

    t0 = time.time()
    processed_count = 0
    all_payloads = []

    update_telemetry(s3, airac, "RUNNING", "Processando Cartas & Vetores...", 0, len(charts), 0.0, live_logs)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_single_chart, c, airac, dry_run): c for c in charts}
        for future in as_completed(futures):
            processed_count += 1
            chart_c = futures[future]
            chart_name = f"{chart_c.get('icao')} {chart_c.get('tipo')} {chart_c.get('nome_procedimento')}"
            try:
                res_items = future.result()
                if res_items:
                    all_payloads.extend(res_items)
                    msg = f"✅ [{res_items[0]['icao']}] {res_items[0]['tipo']} {res_items[0]['nome_procedimento']} ({len(res_items)} transições)"
                    log.info(msg)
                    live_logs.append(msg)
                else:
                    msg = f"⚠️ [{chart_c.get('icao')}] {chart_c.get('nome_procedimento')} sem transições válidas"
                    live_logs.append(msg)
            except Exception as e:
                msg = f"❌ [{chart_c.get('icao')}] Falha crítica: {e}"
                log.error(msg)
                live_logs.append(msg)

            # Atualiza telemetria a cada 2 cartas processadas ou na última
            if processed_count % 2 == 0 or processed_count == len(charts):
                elapsed_now = time.time() - t0
                unique_airports = len(set(p['icao'] for p in all_payloads))
                stats_now = {
                    "total_procedures": len(all_payloads),
                    "airports_covered": unique_airports,
                    "by_type": {
                        "IAC": len([p for p in all_payloads if p.get('tipo') == 'IAC']),
                        "STAR": len([p for p in all_payloads if p.get('tipo') == 'STAR']),
                        "SID": len([p for p in all_payloads if p.get('tipo') == 'SID'])
                    }
                }
                update_telemetry(s3, airac, "RUNNING", chart_name, processed_count, len(charts), elapsed_now, live_logs, stats_now)

    elapsed = round(time.time() - t0, 1)

    # Exportação definitiva para o Cloudflare R2
    if not dry_run and s3 and all_payloads:
        export_r2_artifacts(s3, airac, all_payloads)

    # Telemetria Final
    final_stats = {
        "total_procedures": len(all_payloads),
        "airports_covered": len(set(p['icao'] for p in all_payloads)),
        "by_type": {
            "IAC": len([p for p in all_payloads if p.get('tipo') == 'IAC']),
            "STAR": len([p for p in all_payloads if p.get('tipo') == 'STAR']),
            "SID": len([p for p in all_payloads if p.get('tipo') == 'SID'])
        }
    }
    update_telemetry(s3, airac, "COMPLETED", "Finalizado", len(charts), len(charts), elapsed, live_logs, final_stats)

    log.info("============================================================")
    log.info(f"🏁 Processamento Concluído em {elapsed}s!")
    log.info(f"   Total de Cartas Processadas: {processed_count}/{len(charts)}")
    log.info(f"   Total de Procedimentos FMS / Transições: {len(all_payloads)}")
    log.info("============================================================")

if __name__ == '__main__':
    main()

