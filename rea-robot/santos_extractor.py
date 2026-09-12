#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modulo de Extracao Vetorial e Sanitizacao da REH Bacia de Santos (DECEA).
Extrai com precisao cirurgica os 105 fixos canonicos puros do PDF oficial.
"""

import os
import re
import json
import requests
import fitz  # PyMuPDF

OFFICIAL_5_LETTER = {
    'ARUBU', 'OBLOL', 'ASIGO', 'ITEKI', 'DIBIL', 'EGUDI', 
    'ANKOP', 'BAKUT', 'EKURI', 'PAPIS', 'DOKRA', 'XONER', 
    'XOLAP', 'KADUS', 'ISEMO', 'TOLIN', 'ALDIV'
}

PLATFORM_REMARKS = {
    'BS062': '[REH] Fixo Oficial DECEA — Plataforma FPSO Ilha Bela',
    'BS063': '[REH] Fixo Oficial DECEA — Plataforma FPSO São Paulo',
    'BS066': '[REH] Fixo Oficial DECEA — Plataforma FPSO Itaguaí',
    'BS073': '[REH] Fixo Oficial DECEA — Plataforma FPSO Mangaratiba',
    'BS074': '[REH] Fixo Oficial DECEA — Plataforma FPSO Maricá',
    'BS076': '[REH] Fixo Oficial DECEA — Plataformas P-66 / FPSO Angra dos Reis',
    'BS077': '[REH] Fixo Oficial DECEA — Plataforma P-69',
    'BS086': '[REH] Fixo Oficial DECEA — Plataformas P-67 / FPSO Paraty',
    'BS087': '[REH] Fixo Oficial DECEA — Plataforma FPSO Saquarema',
    'BS092': '[REH] Fixo Oficial DECEA — Plataformas P-74 / P-76',
    'BS093': '[REH] Fixo Oficial DECEA — Plataforma P-75',
    'BS097': '[REH] Fixo Oficial DECEA — Plataforma P-77',
    'ITEKI': '[REH] Fixo Oficial DECEA — Plataforma P-68'
}

IGNORE_WORDS = {
    'OCEANO', 'ATLÂNTICO', 'BRASIL', 'DECEA', 'ICA', 'LIMIT', 'KNOTS', 'ALT', 'MSL', 
    'SANTOS', 'BACIA', 'CORR', 'CORREDOR', 'ESPECIAL', 'HELICÓPTEROS', 'NOTAS', 'ESCALA',
    'MILHAS', 'NÁUTICAS', 'PROJEÇÃO', 'MERCATOR', 'COORDENADAS', 'WGS-84', 'DECLINAÇÃO',
    'MAGNÉTICA', 'ANUAL', 'VARIAÇÃO', 'FONTE', 'CARTAS', 'VISUAIS', 'REH', 'ALTITUDE'
}

def download_bacia_pdf(cache_dir: str = None) -> str:
    """Obtem o PDF oficial da REH Bacia de Santos via AISWEB ou fallback local."""
    if cache_dir is None:
        cache_dir = os.path.dirname(__file__)
    
    local_pdf = os.path.join(cache_dir, "bacia-de-santos_reh.pdf")
    
    # Lista de URLs candidatas (data atual ou datas conhecidas do DECEA)
    candidate_urls = [
        "https://aisweb.decea.mil.br/cartas/visuais/reh/bacia-de-santos_reh_20241128.pdf",
        "https://aisweb.decea.mil.br/cartas/visuais/reh/bacia-de-santos_reh_20241031.pdf",
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for url in candidate_urls:
        try:
            print(f"📥 Tentando baixar PDF Bacia de Santos de {url}...")
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200 and len(r.content) > 100000 and r.content.startswith(b'%PDF'):
                with open(local_pdf, 'wb') as f:
                    f.write(r.content)
                print(f"✅ PDF baixado com sucesso ({len(r.content):,} bytes)!")
                return local_pdf
        except Exception as e:
            print(f"⚠️ Falha ao baixar de {url}: {e}")
            
    if os.path.exists(local_pdf):
        print(f"ℹ️ Usando PDF em cache local: {local_pdf}")
        return local_pdf
        
    # Verificar scratch do ide se existir
    scratch_pdf = "C:/Users/josemir/.gemini/antigravity-ide/brain/692cb77b-1050-4ead-afd7-144804ecd9e4/scratch/bacia_de_santos.pdf"
    if os.path.exists(scratch_pdf):
        return scratch_pdf

    raise RuntimeError("Não foi possível obter o PDF oficial da Bacia de Santos.")

def extract_bacia_santos_fixes(pdf_path: str = None) -> list:
    """
    Executa a extração vetorial PyMuPDF da carta DECEA REH Bacia de Santos.
    Retorna lista de 105 fixos canônicos puros.
    """
    if pdf_path is None or not os.path.exists(pdf_path):
        pdf_path = download_bacia_pdf()

    doc = fitz.open(pdf_path)
    page = doc[0]
    words = page.get_text("words")

    # Passo 1: Detectar nomes de fixos candidatos
    fix_names = []
    for i, w in enumerate(words):
        txt = w[4].strip()
        if re.match(r'^(BS|CS)\d{2,4}$', txt):
            fix_names.append({'name': txt, 'x': (w[0]+w[2])/2, 'y': (w[1]+w[3])/2})
        elif txt in OFFICIAL_5_LETTER:
            fix_names.append({'name': txt, 'x': (w[0]+w[2])/2, 'y': (w[1]+w[3])/2})

    # Passo 2: Extrair coordenadas lat/lng
    lat_tokens = []
    lng_tokens = []

    for i, w in enumerate(words):
        txt = w[4].strip()
        m_lat = re.match(r'^S(2[2-6])$', txt)
        if m_lat and i + 1 < len(words):
            next_txt = words[i+1][4].strip()
            m_min = re.match(r'^(\d{2})[.,](\d{1,2})$', next_txt)
            if m_min:
                deg = float(m_lat.group(1))
                minute = float(f"{m_min.group(1)}.{m_min.group(2)}")
                dec = -(deg + minute / 60.0)
                lat_tokens.append({
                    'dec': dec,
                    'x': (w[0] + words[i+1][2])/2,
                    'y': (w[1] + words[i+1][3])/2
                })

        m_lng = re.match(r'^W(4[1-4])$', txt)
        if m_lng and i + 1 < len(words):
            next_txt = words[i+1][4].strip()
            m_min = re.match(r'^(\d{2})[.,](\d{1,2})$', next_txt)
            if m_min:
                deg = float(m_lng.group(1))
                minute = float(f"{m_min.group(1)}.{m_min.group(2)}")
                dec = -(deg + minute / 60.0)
                lng_tokens.append({
                    'dec': dec,
                    'x': (w[0] + words[i+1][2])/2,
                    'y': (w[1] + words[i+1][3])/2
                })

    # Passo 3: Associação espacial nome -> coordenada
    matched_fixes = []
    for f in fix_names:
        best_lat = None
        best_lat_dist = 60
        for lat in lat_tokens:
            d = ((f['x'] - lat['x'])**2 + (f['y'] - lat['y'])**2)**0.5
            if d < best_lat_dist:
                best_lat_dist = d
                best_lat = lat

        best_lng = None
        best_lng_dist = 60
        for lng in lng_tokens:
            d = ((f['x'] - lng['x'])**2 + (f['y'] - lng['y'])**2)**0.5
            if d < best_lng_dist:
                best_lng_dist = d
                best_lng = lng

        if best_lat and best_lng:
            matched_fixes.append({
                'name': f['name'],
                'lat': round(best_lat['dec'], 6),
                'lng': round(best_lng['dec'], 6)
            })

    # Passo 4: Deduplicação e Formatação Canônica SkyFPL
    seen = set()
    canonical_points = []
    # Ordenar por nome para estabilidade
    matched_fixes.sort(key=lambda x: x['name'])

    for mf in matched_fixes:
        name = mf['name']
        if name in seen:
            continue
        seen.add(name)

        is_portao = name in PLATFORM_REMARKS
        remarks = PLATFORM_REMARKS.get(name, '[REH] Fixo de Navegacao Offshore DECEA')
        freq = '122.550 MHz' if is_portao else '129.200 MHz'
        coord_hash = abs(int(mf['lat'] * 1000) + int(mf['lng'] * 1000))

        canonical_points.append({
            'id': f"rea-BACIA_DE_SANTOS-{name}-{coord_hash}",
            'name': name,
            'lat': mf['lat'],
            'lng': mf['lng'],
            'terminal': 'BACIA DE SANTOS',
            'aic_source': 'CCV REH BACIA DE SANTOS',
            'frequency': freq,
            'remarks': remarks,
            'ceiling': 'FL100',
            'floor': 'MSL'
        })

    return canonical_points

if __name__ == '__main__':
    points = extract_bacia_santos_fixes()
    print(f"Extraídos {len(points)} fixos canônicos da Bacia de Santos.")
    sample = [p['name'] for p in points[:10]]
    print(f"Amostra: {sample}")
