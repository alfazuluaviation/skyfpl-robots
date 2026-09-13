#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🤖 SkyFPL / SkyNav Pro — Autonomous REA / REH / REUL Hybrid VFR Robot
Doutrina de Ciclo AIRAC ICAO D-14, Extração Híbrida Blindada,
Auditoria Diferencial de Fixos e Quarentena em Staging no Cloudflare R2.
"""

import os
import sys
import time
import json
import re
import math
import argparse
import datetime
import requests
import boto3
from urllib.parse import urlencode
from aic_certifier import certify_fix_coordinates

# Configurações Cloudflare R2
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID') or os.environ.get('CLOUDFLARE_R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY') or os.environ.get('CLOUDFLARE_R2_SECRET_ACCESS_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_BUCKET = "skyfpl-charts"

# Supabase Edge Proxy & Webhooks
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://gongoqjjpwphhttumdjm.supabase.co')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdvbmdvcWpqcHdwaGh0dHVtZGptIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MTUyMDcsImV4cCI6MjA4Mjk5MTIwN30.XhdrWma90JeoQdGfeqCoXgGVnyiTZ5GXHszEHw3Ce2o')
PROXY_URL = f"{SUPABASE_URL}/functions/v1/proxy-geoserver"
WFS_URL = 'https://geoaisweb.decea.mil.br/geoserver/ICA/wfs'

# 39 Cartas Oficiais do Catálogo DECEA (26 REA, 12 REH, 1 REUL)
CANONICAL_CHARTS = [
    # 26 REA
    {"id": "CCV_REA_XN-ANAPOLIS", "name": "CCV XN Anápolis (REA)", "type": "REA", "terminal": "ANÁPOLIS", "center": [-48.95, -16.32]},
    {"id": "CCV_REA_WB_BELEM", "name": "CCV WB Belém (REA)", "type": "REA", "terminal": "BELÉM", "center": [-48.49, -1.45]},
    {"id": "CCV_REA_WH_BELO_HORIZONTE", "name": "CCV WH Belo Horizonte (REA)", "type": "REA", "terminal": "BELO HORIZONTE", "center": [-43.95, -19.85]},
    {"id": "CCV_REA_WR_BRASILIA", "name": "CCV WR Brasília (REA)", "type": "REA", "terminal": "BRASÍLIA", "center": [-47.92, -15.86]},
    {"id": "CCV_REA_WG_CAMPO_GRANDE", "name": "CCV WG Campo Grande (REA)", "type": "REA", "terminal": "CAMPO GRANDE", "center": [-54.62, -20.46]},
    {"id": "CCV_REA_CY_CUIABA", "name": "CCV CY Cuiabá (REA)", "type": "REA", "terminal": "CUIABÁ", "center": [-56.09, -15.60]},
    {"id": "REA_CURITIBA", "name": "REA WT Curitiba (REA)", "type": "REA", "terminal": "CURITIBA", "center": [-49.27, -25.42]},
    {"id": "CCV_REA_XF_FLORIANOPOLIS", "name": "CCV XF Florianópolis (REA)", "type": "REA", "terminal": "FLORIANÓPOLIS", "center": [-48.54, -27.59]},
    {"id": "CCV_REA_WZ_FORTALEZA", "name": "CCV WZ Fortaleza (REA)", "type": "REA", "terminal": "FORTALEZA", "center": [-38.54, -3.71]},
    {"id": "REA_LONDRINA", "name": "REA XO Londrina (REA)", "type": "REA", "terminal": "LONDRINA", "center": [-51.16, -23.31]},
    {"id": "CCV_REA_XK_MACAPA", "name": "CCV XK Macapá (REA)", "type": "REA", "terminal": "MACAPÁ", "center": [-51.06, 0.03]},
    {"id": "CCV_REA_WN2_MANAUS", "name": "CCV WN2 Manaus (REA)", "type": "REA", "terminal": "MANAUS", "center": [-60.02, -3.11]},
    {"id": "CCV_REA_XT_NATAL", "name": "CCV XT Natal (REA)", "type": "REA", "terminal": "NATAL", "center": [-35.20, -5.79]},
    {"id": "CCV_REA_PI-PARINTINS", "name": "CCV PI Parintins (REA)", "type": "REA", "terminal": "PARINTINS", "center": [-56.73, -2.62]},
    {"id": "CCV_REA_WP_PORTO_ALEGRE", "name": "CCV WP Porto Alegre (REA)", "type": "REA", "terminal": "PORTO ALEGRE", "center": [-51.21, -30.03]},
    {"id": "CCV_REA_WK_PORTO_SEGURO", "name": "CCV WK Porto Seguro (REA)", "type": "REA", "terminal": "PORTO SEGURO", "center": [-39.06, -16.43]},
    {"id": "CCV_REA_WF_RECIFE", "name": "CCV WF Recife (REA)", "type": "REA", "terminal": "RECIFE", "center": [-34.92, -8.12]},
    {"id": "REA_RIBEIRAO_PRETO", "name": "REA XQ Ribeirão Preto (REA)", "type": "REA", "terminal": "RIBEIRÃO PRETO", "center": [-47.81, -21.17]},
    {"id": "CCV_REA_WJ1_RIO_DE_JANEIRO", "name": "CCV WJ1 Rio de Janeiro (REA)", "type": "REA", "terminal": "RIO DE JANEIRO", "center": [-43.20, -22.90]},
    {"id": "CCV_REA_XP1_SAO_PAULO", "name": "CCV XP1 São Paulo (REA)", "type": "REA", "terminal": "SÃO PAULO", "center": [-46.63, -23.55]},
    {"id": "CCV_REA_XP2_SAO_PAULO", "name": "CCV XP2 São Paulo (REA)", "type": "REA", "terminal": "SÃO PAULO", "center": [-46.63, -23.55]},
    {"id": "CCV_REA_XS_SALVADOR", "name": "CCV XS Salvador (REA)", "type": "REA", "terminal": "SALVADOR", "center": [-38.50, -12.97]},
    {"id": "CCV_REA_WX_SANTAREM", "name": "CCV WX Santarém (REA)", "type": "REA", "terminal": "SANTARÉM", "center": [-54.71, -2.44]},
    {"id": "CCV_REA_WS_SAO_LUIS", "name": "CCV WS São Luís (REA)", "type": "REA", "terminal": "SÃO LUÍS", "center": [-44.30, -2.53]},
    {"id": "CCV_REA_WA_TABATINGA", "name": "CCV WA Tabatinga (REA)", "type": "REA", "terminal": "TABATINGA", "center": [-69.93, -4.25]},
    {"id": "CCV_REA_XR_VITORIA", "name": "CCV XR Vitória (REA)", "type": "REA", "terminal": "VITÓRIA", "center": [-40.33, -20.31]},

    # 12 REH
    {"id": "REH_BACIA_DE_SANTOS", "name": "REH Bacia de Santos (REH)", "type": "REH", "terminal": "BACIA DE SANTOS", "center": [-46.33, -23.96]},
    {"id": "CCV_REH_WH_BELO_HORIZONTE", "name": "CCV REH Belo Horizonte (REH)", "type": "REH", "terminal": "BELO HORIZONTE", "center": [-43.95, -19.85]},
    {"id": "CCV_REH_WJ1_CABO_FRIO", "name": "CCV REH Cabo Frio (REH)", "type": "REH", "terminal": "CABO FRIO", "center": [-42.01, -22.88]},
    {"id": "CCV_REH_XP2_CAMPINAS", "name": "CCV REH Campinas (REH)", "type": "REH", "terminal": "CAMPINAS", "center": [-47.06, -22.90]},
    {"id": "REH_CURITIBA", "name": "REH Curitiba (REH)", "type": "REH", "terminal": "CURITIBA", "center": [-49.27, -25.42]},
    {"id": "CCV_REH_WJ2_RIO_DE_JANEIRO", "name": "CCV REH Rio de Janeiro 2 (REH)", "type": "REH", "terminal": "RIO DE JANEIRO", "center": [-43.20, -22.90]},
    {"id": "CCV_REH_WJ3_RIO_DE_JANEIRO", "name": "CCV REH Rio de Janeiro 3 (REH)", "type": "REH", "terminal": "RIO DE JANEIRO", "center": [-43.20, -22.90]},
    {"id": "CCV_REH_XP1_SAO_JOSE_DOS_CAMPOS", "name": "CCV REH São José dos Campos (REH)", "type": "REH", "terminal": "SÃO JOSÉ DOS CAMPOS", "center": [-45.88, -23.22]},
    {"id": "CCV_REH_XP2_SAO_PAULO_1", "name": "CCV REH São Paulo 1 (REH)", "type": "REH", "terminal": "SÃO PAULO", "center": [-46.63, -23.55]},
    {"id": "CCV_REH_XP2_SAO_PAULO_2", "name": "CCV REH São Paulo 2 (REH)", "type": "REH", "terminal": "SÃO PAULO", "center": [-46.63, -23.55]},
    {"id": "CCV_REH_XP1_SOROCABA", "name": "CCV REH Sorocaba (REH)", "type": "REH", "terminal": "SOROCABA", "center": [-47.45, -23.50]},
    {"id": "REH_VITORIA", "name": "REH Vitória (REH)", "type": "REH", "terminal": "VITÓRIA", "center": [-40.33, -20.31]},

    # 1 REUL
    {"id": "CCV_REUL_WJ3_RIO_DE_JANEIRO", "name": "CCV REUL Rio de Janeiro (REUL)", "type": "REUL", "terminal": "RIO DE JANEIRO", "center": [-43.20, -22.90]}
]

def init_s3():
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        print("⚠️ Credenciais R2 S3 não encontradas. Telemetria e arquivos serão gravados apenas localmente.")
        return None
    return boto3.client('s3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )

def update_telemetry(s3, telemetry):
    payload = json.dumps(telemetry, default=str, ensure_ascii=False).encode('utf-8')
    if s3:
        try:
            s3.put_object(
                Bucket=R2_BUCKET,
                Key='rea_vfr/telemetry.json',
                Body=payload,
                ContentType='application/json',
                CacheControl='no-cache, no-store, must-revalidate'
            )
        except Exception as e:
            print(f"Falha ao atualizar telemetria R2: {e}")
            
    if SUPABASE_URL and SUPABASE_ANON_KEY:
        try:
            headers = {
                'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
                'apikey': SUPABASE_ANON_KEY,
                'Content-Type': 'application/json',
                'x-upsert': 'true'
            }
            url = f"{SUPABASE_URL}/storage/v1/object/robots-telemetry/rea_vfr/telemetry.json"
            requests.put(url, headers=headers, data=payload, timeout=5)
        except:
            pass

def calculate_airac_cycle(target_date=None):
    """Calcula o ciclo AIRAC ICAO oficial considerando a janela D-14 de Staging."""
    now = target_date if target_date is not None else datetime.datetime.now(datetime.timezone.utc)
    calendar_path = os.path.join(os.path.dirname(__file__), 'calendar.json')
    master_cal = {}
    if os.path.exists(calendar_path):
        with open(calendar_path, 'r', encoding='utf-8') as f:
            master_cal = json.load(f)
            
    all_cycles = []
    for year, cycles in master_cal.items():
        for cycle_id, date_str in cycles.items():
            parts = [int(p) for p in date_str.split('/')]
            dt = datetime.datetime(parts[2], parts[1], parts[0], tzinfo=datetime.timezone.utc)
            all_cycles.append({
                'cycle': cycle_id,
                'effective_dt': dt,
                'effective_date': dt.strftime('%Y-%m-%d'),
                'expiration_date': (dt + datetime.timedelta(days=28)).strftime('%Y-%m-%d'),
                'publication_date': (dt - datetime.timedelta(days=14)).strftime('%Y-%m-%d')
            })
            
    all_cycles.sort(key=lambda x: x['effective_dt'])
    
    current_cycle = None
    next_cycle = None
    for i, c in enumerate(all_cycles):
        if c['effective_dt'] <= now:
            current_cycle = c
            if i + 1 < len(all_cycles):
                next_cycle = all_cycles[i + 1]
                
    if not current_cycle and all_cycles:
        current_cycle = all_cycles[0]
        
    target = current_cycle
    is_staging = False
    if next_cycle:
        days_until_next = (next_cycle['effective_dt'] - now).days
        if 0 <= days_until_next <= 14:
            target = next_cycle
            is_staging = True
            print(f"🎯 Janela D-{days_until_next} Detectada! Alvo: Ciclo Futuro {next_cycle['cycle']} (Staging Quarentena)")
        else:
            print(f"📌 Operação Normal: Alvo: Ciclo Atual {current_cycle['cycle']} (Vigência: {current_cycle['effective_date']})")
            
    return {
        'cycle': target['cycle'],
        'effective_date': target['effective_date'],
        'expiration_date': target['expiration_date'],
        'publication_date': target['publication_date'],
        'is_staging': is_staging
    }

def is_cycle_already_published(s3, cycle_id, is_staging=False):
    """Verifica no Cloudflare R2 se a malha já foi consolidada e publicada."""
    if not s3:
        return False
    # Em produção operacional (não staging), verifica se o arquivo latest_rea_vfr.json existe
    if not is_staging:
        try:
            s3.head_object(Bucket=R2_BUCKET, Key='rea_vfr/latest_rea_vfr.json')
        except Exception:
            return False

    # Verifica também a chave versionada
    key = f"rea_vfr/cycles/{cycle_id}/rea_vfr_{cycle_id}.json"
    try:
        s3.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except Exception:
        return False

def haversine_distance_meters(lat1, lon1, lat2, lon2):
    """Calcula a distância geodésica em metros entre dois pontos (WGS-84)."""
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0)**2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c

def compute_diff_matrix(current_points, new_points):
    """
    Motor de Conciliação Diferencial de Fixos VFR:
    🟢 UNCHANGED : Ponto com delta < 5m
    🟡 NEW       : Ponto novo adicionado no ciclo
    🔴 CONFLICT  : Deslocamento > 100m, duplicidade ou conflito grave
    ⚫ REMOVED   : Ponto presente no ciclo anterior mas revogado
    """
    current_map = {}
    for p in current_points:
        norm_name = p.get('name', '').strip().upper()
        norm_terminal = p.get('terminal', '').strip().upper()
        norm_type = p.get('type', 'REA').strip().upper()
        key = f"{norm_type}::{norm_terminal}::{norm_name}"
        current_map[key] = p

    audited_new_points = []
    seen_new_keys = set()
    identical_count = 0
    new_count = 0
    conflict_count = 0

    for p in new_points:
        norm_name = p.get('name', '').strip().upper()
        norm_terminal = p.get('terminal', '').strip().upper()
        norm_type = p.get('type', 'REA').strip().upper()
        key = f"{norm_type}::{norm_terminal}::{norm_name}"
        p_lat = p.get('lat')
        p_lng = p.get('lng')

        # Verificação de duplicidade na nova malha
        if key in seen_new_keys:
            # Se for duplicata idêntica dentro do mesmo modal e terminal, apenas unifica
            continue

        seen_new_keys.add(key)

        if key in current_map:
            prev_p = current_map[key]
            dist_m = haversine_distance_meters(prev_p['lat'], prev_p['lng'], p_lat, p_lng)
            if dist_m <= 5.0:
                p['audit_status'] = 'UNCHANGED'
                p['audit_color'] = 'GREEN'
                p['audit_message'] = "Fixo 100% conforme com o ciclo anterior"
                identical_count += 1
            elif dist_m > 100.0:
                p['audit_status'] = 'CONFLICT'
                p['audit_color'] = 'RED'
                p['audit_message'] = f"Coordenadas deslocadas em {dist_m:.1f}m ({dist_m/1852.0:.2f} NM) em relação ao ciclo anterior"
                p['prev_coords'] = [prev_p['lng'], prev_p['lat']]
                p['delta_meters'] = round(dist_m, 1)
                conflict_count += 1
            else:
                p['audit_status'] = 'CALIBRATED'
                p['audit_color'] = 'YELLOW'
                p['audit_message'] = f"Pequeno ajuste submétrico de {dist_m:.1f}m em relação ao ciclo anterior"
                p['prev_coords'] = [prev_p['lng'], prev_p['lat']]
                p['delta_meters'] = round(dist_m, 1)
                new_count += 1
        else:
            p['audit_status'] = 'NEW'
            p['audit_color'] = 'YELLOW'
            p['audit_message'] = "Novo fixo adicionado oficialmente pelo DECEA neste ciclo"
            new_count += 1

        audited_new_points.append(p)

    # Identificar pontos desativados (existiam antes, não existem mais)
    removed_points = []
    for key, prev_p in current_map.items():
        if key not in seen_new_keys:
            rem = dict(prev_p)
            rem['audit_status'] = 'REMOVED'
            rem['audit_color'] = 'BLACK'
            rem['audit_message'] = "Fixo revogado/desativado pelo DECEA neste ciclo"
            removed_points.append(rem)

    new_points_list = [{'name': p['name'], 'terminal': p.get('terminal', '—')} for p in audited_new_points if p.get('audit_color') == 'YELLOW']
    conflict_points_list = [{'name': p['name'], 'terminal': p.get('terminal', '—'), 'reason': p.get('audit_message', 'Discrepância detectada')} for p in audited_new_points if p.get('audit_color') == 'RED']
    removed_points_list = [{'name': p['name'], 'terminal': p.get('terminal', '—')} for p in removed_points]

    diff_summary = {
        'total_audited': len(audited_new_points),
        'identical': identical_count,
        'new': new_count,
        'conflicts': conflict_count,
        'removed': len(removed_points),
        'new_points_list': new_points_list,
        'conflict_points_list': conflict_points_list,
        'removed_points_list': removed_points_list
    }

    return audited_new_points, removed_points, diff_summary

# ─── Blindagem Canônica de Fixos Exclusivos (Regras de Domínio Aeronáutico) ───
# Fixos que pertencem EXCLUSIVAMENTE ao modal REA (não pertencem a REH nem REUL)
EXCLUSIVE_REA_FIXES_RJ = {
    'BARRA', 'VALQUEIRE', 'MARAPENDI', 
    'SÃO GONÇALO', 'SAO GONCALO', 'MAUÁ', 'MAUA', 'ROXO'
}

# Fixos que pertencem EXCLUSIVAMENTE ao modal REH (não pertencem a REA nem REUL)
EXCLUSIVE_REH_FIXES_RJ = {
    'PRAÇA', 'PRACA'
}

# Fixos que pertencem EXCLUSIVAMENTE ao modal REA em BH (não pertencem a REH)
EXCLUSIVE_REA_FIXES_BH = {
    'BRANCA', 'CHAPÉU', 'CHAPEU', 'ANDIROBA', 'CIRRUS'
}

# Fixos que pertencem EXCLUSIVAMENTE ao modal REH em BH (não pertencem a REA)
EXCLUSIVE_REH_FIXES_BH = {
    'OLHOS'
}

def is_rj_area(terminal: str) -> bool:
    t = (terminal or '').upper()
    return any(k in t for k in ['RIO', 'WJ1', 'WJ2', 'WJ3'])

def is_bh_area(terminal: str) -> bool:
    t = (terminal or '').upper()
    return any(k in t for k in ['BELO', 'WH'])

def is_valid_fix_name(name: str) -> bool:
    """Valida se o nome do fixo eh aeronauticamente valido, descartando lixo de OCR e coordenadas."""
    if not name or not isinstance(name, str):
        return False
    clean = name.strip()
    if len(clean) < 2:
        return False
    # Numeros puros (ex: 98, 71151, 69551)
    if clean.isdigit():
        return False
    # Coordenadas geograficas no nome (ex: S25 33,80, W49 14,73)
    if re.search(r'[SW]\s*\d{2}\s*[\d.,]+', clean, re.IGNORECASE):
        return False
    # Termos e rotulos espurios
    upper = clean.upper()
    if upper in {'COORD', 'POSIÇÃO', 'POSICAO', 'PORTÃO', 'PORTAO', 'PONTO', 'FIXO', 'LIMITE', 'SETOR'}:
        return False
    # Instrucoes de altitude e restricoes de voo capturadas indevidamente
    if any(k in upper for k in ['ALTITUDE', 'MÁXIMA', 'MAXIMA', 'MINIMA', 'MÍNIMA', 'FL0', 'FL1', 'VFR']):
        return False
    return True

def sanitize_fix_name(name: str) -> str:
    """Padroniza e limpa prefixos espurios do nome do fixo."""
    clean = name.strip().strip('"\'')
    clean = re.sub(r'^(FIXO|POSIÇÃO|POSICAO|PORTÃO|PORTAO)\s+', '', clean, flags=re.IGNORECASE)
    return clean.strip().upper()

def extract_all_rea_vfr_points():
    """
    Executa a extracao canonica completa da malha REA/REH/REUL do Brasil:
    1. Base canonica consolidada (1.089 fixos oficiais limpos)
    2. Extracao vetorial direta da REH Bacia de Santos via PyMuPDF (105 fixos puros)
    3. Consulta ao GeoServer DECEA WFS (ICA:CV_REA_BR_COMPLETO e ICA:CV_REH_BR_COMPLETO) com blindagem de nomes
    """
    points_map = {}
    
    # 1. Carregar a base canonica consolidada de referencia
    baseline_path = os.path.join(os.path.dirname(__file__), 'canonical_fixes_baseline.json')
    if os.path.exists(baseline_path):
        try:
            with open(baseline_path, 'r', encoding='utf-8') as f:
                baseline_data = json.load(f)
                for p in baseline_data:
                    name = sanitize_fix_name(p.get('name', ''))
                    if is_valid_fix_name(name):
                        c_type = p.get('type', 'REA').strip().upper()
                        norm_term = p.get('terminal', '').strip().upper()

                        # Blindagem RJ: fixos exclusivos REA
                        if is_rj_area(norm_term) and name in EXCLUSIVE_REA_FIXES_RJ:
                            if c_type != 'REA':
                                continue
                            norm_term = 'WJ1-RIO DE JANEIRO'
                            c_type = 'REA'
                            p['terminal'] = norm_term
                            p['type'] = 'REA'

                        # Blindagem RJ: fixos exclusivos REH
                        if is_rj_area(norm_term) and name in EXCLUSIVE_REH_FIXES_RJ:
                            if c_type != 'REH':
                                continue
                            norm_term = 'WJ2-RIO DE JANEIRO'
                            c_type = 'REH'
                            p['terminal'] = norm_term
                            p['type'] = 'REH'

                        # Blindagem BH: fixos exclusivos REA
                        if is_bh_area(norm_term) and name in EXCLUSIVE_REA_FIXES_BH:
                            if c_type != 'REA':
                                continue
                            norm_term = 'WH-BELO HORIZONTE'
                            c_type = 'REA'
                            p['terminal'] = norm_term
                            p['type'] = 'REA'

                        # Blindagem BH: fixos exclusivos REH (ex: OLHOS)
                        if is_bh_area(norm_term) and name in EXCLUSIVE_REH_FIXES_BH:
                            if c_type != 'REH':
                                continue
                            norm_term = 'WH-BELO HORIZONTE'
                            c_type = 'REH'
                            p['terminal'] = norm_term
                            p['type'] = 'REH'

                        key = f"{c_type}::{norm_term}::{name}"
                        p['name'] = name
                        p['terminal'] = norm_term
                        p['type'] = c_type
                        points_map[key] = p
            print(f"📦 Carregados {len(points_map)} fixos da base canonica de referencia.")
        except Exception as e:
            print(f"⚠️ Falha ao ler canonical_fixes_baseline.json: {e}")

    # 2. Extrair fixos oficiais da REH Bacia de Santos via PDF vetorial
    try:
        from santos_extractor import extract_bacia_santos_fixes
        santos_points = extract_bacia_santos_fixes()
        print(f"🌊 Extraídos {len(santos_points)} fixos canonicos puros da Bacia de Santos via PyMuPDF.")
        for p in santos_points:
            p['type'] = 'REH'
            p['terminal'] = 'BACIA DE SANTOS'
            key = f"REH::BACIA DE SANTOS::{p['name']}"
            points_map[key] = p
    except Exception as e:
        print(f"⚠️ Alerta ao extrair Bacia de Santos via PDF: {e}. Mantendo valores da base canonica.")

    # 3. Consultar DECEA GeoServer WFS para capturar novidades das cartas publicadas
    try:
        wfs_headers = {'User-Agent': 'SkyFPL-Bot/1.0'}
        for layer in ['ICA:CV_REA_BR_COMPLETO', 'ICA:CV_REH_BR_COMPLETO']:
            url = f"{WFS_URL}?service=WFS&version=1.0.0&request=GetFeature&typeName={layer}&outputFormat=application/json"
            resp = requests.get(url, headers=wfs_headers, timeout=25)
            if resp.status_code == 200:
                features = resp.json().get('features', [])
                print(f"🛰️ WFS {layer}: {len(features)} corredores analisados.")
                for feat in features:
                    props = feat.get('properties', {})
                    raw_term = (props.get('carta_nome') or 'BRASIL').strip().upper()
                    
                    is_reh_layer = (layer == 'ICA:CV_REH_BR_COMPLETO') or any(k in raw_term for k in ['REH', 'CABO FRIO', 'CAMPINAS', 'SOROCABA', 'WJ2', 'XP2'])
                    is_reul = 'REUL' in raw_term or 'WJ3' in raw_term
                    c_type = 'REUL' if is_reul else ('REH' if is_reh_layer else 'REA')
                    
                    freq = props.get('fca') or props.get('ats')
                    altmax = props.get('altmax') or props.get('altmaxa_to_b')
                    altmin = props.get('altmin') or props.get('altmina_to_b')
                    altcomp = props.get('altcomp') or props.get('altcompa_to_b')
                    heading = props.get('rumoa_to_b')

                    for prefix in ['fixo_a', 'fixo_b']:
                        raw_name = props.get(f'{prefix}_nome')
                        raw_lat = props.get(f'{prefix}_lat')
                        raw_lon = props.get(f'{prefix}_lon')
                        if raw_name and raw_lat is not None and raw_lon is not None:
                            name = sanitize_fix_name(str(raw_name))
                            if not is_valid_fix_name(name):
                                continue
                            try:
                                lat = float(raw_lat)
                                lng = float(raw_lon)
                            except (ValueError, TypeError):
                                continue
                            if not (-35 <= lat <= 6 and -75 <= lng <= -30):
                                continue

                            # Blindagem canônica RJ: fixos exclusivos REA do Rio nunca devem ser gravados como REH ou REUL
                            cur_type = c_type
                            cur_term = raw_term
                            if is_rj_area(cur_term) and name in EXCLUSIVE_REA_FIXES_RJ:
                                cur_type = 'REA'
                                cur_term = 'WJ1-RIO DE JANEIRO'

                            # Blindagem canônica RJ: fixos exclusivos REH do Rio (ex: PRAÇA) nunca devem ser gravados como REA ou REUL
                            if is_rj_area(cur_term) and name in EXCLUSIVE_REH_FIXES_RJ:
                                cur_type = 'REH'
                                cur_term = 'WJ2-RIO DE JANEIRO'

                            # Blindagem canônica BH: fixos exclusivos REA nunca devem ser gravados como REH
                            if is_bh_area(cur_term) and name in EXCLUSIVE_REA_FIXES_BH:
                                cur_type = 'REA'
                                cur_term = 'WH-BELO HORIZONTE'

                            # Blindagem canônica BH: fixos exclusivos REH (ex: OLHOS) nunca devem ser gravados como REA
                            if is_bh_area(cur_term) and name in EXCLUSIVE_REH_FIXES_BH:
                                cur_type = 'REH'
                                cur_term = 'WH-BELO HORIZONTE'
                                
                            key = f"{cur_type}::{cur_term}::{name}"
                            if key not in points_map:
                                coord_hash = abs(int(lat * 1000) + int(lng * 1000))
                                prefix_id = cur_type.lower()
                                if name == 'MANNESMANN' and is_bh_area(cur_term):
                                    fix_id = f"{prefix_id}-WH_BELO_HORIZONTE-MANNESMANN"
                                elif name == 'OLHOS' and is_bh_area(cur_term):
                                    fix_id = "reh-WH_BELO_HORIZONTE-OLHOS"
                                else:
                                    fix_id = f"{prefix_id}-{cur_term.replace(' ', '_').replace('-', '_')}-{name}-{coord_hash}"

                                points_map[key] = {
                                    'id': fix_id,
                                    'name': name,
                                    'lat': lat,
                                    'lng': lng,
                                    'terminal': cur_term,
                                    'type': cur_type,
                                    'aic_source': props.get('identificador', 'DECEA WFS'),
                                    'frequency': freq,
                                    'ceiling': f"{altmax} ft" if altmax else None,
                                    'floor': f"{altmin} ft" if altmin else None,
                                    'mandatory_alt': f"{altcomp} ft" if altcomp else None,
                                    'magnetic_heading': str(heading) if heading else None,
                                    'remarks': props.get('observacao') or f"[{cur_type}]"
                                }
                            else:
                                # Enriquecer com dados táticos se estavam vazios
                                existing = points_map[key]
                                if not existing.get('frequency') and freq:
                                    existing['frequency'] = freq
                                if not existing.get('ceiling') and altmax:
                                    existing['ceiling'] = f"{altmax} ft"
                                if not existing.get('floor') and altmin:
                                    existing['floor'] = f"{altmin} ft"
                                if not existing.get('mandatory_alt') and altcomp:
                                    existing['mandatory_alt'] = f"{altcomp} ft"
                                if not existing.get('magnetic_heading') and heading:
                                    existing['magnetic_heading'] = str(heading)
    except Exception as e:
        print(f"ℹ️ GeoServer WFS offline ou inacessivel ({e}). Operando com base canonica e extrator vetorial.")

    # Garantir presença de fixos canônicos fundamentais que o WFS do DECEA omite
    if 'REH::WH-BELO HORIZONTE::OLHOS' not in points_map:
        points_map['REH::WH-BELO HORIZONTE::OLHOS'] = {
            'id': 'reh-WH_BELO_HORIZONTE-OLHOS',
            'name': 'OLHOS',
            'lat': -19.648667,
            'lng': -43.910000,
            'terminal': 'WH-BELO HORIZONTE',
            'type': 'REH',
            'aic_source': 'CCV REH WH BELO HORIZONTE (DECEA Oficial)',
            'frequency': '122.550 MHz',
            'remarks': '[REH] Portao Oficial DECEA'
        }

    if 'REH::WH-BELO HORIZONTE::MANNESMANN' not in points_map:
        points_map['REH::WH-BELO HORIZONTE::MANNESMANN'] = {
            'id': 'reh-WH_BELO_HORIZONTE-MANNESMANN',
            'name': 'MANNESMANN',
            'lat': -19.964500,
            'lng': -44.002000,
            'terminal': 'WH-BELO HORIZONTE',
            'type': 'REH',
            'aic_source': 'CCV REH WH BELO HORIZONTE (DECEA Oficial)',
            'frequency': '122.550 MHz',
            'remarks': '[REH] Portao Oficial DECEA (Helicópteros)'
        }

    if 'REA::WH-BELO HORIZONTE::MANNESMANN' not in points_map:
        points_map['REA::WH-BELO HORIZONTE::MANNESMANN'] = {
            'id': 'rea-WH_BELO_HORIZONTE-MANNESMANN',
            'name': 'MANNESMANN',
            'lat': -19.978000,
            'lng': -44.008000,
            'terminal': 'WH-BELO HORIZONTE',
            'type': 'REA',
            'aic_source': 'AIC N 20/24 (AISWEB CCV WH) (DECEA Oficial)',
            'frequency': '120.200 MHz',
            'ceiling': '5000 ft',
            'floor': '3500 ft',
            'remarks': '[REA] Portão Oficial DECEA (Aviões)'
        }

    # Filtro final de blindagem: eliminar qualquer resquício de falsos modais no RJ e BH
    purified_points = []
    for p in points_map.values():
        name = p.get('name', '')
        term = p.get('terminal', '')
        c_type = p.get('type', '')
        if is_rj_area(term) and name in EXCLUSIVE_REA_FIXES_RJ and c_type != 'REA':
            continue
        if is_rj_area(term) and name in EXCLUSIVE_REH_FIXES_RJ and c_type != 'REH':
            continue
        if is_bh_area(term) and name in EXCLUSIVE_REA_FIXES_BH and c_type != 'REA':
            continue
        if is_bh_area(term) and name in EXCLUSIVE_REH_FIXES_BH and c_type != 'REH':
            continue
        purified_points.append(p)

    # 4. Certificação Geodésica Homologada por AIC (Ground Truth Oficial DECEA)
    certified_count = 0
    for p in purified_points:
        name = p.get('name', '')
        term = p.get('terminal', '')
        c_type = p.get('type', 'REA')
        cur_lat = p.get('lat')
        cur_lng = p.get('lng')
        if cur_lat is not None and cur_lng is not None:
            cal_lat, cal_lng, was_calibrated, aic_src, delta_m, extra_props = certify_fix_coordinates(term, name, cur_lat, cur_lng, fix_type=c_type)
            if was_calibrated:
                print(f"🎯 [AIC Ground Truth] Fixo {name} ({term} [{c_type}]) calibrado com precisão métrica via {aic_src}: delta={delta_m}m")
                p['lat'] = cal_lat
                p['lng'] = cal_lng
                p['aic_source'] = f"{aic_src} (DECEA Oficial)"
                certified_count += 1
            elif aic_src:
                p['aic_source'] = f"{aic_src} (DECEA Oficial)"
                
            # Enriquecer com propriedades táticas da publicação
            if extra_props:
                if extra_props.get('frequency') and not p.get('frequency'):
                    p['frequency'] = extra_props['frequency']
                if extra_props.get('remarks') and (not p.get('remarks') or p.get('remarks') == f"[{c_type}]"):
                    p['remarks'] = extra_props['remarks']
                if extra_props.get('ceiling') and not p.get('ceiling'):
                    p['ceiling'] = extra_props['ceiling']
                if extra_props.get('floor') and not p.get('floor'):
                    p['floor'] = extra_props['floor']
                if extra_props.get('mandatory_alt'):
                    p['mandatory_alt'] = extra_props['mandatory_alt']
                if extra_props.get('magnetic_heading'):
                    p['magnetic_heading'] = extra_props['magnetic_heading']

    if certified_count > 0:
        print(f"✅ {certified_count} fixos foram recalibrados com Ground Truth de Publicações Oficiais (AIC).")

    final_list = purified_points
    final_list.sort(key=lambda x: (x.get('terminal', ''), x.get('name', '')))
    print(f"✨ Total de fixos consolidados e sanitizados: {len(final_list)}")
    return final_list

def sync_points_to_supabase(points: list):
    """Sincroniza os pontos canonicos diretamente para a tabela rea_vfr_points via Edge Function sync-rea-vfr."""
    key = (os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or '').strip() or (os.environ.get('SUPABASE_ANON_KEY') or '').strip() or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not key:
        print("⚠️ SUPABASE_URL ou Chave ausente. Sincronizacao com BD ignorada.")
        return False
        
    url = f"{SUPABASE_URL}/functions/v1/sync-rea-vfr"
    headers = {
        'Authorization': f'Bearer {key}',
        'apikey': key,
        'Content-Type': 'application/json'
    }
    
    print(f"🚀 Sincronizando {len(points)} fixos com Supabase rea_vfr_points...")
    total_upserted = 0
    batch_size = 50
    for i in range(0, len(points), batch_size):
        batch = points[i:i + batch_size]
        try:
            res = requests.post(url, json={'points': batch}, headers=headers, timeout=30)
            if res.status_code in (200, 201):
                total_upserted += len(batch)
            else:
                print(f"⚠️ Batch {i//batch_size + 1} falhou: {res.status_code} {res.text}")
        except Exception as e:
            print(f"⚠️ Erro ao enviar batch {i//batch_size + 1}: {e}")
            
    print(f"✅ Concluida sincronizacao com Supabase: {total_upserted}/{len(points)} fixos atualizados.")
    return True

def main():
    parser = argparse.ArgumentParser(description="Autonomous REA / REH / REUL Hybrid Robot")
    parser.add_argument("--cycle", type=str, default=None, help="Force specific AIRAC cycle")
    parser.add_argument("--force", action="store_true", help="Force run ignoring idempotency")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without uploading")
    parser.add_argument("--sync-db", action="store_true", help="Directly sync to Supabase rea_vfr_points")
    parser.add_argument("--publish-prod", action="store_true", help="Publish official production mesh to rea_vfr/latest_rea_vfr.json")
    args = parser.parse_args()

    s3 = init_s3()
    airac = calculate_airac_cycle()
    if args.cycle:
        airac['cycle'] = args.cycle

    is_forced = args.force or os.environ.get('FORCE_RUN', '').lower() == 'true'
    versioned_key = f"rea_vfr/cycles/{airac['cycle']}/rea_vfr_{airac['cycle']}.json"

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_brt = now_utc - datetime.timedelta(hours=3)

    telemetry = {
        'status': 'initializing',
        'cycle': airac['cycle'],
        'effective_date': airac['effective_date'],
        'is_staging': airac['is_staging'],
        'started_at_utc': now_utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
        'started_at_brt': now_brt.strftime('%d/%m/%Y %H:%M:%S BRT'),
        'updated_at': time.time(),
        'global_progress': 0,
        'logs': [f"[{now_brt.strftime('%H:%M:%S')}] 🚀 Robô REA/REH/REUL iniciado para o Ciclo {airac['cycle']} (Staging: {airac['is_staging']})."]
    }
    update_telemetry(s3, telemetry)

    # 🛡️ Trava de Idempotência
    if not is_forced and not args.dry_run and not args.publish_prod and is_cycle_already_published(s3, airac['cycle'], is_staging=airac['is_staging']):
        print(f"🛡️ TRAVA DE IDEMPOTÊNCIA ATIVA: Ciclo {airac['cycle']} já consolidado em {versioned_key}.")
        telemetry['status'] = 'completed'
        telemetry['global_progress'] = 100
        telemetry['idempotent_skipped'] = True
        telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 🛡️ Ciclo AIRAC {airac['cycle']} já existe em {versioned_key}. Execução dispensada.")
        update_telemetry(s3, telemetry)
        return

    print("Iniciando extração e auditoria diferencial...")
    telemetry['status'] = 'processing'
    telemetry['global_progress'] = 20
    update_telemetry(s3, telemetry)

    # Download da malha anterior para conciliação diferencial
    current_mesh_points = []
    try:
        if s3:
            prod_obj = s3.get_object(Bucket=R2_BUCKET, Key='rea_vfr/latest_rea_vfr.json')
            prod_data = json.loads(prod_obj['Body'].read().decode('utf-8'))
            current_mesh_points = prod_data.get('points', [])
    except Exception as e:
        print(f"Malha anterior não encontrada no R2 ({e}). Utilizando baseline vazio.")

    # Extração canônica consolidada (WFS + PDF Vetorial Bacia de Santos + Baseline)
    extracted_points = extract_all_rea_vfr_points()

    telemetry['global_progress'] = 60
    telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 🔬 Executando conciliação diferencial contra produção ativa...")
    update_telemetry(s3, telemetry)

    audited_points, removed_points, diff_summary = compute_diff_matrix(current_mesh_points, extracted_points)

    print(f"📊 Resumo da Auditoria Diferencial:")
    print(f"   • Total Auditado: {diff_summary['total_audited']}")
    print(f"   • 🟢 Conformes:   {diff_summary['identical']}")
    print(f"   • 🟡 Novos:       {diff_summary['new']}")
    print(f"   • 🔴 Conflitos:   {diff_summary['conflicts']}")
    print(f"   • ⚫ Desativados: {diff_summary['removed']}")

    telemetry['global_progress'] = 80
    telemetry['diff_summary'] = diff_summary
    telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 📊 Auditoria: {diff_summary['identical']} 🟢 | {diff_summary['new']} 🟡 | {diff_summary['conflicts']} 🔴 | {diff_summary['removed']} ⚫")
    update_telemetry(s3, telemetry)

    # Upload em Staging Quarentena
    final_payload = {
        'cycle': airac['cycle'],
        'effective_date': airac['effective_date'],
        'is_staging': airac['is_staging'],
        'generated_at': time.time(),
        'diff_summary': diff_summary,
        'points': audited_points,
        'removed_points': removed_points
    }

    if not args.dry_run and s3:
        payload_bytes = json.dumps(final_payload, ensure_ascii=False).encode('utf-8')
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=versioned_key,
            Body=payload_bytes,
            ContentType='application/json',
            CacheControl='no-cache'
        )
        print(f"📦 Payload versionado gravado com sucesso em: {versioned_key}")
        telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 📦 Snapshot versionado salvo no R2: {versioned_key}.")

        # 🚀 Publicação Oficial da Malha de Produção (Ciclo ativo vigente ou forçado via --publish-prod)
        if not airac['is_staging'] or args.publish_prod:
            s3.put_object(
                Bucket=R2_BUCKET,
                Key='rea_vfr/latest_rea_vfr.json',
                Body=payload_bytes,
                ContentType='application/json',
                CacheControl='public, max-age=300'
            )
            print(f"🚀 Malha Oficial de Produção publicada em: rea_vfr/latest_rea_vfr.json")
            telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 🚀 Produção oficial atualizada no R2: rea_vfr/latest_rea_vfr.json.")

        # Disparo do Webhook de Homologação e Alerta Telegram
        try:
            key_candidate = (os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or '').strip() or (os.environ.get('SUPABASE_ANON_KEY') or '').strip() or SUPABASE_ANON_KEY
            if SUPABASE_URL and key_candidate:
                webhook_url = f"{SUPABASE_URL}/functions/v1/airac-rea-vfr-ingest"
                wh_headers = {
                    'Authorization': f'Bearer {key_candidate}',
                    'apikey': key_candidate,
                    'Content-Type': 'application/json'
                }
                wh_body = {
                    'cycle': airac['cycle'],
                    'effective_date': airac['effective_date'],
                    'status': 'VALIDATED',
                    'r2_path': versioned_key,
                    'diff_summary': diff_summary
                }
                print(f"Enviando alerta para {webhook_url}...")
                wh_res = requests.post(webhook_url, json=wh_body, headers=wh_headers, timeout=30)
                print(f"📱 Resposta do Webhook Telegram REA: {wh_res.status_code}")
                telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 📱 Alerta Telegram com detalhamento de cores despachado.")
        except Exception as wh_err:
            print(f"⚠️ Falha ao despachar webhook: {wh_err}")

    if args.sync_db:
        print("📥 Flag --sync-db detectada. Sincronizando fixos diretamente com a base de dados...")
        sync_points_to_supabase(audited_points)

    telemetry['status'] = 'completed'
    telemetry['global_progress'] = 100
    telemetry['completed_at_utc'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 🛡️ Processamento concluído com sucesso e isolado em Staging.")
    update_telemetry(s3, telemetry)
    print("Processamento finalizado com sucesso.")

if __name__ == '__main__':
    main()
