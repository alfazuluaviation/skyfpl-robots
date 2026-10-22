# ==============================================================================
# SkyFPL ARINC 424 Procedures Robot
# Arquivo: arinc424-robot/sync_arinc424.py
# Descricao: Extrai Coding Tables do DECEA AISWEB, decodifica pernas ARINC 424,
#            alimenta tabelas relacionais do Supabase, compila o pacote consolidado
#            diretamente para Quarentena / Staging no Cloudflare R2 e despacha
#            telemetria em tempo real e notificacoes no Telegram.
# ==============================================================================

import os
import sys
import re
import json
import time
import hashlib
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
import requests

try:
    import boto3
    from botocore.config import Config
except ImportError:
    boto3 = None

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

try:
    import pypdf
except ImportError:
    pypdf = None

# ─── CONFIGURACOES DE AMBIENTE ───────────────────────────────────────────────

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://gongoqjjpwphhttumdjm.supabase.co').rstrip('/')
SUPABASE_KEY = (
    os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
    or os.environ.get('SUPABASE_KEY')
    or os.environ.get('SUPABASE_ANON_KEY')
    or 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdvbmdvcWpqcHdwaGh0dHVtZGptIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MTUyMDcsImV4cCI6MjA4Mjk5MTIwN30.XhdrWma90JeoQdGfeqCoXgGVnyiTZ5GXHszEHw3Ce2o'
)

R2_ENDPOINT = os.environ.get('R2_ENDPOINT') or os.environ.get('CLOUDFLARE_R2_ENDPOINT', '')
R2_ACCESS_KEY = os.environ.get('R2_ACCESS_KEY_ID') or os.environ.get('CLOUDFLARE_R2_ACCESS_KEY_ID', '')
R2_SECRET_KEY = os.environ.get('R2_SECRET_ACCESS_KEY') or os.environ.get('CLOUDFLARE_R2_SECRET_ACCESS_KEY', '')
R2_BUCKET = os.environ.get('R2_BUCKET_NAME') or os.environ.get('CLOUDFLARE_R2_BUCKET', 'skyfpl-charts')
R2_PUBLIC_URL = os.environ.get('R2_PUBLIC_URL', 'https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev').rstrip('/')

AISWEB_API_KEY = os.environ.get('AISWEB_API_KEY') or '1587263166'
AISWEB_API_PASS = os.environ.get('AISWEB_API_PASS') or '3199249e-755b-1033-a49b-72567f175e3a'

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

HEADERS_REST = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
}

# ─── PRESETS HOMOLOGADOS DE AERODROMOS ────────────────────────────────────────

PRESET_AIRPORTS = {
    'TOP4': ['SBSV', 'SBGR', 'SBRJ', 'SBBR'],
    'RJSP': ['SBGR', 'SBSP', 'SBRJ', 'SBGL', 'SBKP'],
    'NORDESTE': ['SBSV', 'SBRF', 'SBFZ', 'SBSG', 'SBNT'],
}

# ─── CLIENTE CLOUDFLARE R2 (S3) ───────────────────────────────────────────────

def get_r2_client():
    if not (boto3 and R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY):
        return None
    endpoint = R2_ENDPOINT
    if not endpoint.startswith('http'):
        endpoint = f'https://{endpoint}'
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
        region_name='auto'
    )

# ─── TELEMETRIA EM TEMPO REAL ─────────────────────────────────────────────────

class TelemetryManager:
    def __init__(self, s3_client, target_icaos: List[str]):
        self.s3 = s3_client
        self.target_icaos = target_icaos
        self.logs: List[str] = []
        self.current_icao: Optional[str] = None
        self.current_phase: str = 'Inicializando...'
        self.percent: int = 0
        self.charts_found: int = 0
        self.tables_detected: int = 0
        self.legs_decoded: int = 0
        self.holdings_detected: int = 0
        self.airports_done: List[str] = []

    def log(self, message: str, icao_idx: int = None, sub_percent: float = None, level: str = 'INFO'):
        now_str = datetime.now().strftime('%H:%M:%S')
        prefix = '✓' if level == 'SUCCESS' else ('⚠' if level == 'WARN' else ('✖' if level == 'ERROR' else '›'))
        formatted = f'[{now_str}] {prefix} {message}'
        print(formatted, flush=True)
        self.logs.append(formatted)
        if len(self.logs) > 100:
            self.logs = self.logs[-100:]

        self.current_phase = message
        if icao_idx is not None and sub_percent is not None:
            n_total = max(1, len(self.target_icaos))
            base = ((icao_idx - 1) / n_total) * 100.0
            contrib = (sub_percent / n_total)
            self.percent = min(99, max(0, int(base + contrib)))

        self.sync_r2(status='processing')

    def sync_r2(self, status: str = 'processing'):
        if not self.s3 or not R2_BUCKET:
            return
        payload = {
            'status': status,
            'engine': 'arinc424_robot_v1',
            'current_icao': self.current_icao,
            'phase': self.current_phase,
            'completed_icaos': self.airports_done,
            'target_icaos': self.target_icaos,
            'percent': 100 if status == 'completed' else self.percent,
            'stats': {
                'airports_processed': len(self.airports_done),
                'total_charts_found': self.charts_found,
                'coding_tables_detected': self.tables_detected,
                'legs_decoded': self.legs_decoded,
                'holdings_detected': self.holdings_detected,
            },
            'logs': self.logs,
            'updated_at': datetime.now(timezone.utc).isoformat()
        }
        try:
            self.s3.put_object(
                Bucket=R2_BUCKET,
                Key='procedures/arinc424/telemetry.json',
                Body=json.dumps(payload, indent=2, ensure_ascii=False).encode('utf-8'),
                ContentType='application/json',
                CacheControl='no-cache, no-store, must-revalidate'
            )
        except Exception:
            pass

# ─── NOTIFICACOES TELEGRAM ────────────────────────────────────────────────────

def send_telegram_notification(
    title: str,
    status: str,  # 'SUCCESS', 'FAILED', 'STAGED'
    airac_cycle: str = 'AUTO',
    airports: List[str] = None,
    total_procedures: int = 0,
    total_legs: int = 0,
    total_holdings: int = 0,
    size_kb: float = 0.0,
    staging_url: str = '',
    error_msg: str = None,
    recent_logs: List[str] = None
) -> bool:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return False

    airports = airports or []
    recent_logs = recent_logs or []

    if status == 'FAILED':
        lines = [
            f'🚨 <b>ALERTA DE FALHA — {title}</b>',
            '━━━━━━━━━━━━━━━━━━━━━━━━━━',
            f'🛰️ <b>Alvo:</b> <code>{", ".join(airports) if airports else "Geral"}</code>',
            f'📅 <b>Ciclo:</b> <code>{airac_cycle}</code>',
        ]
        if error_msg:
            lines.append(f'⚠️ <b>Diagnóstico:</b> {error_msg}')
        if recent_logs:
            lines.append('')
            lines.append('📋 <b>Últimos Logs:</b>')
            for l in recent_logs[-4:]:
                lines.append(f'• <code>{l}</code>')
        lines.append('━━━━━━━━━━━━━━━━━━━━━━━━━━')
        lines.append('❌ <b>Status:</b> FAILED (Intervenção Necessária)')
    else:
        lines = [
            f'🛰️ <b>{title}</b>',
            '━━━━━━━━━━━━━━━━━━━━━━━━━━',
            f'📅 <b>Ciclo AIRAC:</b> <code>{airac_cycle}</code>',
            f'🏢 <b>Aeródromos ({len(airports)}):</b> <code>{", ".join(airports)}</code>',
            f'📊 <b>Procedimentos Codificados:</b> <b>{total_procedures}</b>',
            f'📐 <b>Pernas ARINC 424:</b> <b>{total_legs}</b>',
            f'🔄 <b>Órbitas de Espera:</b> <b>{total_holdings}</b>',
            f'📦 <b>Tamanho do Pacote:</b> <code>{size_kb:.1f} KB</code>',
            '━━━━━━━━━━━━━━━━━━━━━━━━━━',
            '🛡️ <b>Destino:</b> Quarentena / Staging R2 (Pronto para Auditoria)',
            f'🔗 <b>Link Staging:</b> <a href="{staging_url}">procedures_arinc424_staging.json</a>',
            '━━━━━━━━━━━━━━━━━━━━━━━━━━',
            '✅ <b>Status:</b> 100% SUCESSO (Aguardando Aprovação no Dashboard)'
        ]

    msg = '\n'.join(lines)
    try:
        url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
        resp = requests.post(url, json={
            'chat_id': TELEGRAM_CHAT_ID,
            'text': msg,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        }, timeout=15)
        if resp.status_code == 200:
            print('📱 [Telegram] Notificação enviada com sucesso!', flush=True)
            return True
        else:
            print(f'⚠️ [Telegram] HTTP {resp.status_code}: {resp.text}', flush=True)
            return False
    except Exception as e:
        print(f'⚠️ [Telegram] Erro de envio: {e}', flush=True)
        return False

# ─── PARSER ARINC 424 ─────────────────────────────────────────────────────────

def clean_cdata(val: str) -> str:
    if not val:
        return ''
    return re.sub(r'<!\[CDATA\[|\]\]>', '', val).strip()

def convert_dms_to_dd(dms_str: str) -> Optional[float]:
    if not dms_str:
        return None
    m = re.search(r'([NSEW])\s*(\d{1,3}):(\d{2}):([\d\.]+)', dms_str, re.I)
    if not m:
        return None
    dir_char = m.group(1).upper()
    deg = float(m.group(2))
    minute = float(m.group(3))
    sec = float(m.group(4))
    dd = deg + (minute / 60.0) + (sec / 3600.0)
    if dir_char in ('S', 'W'):
        dd = -dd
    return round(dd, 6)

def parse_waypoints_section(full_text: str, proc_ref: str) -> Dict[str, dict]:
    waypoints = {}
    wp_regex = re.compile(r'([A-Z0-9]{3,7})\s+([NS]\s*\d{2}:\d{2}:[\d\.]+)\s*([EW]\s*\d{2,3}:\d{2}:[\d\.]+)', re.I)
    for m in wp_regex.finditer(full_text):
        ident = m.group(1).strip().upper()
        lat_dms = m.group(2).strip()
        lng_dms = m.group(3).strip()
        lat = convert_dms_to_dd(lat_dms)
        lng = convert_dms_to_dd(lng_dms)
        if lat is not None and lng is not None:
            waypoints[ident] = {
                'ident': ident,
                'lat_dd': lat,
                'lng_dd': lng,
                'dms_coord': f'{lat_dms} {lng_dms}',
                'source': f'DECEA CODING TABLE {proc_ref}'
            }
    return waypoints

def parse_leg_line(line: str, waypoints: Dict[str, dict], icao_code: str, proc_name: str) -> Tuple[Optional[dict], Optional[dict]]:
    clean = line.strip()
    if not clean:
        return None, None

    mag_course = None
    true_course = None
    m_mag = re.search(r'([\d\.]+)°\s*Mag', clean, re.I)
    if m_mag:
        mag_course = float(m_mag.group(1))
    m_true = re.search(r'([\d\.]+)°\s*True', clean, re.I)
    if m_true:
        true_course = float(m_true.group(1))

    norm = re.sub(r'[\d\.]+°\s*Mag\s+[\d\.]+°\s*True', 'COURSE_PLACEHOLDER', clean, flags=re.I)
    norm = re.sub(r'(\d+\.\d{2})\s+min', r'\1_min', norm, flags=re.I)
    norm = re.sub(r'RNP\s+APCH', 'RNP_APCH', norm, flags=re.I)
    norm = re.sub(r'RNAV\s+1', 'RNAV_1', norm, flags=re.I)
    norm = re.sub(r'RNAV\s+2', 'RNAV_2', norm, flags=re.I)

    tokens = norm.split()
    if len(tokens) < 8:
        return None, None

    try:
        seq_num = int(tokens[0])
    except ValueError:
        return None, None

    transition_ident = tokens[1].upper()
    fly_over = (tokens[2].upper() == 'Y')
    fix_ident = tokens[4].upper()
    path_terminator = tokens[5].upper()

    turn_direction = None
    if len(tokens) > 7 and tokens[7].upper() in ('L', 'R'):
        turn_direction = tokens[7].upper()

    altitude_constraint_type = None
    lower_limit_altitude = None
    upper_limit_altitude = None

    if len(tokens) > 9:
        m_lower = re.match(r'^([+@\-R=B])?(\d{2,5})$', tokens[9], re.I)
        if m_lower:
            altitude_constraint_type = m_lower.group(1)
            lower_limit_altitude = int(m_lower.group(2))

    if len(tokens) > 8:
        m_upper = re.match(r'^([+@\-R=B])?(\d{2,5})$', tokens[8], re.I)
        if m_upper:
            upper_limit_altitude = int(m_upper.group(2))

    speed_limit_kts = None
    if len(tokens) > 10 and re.match(r'^\d{2,3}$', tokens[10]):
        speed_limit_kts = int(tokens[10])

    distance_nm = None
    leg_time_min = None
    if len(tokens) > 12:
        dst = tokens[12]
        if dst.endswith('_min'):
            try:
                leg_time_min = float(dst.replace('_min', ''))
            except ValueError:
                pass
        elif re.match(r'^\d+\.\d{2}$', dst):
            try:
                distance_nm = float(dst)
            except ValueError:
                pass

    vertical_angle = None
    if len(tokens) > 13 and re.match(r'^-?\d+\.\d{2}$', tokens[13]):
        try:
            vertical_angle = float(tokens[13])
        except ValueError:
            pass

    role_of_fix = None
    if len(tokens) > 14 and tokens[14].upper() != 'N/A':
        role_of_fix = tokens[14].upper()

    nav_spec = 'RNP APCH'
    if len(tokens) > 15:
        nav_spec = tokens[15].replace('_', ' ').upper()

    wp = waypoints.get(fix_ident)

    leg = {
        'seq_num': seq_num,
        'transition_ident': transition_ident,
        'fix_ident': fix_ident,
        'is_fly_over': fly_over,
        'path_terminator': path_terminator,
        'mag_course': mag_course,
        'true_course': true_course,
        'turn_direction': turn_direction,
        'altitude_constraint_type': altitude_constraint_type,
        'lower_limit_altitude_ft': lower_limit_altitude,
        'upper_limit_altitude_ft': upper_limit_altitude,
        'speed_limit_kts': speed_limit_kts,
        'distance_nm': distance_nm,
        'leg_time_min': leg_time_min,
        'vertical_angle': vertical_angle,
        'role_of_fix': role_of_fix,
        'nav_spec': nav_spec,
        'lat_dd': wp['lat_dd'] if wp else None,
        'lng_dd': wp['lng_dd'] if wp else None,
        'raw_row_text': clean
    }

    holding = None
    if path_terminator in ('HM', 'HF', 'HA'):
        holding = {
            'fix_ident': fix_ident,
            'icao_associated': icao_code,
            'inbound_mag_course': mag_course or 0,
            'turn_direction': turn_direction or 'R',
            'leg_time_min': leg_time_min or 1.0,
            'leg_distance_nm': distance_nm,
            'min_altitude_ft': lower_limit_altitude or 2000,
            'max_altitude_ft': upper_limit_altitude,
            'speed_limit_kts': speed_limit_kts or 230,
            'source_reference': f'{proc_name} / Coding Table Leg {seq_num}',
            'is_procedural': True
        }

    return leg, holding

def extract_text_from_pdf_stream(pdf_bytes: bytes) -> str:
    if fitz:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype='pdf')
            full = []
            for page in doc:
                full.append(page.get_text('text'))
            return '\n'.join(full)
        except Exception:
            pass

    if pypdf:
        try:
            import io
            reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
            full = []
            for page in reader.pages:
                full.append(page.extract_text() or '')
            return '\n'.join(full)
        except Exception:
            pass

    return ''

# ─── MOTOR DE COMPILACAO STAGING PARA O APP MOVEL ────────────────────────────

def compile_arinc_staging_package(s3, airac_cycle: str, telemetry: TelemetryManager, in_memory_procedures: List[dict] = None) -> Tuple[dict, str]:
    telemetry.log('📦 Iniciando compilação do pacote consolidado ARINC 424...', level='INFO')

    # 1. Carrega todas as pernas, cabeçalhos e órbitas do Supabase
    headers_url = f'{SUPABASE_URL}/rest/v1/procedure_headers?select=*&order=icao_code.asc,procedure_name.asc'
    r_h = requests.get(headers_url, headers=HEADERS_REST, timeout=30)
    headers = r_h.json() if r_h.status_code == 200 else []

    legs_url = f'{SUPABASE_URL}/rest/v1/procedure_legs?select=*&order=seq_num.asc'
    r_l = requests.get(legs_url, headers=HEADERS_REST, timeout=45)
    legs = r_l.json() if r_l.status_code == 200 else []

    holdings_url = f'{SUPABASE_URL}/rest/v1/holding_patterns?select=*'
    r_hold = requests.get(holdings_url, headers=HEADERS_REST, timeout=30)
    holdings = r_hold.json() if r_hold.status_code == 200 else []

    fixes_url = f'{SUPABASE_URL}/rest/v1/enroute_fixes?select=ident,lat_dd,lng_dd'
    r_f = requests.get(fixes_url, headers=HEADERS_REST, timeout=30)
    fixes_list = r_f.json() if r_f.status_code == 200 else []
    fixes_map = {f['ident']: {'lat': f.get('lat_dd'), 'lng': f.get('lng_dd')} for f in fixes_list if f.get('ident')}

    # Indexar pernas por procedure_id
    legs_by_proc: Dict[str, list] = {}
    for leg in legs:
        pid = leg.get('procedure_id')
        if pid:
            legs_by_proc.setdefault(pid, []).append(leg)

    # Indexar holdings por procedure_name ou fix
    holdings_by_proc: Dict[str, list] = {}
    for h in holdings:
        ref = h.get('source_reference') or ''
        proc_key = ref.split(' / ')[0].strip().upper() if ' / ' in ref else ref.strip().upper()
        if proc_key:
            holdings_by_proc.setdefault(proc_key, []).append(h)

    airports_map: Dict[str, dict] = {}
    total_legs_count = 0
    total_holdings_count = 0
    total_procs_count = 0
    if in_memory_procedures:
        for p in in_memory_procedures:
            p_icao = p.get('icao', '').upper()
            p_name = p.get('name', '')
            p_type = p.get('type', 'IAC')
            p_legs = p.get('legs', [])
            p_holdings = p.get('holdings', [])
            p_waypoints = p.get('waypoints', {})

            if not p_icao or not p_legs:
                continue

            if p_icao not in airports_map:
                airports_map[p_icao] = {
                    'icao': p_icao,
                    'procedures': {'IAC': {}, 'SID': {}, 'STAR': {}}
                }

            transitions_p: Dict[str, list] = {}
            for l in p_legs:
                t_ident = l.get('transition_ident') or 'COMMON'
                fix_id = l.get('fix_ident')
                wp_c = p_waypoints.get(fix_id, {})
                lat_c = l.get('lat_dd') if l.get('lat_dd') is not None else wp_c.get('lat_dd')
                lng_c = l.get('lng_dd') if l.get('lng_dd') is not None else wp_c.get('lng_dd')

                transitions_p.setdefault(t_ident, []).append({
                    'seq': l.get('seq_num'),
                    'trans': t_ident,
                    'fix': fix_id,
                    'flyOver': bool(l.get('fly_over')),
                    'path': l.get('path_terminator'),
                    'magCourse': l.get('mag_course'),
                    'trueCourse': l.get('true_course'),
                    'turn': l.get('turn_direction'),
                    'altConstraint': l.get('altitude_constraint_type'),
                    'minAlt': l.get('lower_limit_altitude_ft'),
                    'maxAlt': l.get('upper_limit_altitude_ft'),
                    'speedLimit': l.get('speed_limit_kts'),
                    'distance': l.get('distance_nm'),
                    'vertAngle': l.get('vertical_angle'),
                    'role': l.get('role_of_fix'),
                    'navSpec': l.get('nav_spec'),
                    'lat': lat_c,
                    'lng': lng_c
                })

            holdings_p = []
            for h in p_holdings:
                holdings_p.append({
                    'fix': h.get('fix_ident'),
                    'inboundCourse': h.get('inbound_mag_course'),
                    'turn': h.get('turn_direction'),
                    'minAlt': h.get('min_altitude_ft'),
                    'maxAlt': h.get('max_altitude_ft'),
                    'speedLimit': h.get('speed_limit_kts'),
                    'legDist': h.get('leg_distance_nm'),
                    'legTime': h.get('leg_time_min')
                })

            all_legs_p = []
            for trans_arr in transitions_p.values():
                all_legs_p.extend(trans_arr)
            all_legs_p.sort(key=lambda x: (x.get('trans') or '', x.get('seq') or 0))

            if p_type not in airports_map[p_icao]['procedures']:
                airports_map[p_icao]['procedures'][p_type] = {}

            airports_map[p_icao]['procedures'][p_type][p_name] = {
                'name': p_name,
                'type': p_type,
                'chartUrl': p.get('chart_url'),
                'codingTableUrl': p.get('coding_table_url'),
                'transitions': transitions_p,
                'holdings': holdings_p,
                'allLegs': all_legs_p
            }

            total_procs_count += 1
            total_legs_count += len(p_legs)
            total_holdings_count += len(holdings_p)


    for h in headers:
        icao = (h.get('icao_code') or '').upper()
        pid = h.get('id')
        p_name = h.get('procedure_name') or ''
        p_type = h.get('procedure_type') or 'IAC'

        if not (icao and pid):
            continue

        p_legs = legs_by_proc.get(pid, [])
        if not p_legs:
            continue

        # Agrupar pernas por transição
        transitions: Dict[str, list] = {}
        for l in p_legs:
            t_ident = l.get('transition_ident') or 'COMMON'
            fix_id = l.get('fix_ident')
            coords = fixes_map.get(fix_id, {})
            lat = l.get('lat_dd') or coords.get('lat')
            lng = l.get('lng_dd') or coords.get('lng')

            compiled_leg = {
                'seq': l.get('seq_num'),
                'trans': t_ident,
                'fix': fix_id,
                'flyOver': bool(l.get('is_fly_over')),
                'path': l.get('path_terminator'),
                'magCourse': l.get('mag_course'),
                'trueCourse': l.get('true_course'),
                'turn': l.get('turn_direction'),
                'altConstraint': l.get('altitude_constraint_type'),
                'altLowerFt': l.get('lower_limit_altitude_ft'),
                'altUpperFt': l.get('upper_limit_altitude_ft'),
                'speedKts': l.get('speed_limit_kts'),
                'dstNm': l.get('distance_nm'),
                'vpa': l.get('vertical_angle'),
                'role': l.get('role_of_fix'),
                'navSpec': l.get('nav_spec'),
                'lat': lat,
                'lng': lng
            }
            transitions.setdefault(t_ident, []).append(compiled_leg)
            total_legs_count += 1

        # Extrair pista da carta
        rwy = ''
        m_rwy = re.search(r'RWY\s*(\d{2}[LRC]?)', p_name, re.I)
        if m_rwy:
            rwy = m_rwy.group(1).upper()

        proc_holdings = holdings_by_proc.get(p_name.strip().upper(), [])
        compiled_holdings = []
        for hold in proc_holdings:
            compiled_holdings.append({
                'fix': hold.get('fix_ident'),
                'inboundMag': hold.get('inbound_mag_course', 0),
                'turn': hold.get('turn_direction', 'R'),
                'legTimeMin': hold.get('leg_time_min', 1.0),
                'minAltFt': hold.get('min_altitude_ft', 2000),
                'speedKts': hold.get('speed_limit_kts', 230),
                'isProcedural': True
            })
            total_holdings_count += 1

        compiled_proc = {
            'id': pid,
            'name': p_name,
            'type': p_type,
            'rwy': rwy,
            'amdt': h.get('amdt_number'),
            'effectiveDate': h.get('effective_date'),
            'icp': h.get('icp_code'),
            'chartUrl': h.get('chart_pdf_url'),
            'codingTableUrl': h.get('coding_table_pdf_url'),
            'transitions': transitions,
            'holdings': compiled_holdings
        }

        if icao not in airports_map:
            airports_map[icao] = {'icao': icao, 'procedures': []}
        airports_map[icao]['procedures'].append(compiled_proc)
        total_procs_count += 1

    package_json = {
        'format': 'ARINC_424_SKYFPL_OFFLINE',
        'version': '1.0.0',
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'metadata': {
            'airacCycle': airac_cycle,
            'effectiveDate': 'AUTO',
            'totalAirports': len(airports_map),
            'totalProcedures': total_procs_count,
            'totalLegs': total_legs_count,
            'totalHoldings': total_holdings_count,
            'sourceTables': ['procedure_headers', 'procedure_legs', 'holding_patterns', 'enroute_fixes']
        },
        'airports': airports_map
    }

    raw_bytes = json.dumps(package_json, ensure_ascii=False, indent=2).encode('utf-8')
    size_bytes = len(raw_bytes)
    sha256_hash = hashlib.sha256(raw_bytes).hexdigest()
    package_json['metadata']['fileSizeBytes'] = size_bytes
    package_json['metadata']['sha256'] = sha256_hash

    # Upload para Staging R2
    staging_key = 'procedures/staging/procedures_arinc424_staging.json'
    versioned_key = f'procedures/cycles/{airac_cycle}/procedures_{airac_cycle}_staging.json'
    staging_url = f'{R2_PUBLIC_URL}/{staging_key}'

    if s3 and R2_BUCKET:
        telemetry.log(f'🚀 Enviando pacote para Quarentena / Staging R2 ({staging_key})...', level='INFO')
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=staging_key,
            Body=raw_bytes,
            ContentType='application/json',
            CacheControl='no-cache, no-store, must-revalidate'
        )
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=versioned_key,
            Body=raw_bytes,
            ContentType='application/json',
            CacheControl='no-cache, no-store, must-revalidate'
        )

        # Atualizar procedures_manifest.json com next_standby
        manifest_key = 'procedures/procedures_manifest.json'
        manifest_data = {'updated_at': datetime.now(timezone.utc).isoformat(), 'current_active': None, 'next_standby': None}
        try:
            resp = s3.get_object(Bucket=R2_BUCKET, Key=manifest_key)
            manifest_data = json.loads(resp['Body'].read().decode('utf-8'))
        except Exception:
            pass

        manifest_data['updated_at'] = datetime.now(timezone.utc).isoformat()
        manifest_data['next_standby'] = {
            'cycle': airac_cycle,
            'effective_date': 'VIGENTE',
            'staging_url': staging_url,
            'total_airports': len(airports_map),
            'total_procedures': total_procs_count,
            'total_legs': total_legs_count,
            'total_holdings': total_holdings_count,
            'sha256': sha256_hash,
            'size_bytes': size_bytes,
            'staged_at': datetime.now(timezone.utc).isoformat()
        }

        s3.put_object(
            Bucket=R2_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest_data, indent=2, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json',
            CacheControl='no-cache, no-store, must-revalidate'
        )
        telemetry.log('✅ Pacote e Manifesto de Staging atualizados com sucesso no R2!', level='SUCCESS')

    return package_json, staging_url

# ─── EXECUCAO PRINCIPAL ───────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='SkyFPL ARINC 424 Procedures Robot')
    parser.add_argument('--icao', type=str, default='SBSV,SBGR,SBRJ,SBBR', help='Códigos ICAO separados por vírgula ou preset (TOP4, RJSP, NORDESTE, ALL)')
    parser.add_argument('--airac', type=str, default='AUTO', help='Ciclo AIRAC (Ex: 2610 ou AUTO)')
    parser.add_argument('--dry-run', type=str, default='false', help='Modo simulação')
    args = parser.parse_args()

    s3 = get_r2_client()
    if s3:
        print(f'☁️ Cloudflare R2 Conectado: Bucket {R2_BUCKET}.', flush=True)

    raw_input = args.icao.strip().upper()
    if raw_input in ('ALL', 'TODOS'):
        print('🌍 Modo Malha Nacional Ativado: Baixando lista completa de aeródromos...', flush=True)
        target_icaos = []
        try:
            r_nav = requests.get('https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/latest_navdata.json', timeout=30)
            if r_nav.status_code == 200:
                nav_data = r_nav.json().get('data', [])
                target_icaos = sorted({p['icao'] for p in nav_data if p.get('icao') and len(p.get('icao')) == 4 and p['icao'].startswith(('SB', 'SW', 'SS', 'SN', 'SD', 'SI', 'SJ'))})
        except Exception as e:
            print(f'⚠️ Falha ao baixar latest_navdata.json: {e}', flush=True)

        if not target_icaos:
            try:
                r_sb = requests.get(f'{SUPABASE_URL}/rest/v1/procedure_headers?select=icao_code', headers=HEADERS_REST, timeout=20)
                if r_sb.status_code == 200:
                    target_icaos = sorted({x['icao_code'] for x in r_sb.json() if x.get('icao_code')})
            except Exception:
                pass

        if not target_icaos:
            target_icaos = ['SBSV', 'SBGR', 'SBRJ', 'SBBR']
    else:
        target_icaos = [x.strip() for x in raw_input.split(',') if len(x.strip()) == 4]

    if not target_icaos:
        target_icaos = ['SBSV']

    telemetry = TelemetryManager(s3, target_icaos)
    in_memory_procs: List[dict] = []
    telemetry.log(f'🚀 Robô ARINC 424 iniciado para {len(target_icaos)} aeródromo(s): {", ".join(target_icaos)}', 1, 0)

    total_procs_ingested = 0
    total_legs_ingested = 0
    total_holdings_ingested = 0

    for idx, icao in enumerate(target_icaos, start=1):
        telemetry.current_icao = icao
        telemetry.log(f'🌐 [{idx}/{len(target_icaos)}] Consultando DECEA AISWEB para {icao}...', idx, 10)

        # 1. Consulta AISWEB API de Cartas
        url = f'https://aisweb.decea.mil.br/api/?apiKey={AISWEB_API_KEY}&apiPass={AISWEB_API_PASS}&area=cartas&icaoCode={icao}'
        try:
            r = requests.get(url, headers={'User-Agent': 'SkyFPL/ARINC424-Robot'}, timeout=30)
            if r.status_code != 200:
                telemetry.log(f'❌ [{icao}] Erro HTTP {r.status_code} na API AISWEB', idx, 90, level='ERROR')
                continue

            xml = r.text
            if 'Informe uma chave' in xml or '<erro>' in xml:
                telemetry.log(f'❌ [{icao}] Erro de autenticação AISWEB: chave inválida', idx, 90, level='ERROR')
                continue

            items = re.findall(r'<item.*?>.*?</item>', xml, re.DOTALL | re.IGNORECASE)
            telemetry.charts_found += len(items)

            # Filtrar cartas IFR que possuam <tabcode>
            for it in items:
                m_type = re.search(r'<tipo>(.*?)</tipo>', it, re.DOTALL | re.IGNORECASE)
                t_val = clean_cdata(m_type.group(1)) if m_type else ''
                if t_val not in ('IAC', 'STAR', 'SID'):
                    continue

                m_name = re.search(r'<nome>(.*?)</nome>', it, re.DOTALL | re.IGNORECASE)
                name_val = clean_cdata(m_name.group(1)) if m_name else ''

                m_tab = re.search(r'<tabcode>(.*?)</tabcode>', it, re.DOTALL | re.IGNORECASE)
                tab_link = clean_cdata(m_tab.group(1)) if m_tab else ''

                m_chart = re.search(r'<link>(.*?)</link>', it, re.DOTALL | re.IGNORECASE)
                chart_link = clean_cdata(m_chart.group(1)) if m_chart else ''

                has_table = bool(tab_link and tab_link.startswith('http'))
                if not has_table:
                    # O módulo ARINC 424 processa estritamente procedimentos com Coding Table oficial
                    continue

                telemetry.tables_detected += 1

                # Upsert em procedure_headers com ID deterministico
                p_slug = re.sub(r'[^A-Z0-9]', '_', name_val.upper())
                p_slug = re.sub(r'_+', '_', p_slug).strip('_')
                calc_id = f'{icao}_{t_val}_{p_slug}'

                header_payload = {
                    'id': calc_id,
                    'icao_code': icao,
                    'procedure_name': name_val,
                    'procedure_type': t_val,
                    'has_coding_table': has_table,
                    'coding_table_pdf_url': tab_link if has_table else None,
                    'chart_pdf_url': chart_link or None,
                    'parsing_status': 'PENDING' if has_table else 'NO_CODING_TABLE',
                    'updated_at': datetime.now(timezone.utc).isoformat()
                }

                h_resp = requests.post(
                    f'{SUPABASE_URL}/rest/v1/procedure_headers?on_conflict=id',
                    headers={**HEADERS_REST, 'Prefer': 'return=representation,resolution=merge-duplicates'},
                    json=header_payload,
                    timeout=15
                )

                proc_id = calc_id

                # Se tem tabela de codificação, baixa e decodifica pernas
                if has_table and proc_id:
                    try:
                        pdf_r = requests.get(tab_link, headers={'User-Agent': 'SkyFPL/ARINC424-Robot'}, timeout=30)
                        if pdf_r.status_code == 200:
                            raw_text = extract_text_from_pdf_stream(pdf_r.content)
                            if raw_text:
                                waypoints = parse_waypoints_section(raw_text, name_val)

                                # Salvar fixos em enroute_fixes
                                if waypoints:
                                    fixes_to_upsert = list(waypoints.values())
                                    requests.post(
                                        f'{SUPABASE_URL}/rest/v1/enroute_fixes?on_conflict=ident',
                                        headers=HEADERS_REST,
                                        json=fixes_to_upsert,
                                        timeout=15
                                    )

                                # Deletar pernas antigas antes de inserir
                                requests.delete(
                                    f'{SUPABASE_URL}/rest/v1/procedure_legs?procedure_id=eq.{proc_id}',
                                    headers=HEADERS_REST,
                                    timeout=15
                                )

                                legs_batch = []
                                holdings_batch = []

                                # Estruturacao via tabelas PyMuPDF (fitz)
                                if fitz:
                                    try:
                                        doc_tab = fitz.open(stream=pdf_r.content, filetype='pdf')
                                        # 1. Busca waypoints em tabelas
                                        for p_tab in doc_tab:
                                            tf = p_tab.find_tables()
                                            for t_item in getattr(tf, 'tables', []):
                                                for r_row in t_item.extract():
                                                    c_row = [c.replace('\n', ' ').strip() if c else '' for c in r_row]
                                                    if len(c_row) >= 2:
                                                        w_id = c_row[0].strip().upper()
                                                        w_co = c_row[1].strip()
                                                        if re.match(r'^[A-Z0-9]{3,7}$', w_id) and ('S ' in w_co or 'N ' in w_co):
                                                            lat_m = re.search(r'([NS]\s*\d{2}:\d{2}:[\d\.]+)', w_co, re.I)
                                                            lng_m = re.search(r'([EW]\s*\d{2,3}:\d{2}:[\d\.]+)', w_co, re.I)
                                                            if lat_m and lng_m:
                                                                lat_v = convert_dms_to_dd(lat_m.group(1))
                                                                lng_v = convert_dms_to_dd(lng_m.group(1))
                                                                if lat_v is not None and lng_v is not None:
                                                                    waypoints[w_id] = {
                                                                        'ident': w_id,
                                                                        'lat_dd': lat_v,
                                                                        'lng_dd': lng_v,
                                                                        'dms_coord': f"{lat_m.group(1)} {lng_m.group(1)}",
                                                                        'source': f'DECEA CODING TABLE {name_val}'
                                                                    }
                                        # 2. Busca pernas da Coding Table
                                        for p_tab in doc_tab:
                                            tf = p_tab.find_tables()
                                            for t_item in getattr(tf, 'tables', []):
                                                for r_row in t_item.extract():
                                                    c_row = [c.replace('\n', ' ').strip() if c else '' for c in r_row]
                                                    if len(c_row) >= 6:
                                                        s_num = c_row[0].strip()
                                                        p_term = c_row[5].strip().upper()
                                                        if s_num.isdigit() and p_term in ('IF', 'TF', 'CF', 'DF', 'FA', 'FC', 'FD', 'FM', 'HA', 'HF', 'HM', 'PI', 'RF', 'VA', 'VD', 'VI', 'VM', 'VR'):
                                                            seq_val = int(s_num)
                                                            tr_id = c_row[1].strip() or 'ALL'
                                                            fl_ov = c_row[2].strip().upper() == 'Y'
                                                            fx_id = c_row[4].strip()
                                                            crs_str = c_row[6].strip() if len(c_row) > 6 else ''
                                                            trn_str = c_row[7].strip().upper() if len(c_row) > 7 else ''
                                                            u_alt_str = c_row[8].strip() if len(c_row) > 8 else ''
                                                            l_alt_str = c_row[9].strip() if len(c_row) > 9 else ''
                                                            spd_str = c_row[10].strip() if len(c_row) > 10 else ''
                                                            dst_str = c_row[12].strip() if len(c_row) > 12 else ''
                                                            va_str = c_row[13].strip() if len(c_row) > 13 else ''
                                                            rol_str = c_row[14].strip() if len(c_row) > 14 else ''
                                                            nav_str = c_row[15].strip() if len(c_row) > 15 else ''

                                                            mg_crs = None
                                                            m_mg = re.search(r'([\d\.]+)\s*°?\s*Mag', crs_str, re.I)
                                                            if m_mg: mg_crs = float(m_mg.group(1))

                                                            tr_crs = None
                                                            m_tr = re.search(r'([\d\.]+)\s*°?\s*True', crs_str, re.I)
                                                            if m_tr: tr_crs = float(m_tr.group(1))

                                                            lw_alt = None
                                                            m_lw = re.search(r'(\d{3,5})', l_alt_str)
                                                            if m_lw: lw_alt = int(m_lw.group(1))

                                                            up_alt = None
                                                            m_up = re.search(r'(?:FL\s*(\d{2,3})|(\d{3,5}))', u_alt_str, re.I)
                                                            if m_up:
                                                                if m_up.group(1): up_alt = int(m_up.group(1)) * 100
                                                                elif m_up.group(2): up_alt = int(m_up.group(2))

                                                            spd_lim = None
                                                            m_sp = re.search(r'(\d{2,3})', spd_str)
                                                            if m_sp: spd_lim = int(m_sp.group(1))

                                                            dst_nm = None
                                                            if dst_str and dst_str != 'N/A':
                                                                try: dst_nm = float(dst_str)
                                                                except: pass

                                                            v_ang = None
                                                            if va_str and va_str != 'N/A':
                                                                try: v_ang = float(va_str)
                                                                except: pass

                                                            wp_obj = waypoints.get(fx_id)
                                                            leg_entry = {
                                                                'procedure_id': proc_id,
                                                                'seq_num': seq_val,
                                                                'transition_ident': tr_id,
                                                                'fix_ident': fx_id,
                                                                'path_terminator': p_term,
                                                                'is_fly_over': fl_ov,
                                                                'mag_course': mg_crs,
                                                                'true_course': tr_crs,
                                                                'turn_direction': trn_str if trn_str in ('R', 'L') else None,
                                                                'altitude_constraint_type': None,
                                                                'lower_limit_altitude_ft': lw_alt,
                                                                'upper_limit_altitude_ft': up_alt,
                                                                'speed_limit_kts': spd_lim,
                                                                'distance_nm': dst_nm,
                                                                'leg_time_min': None,
                                                                'vertical_angle': v_ang,
                                                                'role_of_fix': rol_str if rol_str and rol_str != 'N/A' else None,
                                                                'nav_spec': nav_str,
                                                                'lat_dd': wp_obj['lat_dd'] if wp_obj else None,
                                                                'lng_dd': wp_obj['lng_dd'] if wp_obj else None,
                                                                'raw_row_text': ' | '.join(c_row)
                                                            }
                                                            legs_batch.append(leg_entry)

                                                            if p_term in ('HM', 'HF', 'HA'):
                                                                holdings_batch.append({
                                                                    'fix_ident': fx_id,
                                                                    'icao_associated': icao,
                                                                    'inbound_mag_course': mg_crs or 0,
                                                                    'turn_direction': trn_str if trn_str in ('R', 'L') else 'R',
                                                                    'leg_time_min': 1.0,
                                                                    'leg_distance_nm': dst_nm,
                                                                    'min_altitude_ft': lw_alt or 2000,
                                                                    'max_altitude_ft': up_alt,
                                                                    'speed_limit_kts': spd_lim or 230,
                                                                    'source_reference': f'{name_val} / Coding Table Leg {seq_val}',
                                                                    'is_procedural': True
                                                                })
                                    except Exception as ex_t:
                                        print(f'  [Aviso] Falha na extracao por tabelas PyMuPDF: {ex_t}', flush=True)

                                # Fallback para regex linha a linha se a tabela nao tiver retornado pernas
                                if not legs_batch:
                                    lines_f = [l.strip() for l in raw_text.split('\n') if l.strip()]
                                    for line in lines_f:
                                        leg, hold = parse_leg_line(line, waypoints, icao, name_val)
                                        if leg:
                                            leg['procedure_id'] = proc_id
                                            legs_batch.append(leg)
                                        if hold:
                                            holdings_batch.append(hold)

                                if legs_batch:
                                    db_legs = []
                                    for leg in legs_batch:
                                        db_legs.append({
                                            'procedure_id': leg.get('procedure_id'),
                                            'seq_num': leg.get('seq_num'),
                                            'transition_ident': leg.get('transition_ident'),
                                            'fix_ident': leg.get('fix_ident'),
                                            'is_fly_over': bool(leg.get('is_fly_over', False)),
                                            'path_terminator': leg.get('path_terminator'),
                                            'mag_course': leg.get('mag_course'),
                                            'true_course': leg.get('true_course'),
                                            'turn_direction': leg.get('turn_direction'),
                                            'upper_limit_altitude_ft': leg.get('upper_limit_altitude_ft'),
                                            'lower_limit_altitude_ft': leg.get('lower_limit_altitude_ft'),
                                            'altitude_constraint_type': leg.get('altitude_constraint_type'),
                                            'speed_limit_kts': leg.get('speed_limit_kts'),
                                            'distance_nm': leg.get('distance_nm'),
                                            'vertical_angle': leg.get('vertical_angle'),
                                            'role_of_fix': leg.get('role_of_fix'),
                                            'nav_spec': leg.get('nav_spec') or 'RNP APCH',
                                            'raw_row_text': leg.get('raw_row_text')
                                        })
                                    r_legs = requests.post(
                                        f'{SUPABASE_URL}/rest/v1/procedure_legs',
                                        headers=HEADERS_REST,
                                        json=db_legs,
                                        timeout=20
                                    )
                                    if r_legs.status_code not in (200, 201):
                                        print(f'  [Aviso DB] Erro ao gravar pernas de {name_val}: {r_legs.status_code} - {r_legs.text}', flush=True)
                                    telemetry.legs_decoded += len(legs_batch)
                                    total_legs_ingested += len(legs_batch)

                                if holdings_batch:
                                    requests.post(
                                        f'{SUPABASE_URL}/rest/v1/holding_patterns',
                                        headers=HEADERS_REST,
                                        json=holdings_batch,
                                        timeout=15
                                    )
                                    telemetry.holdings_detected += len(holdings_batch)
                                    total_holdings_ingested += len(holdings_batch)

                                # Atualiza status para PARSED
                                requests.patch(
                                    f'{SUPABASE_URL}/rest/v1/procedure_headers?id=eq.{proc_id}',
                                    headers=HEADERS_REST,
                                    json={'parsing_status': 'PARSED', 'updated_at': datetime.now(timezone.utc).isoformat()},
                                    timeout=15
                                )
                                total_procs_ingested += 1
                                in_memory_procs.append({
                                    'icao': icao,
                                    'id': proc_id or f"{icao}_{name_val}",
                                    'name': name_val,
                                    'type': t_val,
                                    'chart_url': chart_link,
                                    'coding_table_url': tab_link,
                                    'legs': legs_batch,
                                    'holdings': holdings_batch,
                                    'waypoints': waypoints
                                })

                    except Exception as err_p:
                        print(f'  [Aviso] Falha ao decodificar {name_val}: {err_p}', flush=True)

            telemetry.airports_done.append(icao)
            telemetry.log(f'📊 [{icao}] Concluído com sucesso!', idx, 100, level='SUCCESS')

        except Exception as e_icao:
            telemetry.log(f'⚠️ [{icao}] Falha de comunicação: {e_icao}', idx, 90, level='ERROR')

    # ── FASE FINAL: COMPILACAO AUTOMATICA E ENTREGA NO STAGING DO R2 ───────────
    try:
        pkg, staging_url = compile_arinc_staging_package(s3, args.airac, telemetry, in_memory_procedures=in_memory_procs)
        size_kb = pkg['metadata']['fileSizeBytes'] / 1024.0

        telemetry.log(
            f'🏁 Esteira Concluída! {len(target_icaos)} aeródromos, {pkg["metadata"]["totalProcedures"]} procedimentos, {pkg["metadata"]["totalLegs"]} pernas ({size_kb:.1f} KB em Staging).',
            len(target_icaos), 100, level='SUCCESS'
        )
        telemetry.sync_r2(status='completed')

        # Dispara notificação Telegram de Sucesso
        send_telegram_notification(
            title='Robô ARINC 424 Procedures',
            status='STAGED',
            airac_cycle=args.airac,
            airports=target_icaos,
            total_procedures=pkg['metadata']['totalProcedures'],
            total_legs=pkg['metadata']['totalLegs'],
            total_holdings=pkg['metadata']['totalHoldings'],
            size_kb=size_kb,
            staging_url=staging_url,
            recent_logs=telemetry.logs
        )

    except Exception as e_compile:
        err_str = f'Erro na compilação do pacote Staging: {e_compile}'
        telemetry.log(err_str, len(target_icaos), 100, level='ERROR')
        telemetry.sync_r2(status='failed')
        send_telegram_notification(
            title='Robô ARINC 424 Procedures',
            status='FAILED',
            airac_cycle=args.airac,
            airports=target_icaos,
            error_msg=err_str,
            recent_logs=telemetry.logs
        )

if __name__ == '__main__':
    main()
