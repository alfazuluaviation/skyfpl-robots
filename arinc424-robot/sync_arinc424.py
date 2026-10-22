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
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or os.environ.get('SUPABASE_KEY') or os.environ.get('SUPABASE_ANON_KEY', '')

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

def compile_arinc_staging_package(s3, airac_cycle: str, telemetry: TelemetryManager) -> Tuple[dict, str]:
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
    if raw_input in PRESET_AIRPORTS:
        target_icaos = PRESET_AIRPORTS[raw_input]
    else:
        target_icaos = [x.strip() for x in raw_input.split(',') if len(x.strip()) == 4]

    if not target_icaos:
        target_icaos = ['SBSV']

    telemetry = TelemetryManager(s3, target_icaos)
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
                if has_table:
                    telemetry.tables_detected += 1

                # Upsert em procedure_headers
                header_payload = {
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
                    f'{SUPABASE_URL}/rest/v1/procedure_headers?on_conflict=icao_code,procedure_name',
                    headers={**HEADERS_REST, 'Prefer': 'return=representation,resolution=merge-duplicates'},
                    json=header_payload,
                    timeout=15
                )

                proc_id = None
                if h_resp.status_code in (200, 201):
                    res_json = h_resp.json()
                    if isinstance(res_json, list) and len(res_json) > 0:
                        proc_id = res_json[0].get('id')

                if not proc_id:
                    try:
                        q_resp = requests.get(
                            f'{SUPABASE_URL}/rest/v1/procedure_headers?icao_code=eq.{icao}&procedure_name=eq.{requests.utils.quote(name_val)}&select=id',
                            headers=HEADERS_REST,
                            timeout=10
                        )
                        if q_resp.status_code == 200 and q_resp.json():
                            proc_id = q_resp.json()[0].get('id')
                    except Exception:
                        pass

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

                                lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
                                legs_batch = []
                                holdings_batch = []

                                for line in lines:
                                    leg, hold = parse_leg_line(line, waypoints, icao, name_val)
                                    if leg:
                                        leg['procedure_id'] = proc_id
                                        legs_batch.append(leg)
                                    if hold:
                                        holdings_batch.append(hold)

                                if legs_batch:
                                    requests.post(
                                        f'{SUPABASE_URL}/rest/v1/procedure_legs',
                                        headers=HEADERS_REST,
                                        json=legs_batch,
                                        timeout=20
                                    )
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

                    except Exception as err_p:
                        print(f'  [Aviso] Falha ao decodificar {name_val}: {err_p}', flush=True)

            telemetry.airports_done.append(icao)
            telemetry.log(f'📊 [{icao}] Concluído com sucesso!', idx, 100, level='SUCCESS')

        except Exception as e_icao:
            telemetry.log(f'⚠️ [{icao}] Falha de comunicação: {e_icao}', idx, 90, level='ERROR')

    # ── FASE FINAL: COMPILACAO AUTOMATICA E ENTREGA NO STAGING DO R2 ───────────
    try:
        pkg, staging_url = compile_arinc_staging_package(s3, args.airac, telemetry)
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
