"""
MÓDULO DE INTEGRAÇÃO FPDAM / AIXM 5.1 - SKYFPL NAVDATA ENGINE
=============================================================
Responsável por decodificar procedimentos IFR nativos do DECEA (FPDAM) em formato AIXM 5.1,
resolvendo:
- Designadores de procedimentos (IAC, STAR, SID)
- Aeródromos ICAO
- Sequenciamento ARINC 424 (legs: IF, TF, DF, CF, RF, HM, VA)
- Restrições de altitude, velocidade e ângulo de descida
- Fixos com coordenadas geodésicas WGS-84 oficiais de alta precisão
"""

import zipfile
import re
import json
import os
import time

class AixmFpdamEngine:
    def __init__(self, zip_path=None, cache_path=None):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.zip_path = zip_path or os.path.join(base_dir, "..", "..", "skynav-pro-official", "admin", "aixm-lab", "aixm_snapshot.zip")
        self.cache_path = cache_path or os.path.join(base_dir, "cache", "fpdam_cache.json")
        self.procedures = {}
        self.airports = {}
        self.waypoints = {}

    def is_cache_valid(self):
        if not os.path.exists(self.cache_path):
            return False
        if not os.path.exists(self.zip_path):
            return True # usa cache se zip não existir
        return os.path.getmtime(self.cache_path) >= os.path.getmtime(self.zip_path)

    def load(self, force_refresh=False):
        """Carrega do cache rápido JSON ou compila diretamente do snapshot AIXM."""
        if not force_refresh and self.is_cache_valid():
            try:
                t0 = time.time()
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.procedures = data.get('procedures', {})
                    self.airports = data.get('airports', {})
                    self.waypoints = data.get('waypoints', {})
                print(f"⚡ [FPDAM] Cache carregado em {time.time()-t0:.2f}s: {len(self.procedures)} procedimentos digitais.")
                return True
            except Exception as e:
                print(f"⚠️ [FPDAM] Falha ao ler cache ({e}). Recarregando do snapshot...")

        return self.compile_from_snapshot()

    def compile_from_snapshot(self):
        """Lê o snapshot AIXM 5.1 e constrói o banco de procedimentos estruturados."""
        if not os.path.exists(self.zip_path):
            print(f"❌ [FPDAM] Arquivo de snapshot não encontrado: {self.zip_path}")
            return False

        t0 = time.time()
        print(f"📦 [FPDAM] Compilando banco digital a partir de {self.zip_path}...")

        with zipfile.ZipFile(self.zip_path, 'r') as z:
            with z.open('BL_.xml') as f:
                xml = f.read().decode('utf-8', errors='ignore')

        # 1. Aeródromos
        self.airports = {}
        for m in re.finditer(r'<aixm:AirportHeliport\b[^>]*gml:id="uuid\.([^"]+)"[^>]*>.*?<aixm:designator>([A-Z0-9]{4})</aixm:designator>', xml, re.DOTALL):
            self.airports[m.group(1)] = m.group(2)
        for m in re.finditer(r'<aixm:AirportHeliport\b[^>]*gml:id="uuid\.([^"]+)"[^>]*>.*?<aixm:locationIndicatorICAO>([A-Z0-9]{4})</aixm:locationIndicatorICAO>', xml, re.DOTALL):
            self.airports[m.group(1)] = m.group(2)

        # 2. Waypoints / DesignatedPoints com coordenadas WGS-84
        self.waypoints = {}
        for m in re.finditer(r'<aixm:DesignatedPoint\b[^>]*gml:id="uuid\.([^"]+)"[^>]*>(.*?)</aixm:DesignatedPoint>', xml, re.DOTALL):
            uid = m.group(1)
            body = m.group(2)
            name_m = re.search(r'<aixm:designator>([^<]+)</aixm:designator>', body)
            pos_m = re.search(r'<gml:pos>([^<]+)</gml:pos>', body)
            if name_m and pos_m:
                coords = pos_m.group(1).strip().split()
                if len(coords) == 2:
                    self.waypoints[uid] = {
                        'name': name_m.group(1),
                        'lat': float(coords[0]),
                        'lon': float(coords[1])
                    }

        # 3. Pernas ARINC (Classes OACI)
        all_leg_tags = [
            'InitialLeg', 'IntermediateLeg', 'FinalLeg', 'MissedApproachLeg',
            'ArrivalLeg', 'ArrivalFeederLeg', 'DepartureLeg', 'ApproachLeg'
        ]

        legs_db = {}
        for ltag in all_leg_tags:
            pattern = rf'<aixm:{ltag}\b[^>]*gml:id="uuid\.([^"]+)"[^>]*>(.*?)</aixm:{ltag}>'
            for m in re.finditer(pattern, xml, re.DOTALL):
                l_uid = m.group(1)
                l_body = m.group(2)
                
                type_m = re.search(r'<aixm:legTypeARINC>([^<]+)</aixm:legTypeARINC>', l_body)
                leg_type = type_m.group(1) if type_m else 'TF'
                
                alt_min_m = re.search(r'<aixm:lowerLimitAltitude[^>]*>([^<]+)</aixm:lowerLimitAltitude>', l_body)
                alt_min = alt_min_m.group(1) if alt_min_m else None
                
                alt_max_m = re.search(r'<aixm:upperLimitAltitude[^>]*>([^<]+)</aixm:upperLimitAltitude>', l_body)
                alt_max = alt_max_m.group(1) if alt_max_m else None
                
                speed_m = re.search(r'<aixm:speedLimit[^>]*>([^<]+)</aixm:speedLimit>', l_body)
                speed = speed_m.group(1) if speed_m else None
                
                course_m = re.search(r'<aixm:course[^>]*>([^<]+)</aixm:course>', l_body)
                course = course_m.group(1) if course_m else None
                
                pt_m = re.search(r'<aixm:pointChoice_fixDesignatedPoint\b[^>]*xlink:href="urn:uuid:([^"]+)"', l_body)
                role_m = re.search(r'<aixm:role>([^<]+)</aixm:role>', l_body)
                
                fix_uid = pt_m.group(1) if pt_m else None
                role = role_m.group(1) if role_m else ltag.replace('Leg', '').upper()
                wpt = self.waypoints.get(fix_uid)
                
                legs_db[l_uid] = {
                    'segment_class': ltag,
                    'leg_type': leg_type,
                    'role': role,
                    'alt_min': float(alt_min) if alt_min else None,
                    'alt_max': float(alt_max) if alt_max else None,
                    'speed': float(speed) if speed else None,
                    'course': float(course) if course else None,
                    'fix_uid': fix_uid,
                    'fix_name': wpt['name'] if wpt else None,
                    'lat': wpt['lat'] if wpt else None,
                    'lon': wpt['lon'] if wpt else None
                }

        # 4. Decodificar Procedimentos (IAC, STAR, SID)
        proc_specs = [
            ('IAC', 'InstrumentApproachProcedure'),
            ('STAR', 'StandardInstrumentArrival'),
            ('SID', 'StandardInstrumentDeparture')
        ]

        self.procedures = {}
        for ptype, tag in proc_specs:
            pattern = rf'<aixm:{tag}\b[^>]*gml:id="uuid\.([^"]+)"[^>]*>(.*?)</aixm:{tag}>'
            for m in re.finditer(pattern, xml, re.DOTALL):
                p_uid = m.group(1)
                p_body = m.group(2)
                
                name_m = re.search(r'<aixm:name>([^<]+)</aixm:name>', p_body)
                p_name = name_m.group(1).strip() if name_m else 'UNKNOWN'
                
                apt_m = re.search(r'<aixm:airportHeliport\b[^>]*xlink:href="urn:uuid:([^"]+)"', p_body)
                apt_uid = apt_m.group(1) if apt_m else None
                apt_icao = self.airports.get(apt_uid, 'UNKNOWN')
                
                # Identificar Pista
                rwy_m = re.search(r'<aixm:runwayDirection\b[^>]*xlink:href="urn:uuid:([^"]+)"', p_body)
                pista = None
                pista_match = re.search(r'RWY\s*([0-9]{2}[LRC]?)', p_name, re.IGNORECASE)
                if pista_match:
                    pista = pista_match.group(1).upper()

                # Instrução Missed em RTF
                inst_m = re.search(r'<aixm:instruction>([^<]+)</aixm:instruction>', p_body)
                missed_text = None
                if inst_m:
                    rtf_clean = re.sub(r'\\[a-z0-9]+|\{|\}', '', inst_m.group(1)).strip()
                    rtf_clean = re.sub(r'\s+', ' ', rtf_clean)
                    missed_text = rtf_clean

                # Transições
                transitions = {}
                for t_m in re.finditer(r'<aixm:ProcedureTransition\b[^>]*>(.*?)</aixm:ProcedureTransition>', p_body, re.DOTALL):
                    t_body = t_m.group(1)
                    trans_id_m = re.search(r'<aixm:transitionId>([^<]+)</aixm:transitionId>', t_body)
                    trans_type_m = re.search(r'<aixm:type>([^<]+)</aixm:type>', t_body)
                    trans_id = trans_id_m.group(1) if trans_id_m else (trans_type_m.group(1) if trans_type_m else 'COMMON')
                    
                    legs = []
                    for l_m in re.finditer(r'<aixm:ProcedureTransitionLeg\b[^>]*>(.*?)</aixm:ProcedureTransitionLeg>', t_body, re.DOTALL):
                        leg_body = l_m.group(1)
                        seq_m = re.search(r'<aixm:seqNumberARINC>([^<]+)</aixm:seqNumberARINC>', leg_body)
                        seg_m = re.search(r'<aixm:theSegmentLeg\b[^>]*xlink:href="urn:uuid:([^"]+)"', leg_body)
                        seq = int(seq_m.group(1)) if seq_m else len(legs) + 1
                        seg_uid = seg_m.group(1) if seg_m else None
                        
                        leg_data = legs_db.get(seg_uid)
                        if leg_data:
                            legs.append({
                                'seq': seq,
                                'type': leg_data['leg_type'],
                                'fix': leg_data['fix_name'] or 'DIRECT',
                                'role': leg_data['role'],
                                'alt_min': leg_data['alt_min'],
                                'alt_max': leg_data['alt_max'],
                                'speed_max': leg_data['speed'],
                                'course': leg_data['course'],
                                'lat': leg_data['lat'],
                                'lon': leg_data['lon'],
                                'segment': leg_data['segment_class']
                            })
                    
                    legs.sort(key=lambda x: x['seq'])
                    transitions[trans_id] = {
                        'type': trans_type_m.group(1) if trans_type_m else 'APPROACH',
                        'legs': legs
                    }

                # Salvar procedimento indexado
                key = f"{apt_icao}_{p_name}".upper()
                self.procedures[key] = {
                    'id': p_uid,
                    'icao': apt_icao,
                    'tipo': ptype,
                    'nome': p_name,
                    'pista': pista,
                    'missed_instruction': missed_text,
                    'transitions': transitions
                }

        # Salvar em cache JSON
        os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
        with open(self.cache_path, 'w', encoding='utf-8') as f:
            json.dump({
                'procedures': self.procedures,
                'airports': self.airports,
                'waypoints': self.waypoints
            }, f, indent=2, ensure_ascii=False)

        print(f"✅ [FPDAM] Compilação concluída em {time.time()-t0:.2f}s!")
        print(f"   - {len(self.airports)} aeródromos")
        print(f"   - {len(self.waypoints)} waypoints WGS-84")
        print(f"   - {len(self.procedures)} procedimentos FPDAM indexados")
        print(f"   - Cache salvo em: {self.cache_path}")
        return True

    def find_procedure(self, icao, chart_name):
        """Busca flexível de procedimento FPDAM compatível com o nome da carta DECEA."""
        icao = icao.upper()
        # Normalização de nomes (ex: 'RNP RWY 10', 'IAC RNP RWY 10', 'RNP Z RWY 35L')
        chart_clean = chart_name.upper().replace('IAC', '').replace('SID', '').replace('STAR', '').strip()
        
        # 1. Busca exata
        for key, proc in self.procedures.items():
            if proc['icao'] == icao:
                p_clean = proc['nome'].upper().replace('IAC', '').strip()
                if p_clean == chart_clean or p_clean in chart_clean or chart_clean in p_clean:
                    return proc
        return None

if __name__ == "__main__":
    engine = AixmFpdamEngine()
    engine.load()
    sample = engine.find_procedure('SBSP', 'RNP Z RWY 35L')
    if sample:
        print("\n[TESTE DE BUSCA] Encontrado com sucesso:")
        print(f"  Procedimento: {sample['nome']} ({sample['icao']})")
        print(f"  Pista: {sample['pista']}")
        print(f"  Transições: {list(sample['transitions'].keys())}")
        for t_name, t_val in sample['transitions'].items():
            print(f"    - {t_name}: {len(t_val['legs'])} pernas")
    else:
        print("Procedimento de teste não encontrado.")
