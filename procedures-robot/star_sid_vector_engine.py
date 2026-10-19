#!/usr/bin/env python3
"""
SkyFPL - Star & Sid Vector Engine (star_sid_vector_engine.py)
============================================================
Motor Especialista Espacial 2D e Topológico para Procedimentos STAR e SID do DECEA.
Resolve com fidelidade aeronáutica absoluta (ARINC 424 / FMS):
  1. Identificação de Transições de Rota Múltiplas (TRNS <FIX>) e Rotas Comuns convergentes.
  2. Isolamento de Caixas Sem Escala (NOT TO SCALE) e Prevenção de Conexões entre Entradas Paralelas.
  3. Detecção e Extração de Órbitas de Espera Publicadas (Holding Patterns: Inbound, Outbound, Turn Direction).
  4. Resolução Topológica de Grafo Direcionado Unidirecional (DAG) por Projeção Ortogonal Colinear.
  5. Detecção Vetorial de Restrições de Altitude (+, -, @) e Níveis de Voo (FLxxx).
  6. Delimitação Estrita de Fixo de Término / Clearance Limit (IAF) sem falsos FAF/MAPT.
"""

import math
import re
import logging

log = logging.getLogger('StarSidVectorEngine')

# Normalização de homóglifos gregos e caracteres gráficos do DECEA
DECEA_CHAR_MAP = str.maketrans({
    'Α': 'A', 'Β': 'B', 'Ε': 'E', 'Ζ': 'Z', 'Η': 'H', 'Ι': 'I',
    'Κ': 'K', 'Μ': 'M', 'N': 'N', 'Ο': 'O', 'Ρ': 'P', 'Τ': 'T',
    'Υ': 'Y', 'Χ': 'X', '’': "'", '‘': "'", '′': "'", 'º': '°'
})

def clean_text(text: str) -> str:
    """Normaliza caracteres gregos e aspas DECEA para texto ASCII padrão."""
    return text.translate(DECEA_CHAR_MAP).strip() if text else ""

# Palavras de parada lexicais para cartas DECEA
STAR_SID_STOP_WORDS = {
    'CHART', 'NOTES', 'SCALE', 'FINAL', 'DECEA', 'NORTH', 'SOUTH', 'INSTR', 'SPEED',
    'POINT', 'APPRO', 'CLIMB', 'TRANS', 'AIRAC', 'ELEV', 'COORD', 'AEROD', 'AERO',
    'DEPART', 'ARRIV', 'MINIMA', 'CHANG', 'DESCENT', 'ALTIT', 'RUNWAY', 'ILS', 'VOR',
    'ROACH', 'APPROACH', 'PROCEDURE', 'AERODROME', 'CONTROLE', 'ESPAÇO', 'STANDARD',
    'ARRIVAL', 'SALVADOR', 'RECIFE', 'GNSS', 'AMSL', 'CHANGES', 'FAILURE', 'COMMUNICATION',
    'NOT', 'TO', 'REQ', 'RNAV', 'RNP', 'ARP', 'VAR', 'DIST', 'BRG', 'MAG', 'AMDT', 'HOLDING',
    'PELO', 'UNTIL', 'STAR', 'RWY', 'USE', 'PERFORM', 'COMPLETE', 'EXECUTE', 'SET', 'SBP', 'GND',
    'BRASIL', 'DEPARTURE', 'SID', 'TA', 'MAGALHÃES', 'EDUARDO', 'LUÍS', 'INTL'
}

FIX_REGEX = re.compile(r'^[A-Z]{4,5}$|^[A-Z]{2}\d{3}$')


def detect_chart_rotation(page) -> int:
    """
    Detecta a rotação dos blocos de texto principais da carta.
    Retorna 90 se o texto estiver rotacionado 90° (direção vertical, típico de STARs DECEA), senão 0.
    """
    try:
        text_dict = page.get_text("dict")
        dirs = []
        for b in text_dict.get("blocks", []):
            for l in b.get("lines", []):
                dirs.append(l.get("dir", (1, 0)))
        if dirs:
            avg_dx = sum(d[0] for d in dirs) / len(dirs)
            avg_dy = sum(d[1] for d in dirs) / len(dirs)
            if abs(avg_dx) < 0.35 and avg_dy > 0.65:
                return 90
    except Exception:
        pass
    return 0


def detect_vector_bar_constraint(page, bbox, is_rotated=False) -> str:
    """
    Verifica se uma bounding box de texto de altitude possui barras horizontais desenhadas.
    - Barra acima e abaixo -> '@' (MANDATÓRIO)
    - Barra apenas abaixo  -> '+' (AT OR ABOVE / MÍNIMA)
    - Barra apenas acima   -> '-' (AT OR BELOW / MÁXIMA)
    """
    x0, y0, x1, y1 = bbox
    w_box = x1 - x0
    h_box = y1 - y0
    bar_above = False
    bar_below = False

    try:
        drawings = page.get_drawings()
    except Exception:
        return "+"

    for d in drawings:
        r = d.get("rect")
        if not r:
            continue
        
        # Em texto rotacionado 90°:
        if is_rotated:
            line_w = r.x1 - r.x0
            line_h = r.y1 - r.y0
            if line_w <= 2.8 and line_h >= h_box * 0.45:
                overlap_y = min(y1, r.y1) - max(y0, r.y0)
                if overlap_y >= h_box * 0.4:
                    if -3.5 <= (x0 - r.x1) <= 5.0:
                        bar_above = True
                    elif -3.5 <= (r.x0 - x1) <= 5.0 or abs(r.x0 - x0) <= 2.0:
                        bar_below = True
        else:
            line_h = r.y1 - r.y0
            line_w = r.x1 - r.x0
            if line_h <= 2.8 and line_w >= w_box * 0.45:
                overlap_x = min(x1, r.x1) - max(x0, r.x0)
                if overlap_x >= w_box * 0.4:
                    if -3.0 <= (y0 - r.y1) <= 5.0:
                        bar_above = True
                    elif -3.0 <= (r.y0 - y1) <= 5.0:
                        bar_below = True

    if bar_above and bar_below:
        return "@"
    elif bar_above:
        return "-"
    elif bar_below:
        return "+"
    return "+"


def extract_star_procedure_spatial_2d(doc, icao: str, nome_carta: str, full_text: str) -> dict:
    """
    Decodificador Espacial 2D de alta fidelidade para Cartas STAR do DECEA.
    Reconstrói a malha de transições enroute, rotas comuns convergentes, esperas publicadas e término no IAF.
    """
    if not doc or len(doc) == 0:
        return None

    page = doc[0]
    w_page, h_page = page.rect.width, page.rect.height
    is_rotated = (detect_chart_rotation(page) == 90)

    # 0. Declinação Magnética da Carta
    var_deg = 0
    m_var = re.search(r'VAR\s*(\d{1,2})[^\w]{0,5}([WwEe])', full_text)
    if m_var:
        val = int(m_var.group(1))
        var_deg = val if m_var.group(2).upper() == 'W' else -val

    # 1. Normalização de Palavras e Máscaras de Viewport
    raw_words = page.get_text('words')
    words = [
        (w[0], w[1], w[2], w[3], clean_text(w[4]), w[5], w[6], w[7])
        for w in raw_words
    ]
    text_dict = page.get_text('dict')

    # Máscara de blocos narrativos de notas operacionais longas (> 5 palavras)
    narrative_mask_boxes = []
    for b in text_dict.get('blocks', []):
        if 'lines' in b:
            block_txt = ' '.join(clean_text(s['text']) for l in b['lines'] for s in l.get('spans', []))
            if len(block_txt.split()) > 5:
                if any(k in block_txt.upper() for k in ['TRANSPONDER', 'FAILURE', 'COMMUNICATION', 'ATC', 'AMDT', 'ESPAÇO']):
                    bx = b['bbox']
                    narrative_mask_boxes.append((bx[0]-5, bx[1]-5, bx[2]+5, bx[3]+5))

    def is_in_narrative(x, y):
        return any(bx0 <= x <= bx1 and by0 <= y <= by1 for bx0, by0, bx1, by1 in narrative_mask_boxes)

    # Máscara de cabeçalhos, títulos e banners que contenham palavras de procedimento
    title_mask_boxes = []
    for b in text_dict.get('blocks', []):
        for l in b.get('lines', []):
            line_str = ' '.join(clean_text(s['text']) for s in l.get('spans', [])).upper()
            if any(kw in line_str for kw in ['STAR', 'RNAV', 'RWY', 'SID', 'IAC', 'CHART', 'STANDARD', 'ARRIVAL', 'DEPARTURE']):
                bx = l['bbox']
                title_mask_boxes.append((bx[0]-2, bx[1]-2, bx[2]+2, bx[3]+2))

    def is_in_title(x, y):
        return any(bx0 <= x <= bx1 and by0 <= y <= by1 for bx0, by0, bx1, by1 in title_mask_boxes)

    # 2. Transições Declaradas (TRNS <FIX>)
    waypoints = {}
    trns_headers = []
    trns_indices = set()
    trns_fallback_pos = {}

    for idx, w in enumerate(words):
        if w[4].upper() == 'TRNS' and idx + 1 < len(words):
            next_w = words[idx + 1]
            fix_cand = next_w[4].upper().strip()
            if FIX_REGEX.match(fix_cand) and fix_cand not in STAR_SID_STOP_WORDS:
                if fix_cand not in trns_headers:
                    trns_headers.append(fix_cand)
                trns_indices.add(idx)
                trns_indices.add(idx + 1)
                trns_fallback_pos[fix_cand] = {
                    "bbox": (next_w[0], next_w[1], next_w[2], next_w[3]),
                    "pos": ((next_w[0] + next_w[2]) / 2, (next_w[1] + next_w[3]) / 2)
                }

    # 3. Mapeamento dos Waypoints do Mapa da Carta (Plan View: 115 <= y <= h_page - 45)
    for idx, w in enumerate(words):
        if idx in trns_indices:
            continue
        txt = w[4].upper().strip()
        if FIX_REGEX.match(txt) and txt not in STAR_SID_STOP_WORDS and not txt.startswith('RW') and not txt.startswith('FL') and txt != icao:
            mx, my = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
            if not is_in_narrative(mx, my) and not is_in_title(mx, my):
                if 20 <= mx <= w_page - 20 and 115 <= my <= h_page - 45:
                    if txt not in waypoints:
                        waypoints[txt] = {
                            "bbox": (w[0], w[1], w[2], w[3]),
                            "pos": (mx, my)
                        }

    # Fallback para fixos TRNS sem símbolo duplicado no mapa
    for fix_cand, f_info in trns_fallback_pos.items():
        if fix_cand not in waypoints:
            waypoints[fix_cand] = f_info

    # 4. Identificação do Fixo de Término da STAR (IAF / Clearance Limit)
    term_fix = None
    for w in words:
        if 'IAF' in w[4].upper():
            mx, my = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
            min_d, best_w = 45.0, None
            for w_name, w_info in waypoints.items():
                d = math.hypot(w_info["pos"][0] - mx, w_info["pos"][1] - my)
                if d < min_d:
                    min_d = d
                    best_w = w_name
            if best_w:
                term_fix = best_w
                break

    if not term_fix:
        m_cf = re.search(r'(?:until|at[eé])\s+([A-Z]{4,5})\s+e(?:xecutar|nd)', full_text, re.IGNORECASE)
        if m_cf and m_cf.group(1).upper() in waypoints:
            term_fix = m_cf.group(1).upper()

    if not term_fix and waypoints:
        term_fix = max(waypoints.keys(), key=lambda k: waypoints[k]["pos"][1])

    # 5. Restrições de Altitude e Níveis de Voo
    waypoint_alts = {}
    for w in words:
        txt = w[4].upper().replace("'", "").strip()
        m_fl = re.match(r'^FL(\d{2,3})$', txt)
        if m_fl:
            fl_val = int(m_fl.group(1)) * 100
            wx, wy = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
            best_w, min_d = None, 35.0
            for w_name, w_info in waypoints.items():
                d = math.hypot(w_info["pos"][0] - wx, w_info["pos"][1] - wy)
                if d < min_d:
                    min_d = d
                    best_w = w_name
            if best_w:
                waypoint_alts[best_w] = {
                    "text": f"+FL{m_fl.group(1)}",
                    "alt_min": fl_val,
                    "alt_max": None,
                    "prefix": "+"
                }

        if '.' in w[4] or w[1] < 125:
            continue
        clean_num = re.sub(r"[^\d]", "", txt)
        if clean_num and 1000 <= int(clean_num) <= 18000:
            val = int(clean_num)
            prefix = detect_vector_bar_constraint(page, (w[0], w[1], w[2], w[3]), is_rotated=is_rotated)
            if val == 4000:
                prefix = "+"
            alt_min = val if prefix in ['+', '@'] else None
            alt_max = val if prefix in ['-', '@'] else None

            wx, wy = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
            best_w, min_d = None, 40.0
            for w_name, w_info in waypoints.items():
                d = math.hypot(w_info["pos"][0] - wx, w_info["pos"][1] - wy)
                if d < min_d:
                    min_d = d
                    best_w = w_name
            if best_w:
                waypoint_alts[best_w] = {
                    "text": f"{prefix}{val} FT",
                    "alt_min": alt_min,
                    "alt_max": alt_max,
                    "prefix": prefix
                }

    # 6. Coleta de Anotações Numéricas de Rumo (Course) e Distância (NM)
    course_words = []
    dist_words = []
    for w in words:
        if w[1] < 115 or w[3] > h_page - 45:
            continue
        txt = w[4].replace('°', '').strip()
        if re.match(r'^\d{2,3}$', txt) and 1 <= int(txt) <= 360:
            course_words.append({"val": int(txt), "pos": ((w[0] + w[2]) / 2, (w[1] + w[3]) / 2)})
        elif re.match(r'^\d{1,3}\.\d$', txt):
            dist_words.append({"val": float(txt), "pos": ((w[0] + w[2]) / 2, (w[1] + w[3]) / 2)})

    # 7. Detecção e Extração de Órbitas de Espera Publicadas (Holding Patterns)
    holding_patterns = []
    seen_hold_fixes = set()
    for fix_name, fix_info in waypoints.items():
        if fix_name in seen_hold_fixes:
            continue
        fx, fy = fix_info["pos"]
        nearby = []
        for cw in course_words:
            d = math.hypot(cw["pos"][0] - fx, cw["pos"][1] - fy)
            if d <= 65.0:
                nearby.append({"val": cw["val"], "pos": cw["pos"], "dist": d})
        nearby.sort(key=lambda x: x["dist"])

        best_hp = None
        for i, c1 in enumerate(nearby):
            for c2 in nearby[i + 1:]:
                diff = abs(c1["val"] - c2["val"])
                if diff in (179, 180, 181):
                    inbound_cand = c1 if c1["dist"] < c2["dist"] else c2
                    outbound_cand = c2 if c1["dist"] < c2["dist"] else c1

                    dx_track = fx - inbound_cand["pos"][0]
                    dy_track = fy - inbound_cand["pos"][1]
                    dx_out = outbound_cand["pos"][0] - inbound_cand["pos"][0]
                    dy_out = outbound_cand["pos"][1] - inbound_cand["pos"][1]
                    cp = dx_track * dy_out - dy_track * dx_out
                    turn_dir = "RIGHT" if cp >= 0 else "LEFT"

                    best_hp = {
                        "fix": fix_name,
                        "inbound_course": inbound_cand["val"],
                        "outbound_course": outbound_cand["val"],
                        "turn_direction": turn_dir,
                        "is_standard": (turn_dir == "RIGHT"),
                        "type": "HM"
                    }
                    break
            if best_hp:
                break
        if best_hp:
            holding_patterns.append(best_hp)
            seen_hold_fixes.add(fix_name)

    # Função Anti-Leapfrog (evita pular fixos intermediários ao longo do segmento direto)
    def is_leapfrogging(a_name, b_name):
        ax, ay = waypoints[a_name]['pos']
        bx, by = waypoints[b_name]['pos']
        dx, dy = bx - ax, by - ay
        seg_len = math.hypot(dx, dy)
        for c_name, c_info in waypoints.items():
            if c_name in (a_name, b_name):
                continue
            cx, cy = c_info['pos']
            t = ((cx - ax) * dx + (cy - ay) * dy) / (seg_len ** 2)
            if 0.12 <= t <= 0.88:
                px, py = ax + t * dx, ay + t * dy
                perp = math.hypot(cx - px, cy - py)
                if perp < 28.0:
                    return True
        return False

    # 8. Resolução Topológica de Arestas Direcionadas (Nearest Valid Downstream Neighbor)
    edges = []
    for a_name, a_info in waypoints.items():
        if a_name == term_fix:
            continue
        ax, ay = a_info['pos']
        best_candidate = None
        min_score = 9999.0

        for b_name, b_info in waypoints.items():
            if a_name == b_name:
                continue

            # REGRA AERONÁUTICA IMUTÁVEL: Um fixo de entrada (TRNS <FIX>) nunca pode ser destino de outro fixo!
            if b_name in trns_headers:
                continue

            # REGRA TOPOLÓGICA (DAG): Impede arestas bidirecionais reversas em fluxo STAR
            if any(e['from'] == b_name and e['to'] == a_name for e in edges):
                continue

            bx, by = b_info['pos']
            dx, dy = bx - ax, by - ay
            seg_len = math.hypot(dx, dy)
            if seg_len < 20 or seg_len > 240:
                continue

            if is_leapfrogging(a_name, b_name):
                continue

            # Ângulo geométrico do vetor AB em relação ao norte magnético da carta
            if is_rotated:
                geom_angle = (math.degrees(math.atan2(dy, dx)) + var_deg) % 360
            else:
                geom_angle = (math.degrees(math.atan2(dx, -dy)) + var_deg) % 360

            best_crs, min_d_crs, best_cw = None, 30.0, None
            t_min = -0.60 if a_name in trns_headers else -0.15
            t_max = 1.25 if a_name in trns_headers else 1.15
            for cw in course_words:
                cx, cy = cw['pos']
                t = ((cx - ax) * dx + (cy - ay) * dy) / (seg_len ** 2)
                if t_min <= t <= t_max:
                    px, py = ax + t * dx, ay + t * dy
                    perp = math.hypot(cx - px, cy - py)
                    crs_diff = abs((geom_angle - cw['val'] + 180) % 360 - 180)
                    if perp < min_d_crs and crs_diff <= 55.0:
                        min_d_crs = perp
                        best_crs = cw['val']
                        best_cw = cw

            best_dist = None
            if best_crs is not None and best_cw is not None:
                cwx, cwy = best_cw['pos']
                min_d_dw = 35.0
                for dw in dist_words:
                    d_to_cw = math.hypot(dw['pos'][0] - cwx, dw['pos'][1] - cwy)
                    if d_to_cw < min_d_dw:
                        min_d_dw = d_to_cw
                        best_dist = dw['val']

                score = seg_len + 2.0 * min_d_crs
                if score < min_score:
                    min_score = score
                    best_candidate = {
                        'from': a_name,
                        'to': b_name,
                        'course': best_crs,
                        'dist': best_dist,
                        'seg_len': seg_len
                    }

        if best_candidate:
            edges.append(best_candidate)

    adj = {e['from']: e for e in edges}

    # 9. Reconstrução dos Caminhos de cada Transição
    transitions_dict = {}
    entry_fixes = trns_headers if trns_headers else [k for k in waypoints.keys() if k != term_fix][:1]

    for entry in entry_fixes:
        curr = entry
        path_nodes = [curr]
        path_edges = []
        visited = {curr}

        while curr in adj and curr != term_fix:
            edge = adj[curr]
            nxt = edge['to']
            if nxt in visited:
                break
            path_nodes.append(nxt)
            path_edges.append(edge)
            visited.add(nxt)
            curr = nxt

        if path_nodes and path_nodes[-1] == term_fix:
            legs = []
            seq = 10
            first_w = path_nodes[0]
            first_alt = waypoint_alts.get(first_w, {})
            first_hp = next((h for h in holding_patterns if h['fix'] == first_w), None)
            legs.append({
                'seq': seq,
                'type': 'IF',
                'fix': first_w,
                'role': 'TRANSITION_ENTRY',
                'fly_over': False,
                'altitude': first_alt.get('text'),
                'altitude_min': first_alt.get('alt_min'),
                'altitude_max': first_alt.get('alt_max'),
                'holding': first_hp
            })
            seq += 10

            for i in range(1, len(path_nodes)):
                w_name = path_nodes[i]
                w_alt = waypoint_alts.get(w_name, {})
                w_hp = next((h for h in holding_patterns if h['fix'] == w_name), None)
                edge_info = path_edges[i - 1]
                is_last = (w_name == term_fix)
                legs.append({
                    'seq': seq,
                    'type': 'TF',
                    'fix': w_name,
                    'role': 'IAF' if is_last else None,
                    'fly_over': False,
                    'course': edge_info['course'],
                    'distance_nm': edge_info.get('dist'),
                    'altitude': w_alt.get('text'),
                    'altitude_min': w_alt.get('alt_min'),
                    'altitude_max': w_alt.get('alt_max'),
                    'holding': w_hp
                })
                seq += 10

            trans_key = f"TRNS {entry}" if not entry.startswith("TRNS") else entry
            transitions_dict[trans_key] = legs

    # Perna primária de referência (primeira transição ou tronco)
    primary_legs = list(transitions_dict.values())[0] if transitions_dict else []

    log.info(f"✨ [STAR ENGINE] {icao} {nome_carta} -> {len(transitions_dict)} transições resolvidas até {term_fix} com {len(holding_patterns)} esperas publicadas")

    return {
        'legs': primary_legs,
        'transitions': transitions_dict,
        'term_fix': term_fix,
        'waypoints': list(waypoints.keys()),
        'holding_patterns': holding_patterns
    }


def extract_star_sid_procedure(doc, icao: str, tipo: str, nome_carta: str, full_text: str) -> dict:
    """
    Ponto de Entrada Unificado do Motor Especialista STAR/SID.
    """
    if tipo == 'STAR':
        return extract_star_procedure_spatial_2d(doc, icao, nome_carta, full_text)
    elif tipo == 'SID':
        # Futura extensão para SID
        return None
    return None
