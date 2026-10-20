#!/usr/bin/env python3
"""
SkyFPL - Gemini Multimodal FMS Chart Auditor & 360° Extractor
=============================================================
Auditor de Voo e Extrator Especialista ICAO / DECEA.
Recebe o PDF oficial da carta (em bytes binários) + o rascunho topológico do motor Python.
Executa:
  1. Validação visual rigorosa de cada perna (elimina tetos/pisos de espaços aéreos condicionados
     como SBP/SBR/SBD e cotas de relevo, confirmando barras vetoriais +, -, @).
  2. Extração 360° de elementos complementares não tabulados:
     - Notas Operacionais (RMK / Restrições ATC / Ruído)
     - Obstáculos Notáveis e Cotas Altimétricas no Relevo
     - Quadrículas de Altitude Mínima de Área (AMA / MEF)
     - Variação Magnética Anual (VA/CHG)
     - Rádio-Auxílios Terrestres (VOR/NDB/DME com canal e morse)
"""

import os
import json
import logging

log = logging.getLogger('GeminiAuditor')

def audit_and_enrich_chart_with_gemini(pdf_bytes: bytes, icao: str, tipo: str, nome_carta: str, transitions_dict: dict, extracted_base: dict) -> tuple:
    """
    Submete a carta PDF e o rascunho de pernas à auditoria multimodal do Gemini.
    Retorna (transitions_dict_auditado, extracted_base_enriquecido).
    Em caso de ausência de chave ou falha de rede, retorna os dados originais com segurança.
    """
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        log.info(f"ℹ️ [AI AUDITOR] GEMINI_API_KEY não configurada. Usando motor determinístico padrão para {icao} {nome_carta}.")
        return transitions_dict, extracted_base

    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)

        # Prepara resumo compacto das transições para auditoria
        draft_summary = {}
        for trans_name, legs in transitions_dict.items():
            draft_summary[trans_name] = [
                {
                    'seq': l.get('seq'),
                    'type': l.get('type'),
                    'fix': l.get('fix'),
                    'role': l.get('role'),
                    'course': l.get('course'),
                    'distance_nm': l.get('distance_nm'),
                    'altitude': l.get('altitude'),
                    'altitude_exact': l.get('altitude_exact'),
                    'altitude_min': l.get('altitude_min'),
                    'altitude_max': l.get('altitude_max'),
                    'speed_max': l.get('speed_max')
                }
                for l in legs
            ]

        prompt = f"""
Você é o Auditor Chefe de Procedimentos de Navegação FMS (Flight Management System) e Especialista em Cartografia Aeronáutica ICAO / DECEA do ecossistema SkyFPL.

Você está recebendo o arquivo PDF REAL e OFICIAL da carta aeronáutica:
- Aeródromo: {icao}
- Tipo de Carta: {tipo}
- Nome do Procedimento: {nome_carta}

O motor vetorial em Python gerou este rascunho preliminar das rotas e transições:
```json
{json.dumps(draft_summary, ensure_ascii=False, indent=2)}
```

SUA MISSÃO DIVIDE-SE EM DUAS ETAPAS OBRIGATÓRIAS:

ETAPA 1: AUDITORIA DE SEGURANÇA DAS PERNAS E RESTRIÇÕES VERTICAIS
Examine visualmente o PDF anexado:
1. Verifique cada waypoint listado no rascunho.
2. Identifique e elimine qualquer restrição de altitude espúria capturada por engano que pertença na verdade a:
   - Limites verticais de Espaços Aéreos Condicionados (ex: '1500 AMSL', 'GND', 'SBP 225', 'SBR...', 'SBD...', 'CTR', 'TMA', 'PROIBIDO', 'RESTRITO').
   - Cotas soltas de obstáculos ou relevo terrestre (ex: 426', 534').
3. Se um waypoint tiver uma restrição real de tráfego aéreo na carta (ex: 7000' em SV076 ou 4000' em UDESI), confirme o valor e o sinal:
   - Barra preta horizontal ABAIXO do número -> '+' (AT OR ABOVE, ex: +7000 FT)
   - Barra preta horizontal ACIMA do número -> '-' (AT OR BELOW)
   - Barras em AMBOS os lados -> '@' (MANDATORY AT)
   - Janela entre duas altitudes (ex: 3000' e 6000') -> '+3000 / -6000 FT' (alt_min=3000, alt_max=6000)
   - Sem barra ou altitude no fixo -> altitude=null, altitude_min=null, altitude_max=null.
4. Mantenha os cursos (course), distâncias (distance_nm) e papéis táticos (role: IAF, IF, FAF, MAPT, STAR_ENTRY).

ETAPA 2: EXTRAÇÃO 360° DE ELEMENTOS NÃO TABULADOS
Examine a folha da carta inteira e extraia:
1. operational_remarks: Lista de strings contendo todas as notas operacionais, avisos ATC, restrições de atenuação de ruído, proibições de curva antes do DER e notas livres publicadas no texto da folha (ex: 'Curva antes do DER proibida', 'Aeronaves Cat A, B, C e D', etc.).
2. notable_obstacles: Lista de obstáculos com cota altimétrica plotados no relevo (ex: [ {{"elevation_ft": 426, "type": "ANTENNA"}}, {{"elevation_ft": 534, "type": "TERRAIN_PEAK"}} ]).
3. area_minimum_altitudes: Lista de valores de altitude mínima de área (AMA / MEF) impressos nas quadrículas da carta (ex: [ 2900, 2000 ]).
4. mag_var_annual_change: Taxa anual de variação magnética (ex: '02\\' E' de 'VAR 23° W / VA/CHG 02\\' E').
5. navaids: Lista de rádio-auxílios convencionais plotados com frequência, canal e indicativo morse (ex: [ {{"id": "SVD", "type": "VOR/DME", "frequency": "116.80", "channel": "115X"}} ]).

FORMATO DE RESPOSTA OBRIGATÓRIO (JSON PURO):
Retorne ESTRITAMENTE um objeto JSON com esta estrutura:
{{
  "transitions": {{
    "<NOME_DA_TRANSICAO>": [
      {{
        "seq": 10,
        "type": "IF",
        "fix": "FIXNAME",
        "role": "IAF",
        "course": null,
        "distance_nm": null,
        "altitude": "+7000 FT",
        "altitude_exact": null,
        "altitude_min": 7000,
        "altitude_max": null,
        "speed_max": null
      }}
    ]
  }},
  "operational_remarks": ["..."],
  "notable_obstacles": [ {{"elevation_ft": 426, "type": "ANTENNA"}} ],
  "area_minimum_altitudes": [2900],
  "mag_var_annual_change": "02' E",
  "navaids": []
}}
"""

        log.info(f"🤖 [AI AUDITOR] Enviando PDF real de {icao} {nome_carta} para auditoria multimodal Gemini 2.5 Flash...")
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[
                types.Part.from_bytes(data=pdf_bytes, mime_type='application/pdf'),
                prompt
            ],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.0
            ),
        )

        if not response or not response.text:
            log.warning(f"⚠️ [AI AUDITOR] Resposta vazia da IA para {icao} {nome_carta}. Mantendo dados brutos.")
            return transitions_dict, extracted_base

        audited_data = json.loads(response.text)

        # 1. Atualiza as pernas das transições com as pernas auditadas
        audited_transitions = audited_data.get('transitions')
        if audited_transitions and isinstance(audited_transitions, dict):
            for t_name, a_legs in audited_transitions.items():
                if t_name in transitions_dict:
                    # Preserva campos complementares como holding patterns calculadas
                    orig_legs = transitions_dict[t_name]
                    orig_by_seq = {l.get('seq'): l for l in orig_legs}
                    
                    merged_legs = []
                    for al in a_legs:
                        seq = al.get('seq')
                        base_leg = orig_by_seq.get(seq, {}).copy()
                        # Atualiza com as validações de altitude da IA
                        base_leg['altitude'] = al.get('altitude')
                        base_leg['altitude_exact'] = al.get('altitude_exact')
                        base_leg['altitude_min'] = al.get('altitude_min')
                        base_leg['altitude_max'] = al.get('altitude_max')
                        if al.get('speed_max'):
                            base_leg['speed_max'] = al.get('speed_max')
                        merged_legs.append(base_leg)
                    
                    if merged_legs:
                        transitions_dict[t_name] = merged_legs
                        log.info(f"  ✅ [AI AUDITOR] Transição '{t_name}' auditada e homologada pela IA.")

        # 2. Enriquece os metadados 360° da carta
        meta = extracted_base.setdefault('chart_metadata', {})
        if audited_data.get('operational_remarks'):
            meta['operational_remarks'] = audited_data['operational_remarks']
        if audited_data.get('notable_obstacles'):
            meta['notable_obstacles'] = audited_data['notable_obstacles']
        if audited_data.get('area_minimum_altitudes'):
            meta['area_minimum_altitudes'] = audited_data['area_minimum_altitudes']
        if audited_data.get('mag_var_annual_change'):
            meta['mag_var_annual_change'] = audited_data['mag_var_annual_change']
        if audited_data.get('navaids'):
            meta['navaids'] = audited_data['navaids']

        log.info(f"✨ [AI AUDITOR] Auditoria e enriquecimento 360° concluídos com sucesso para {icao} {nome_carta}!")
        return transitions_dict, extracted_base

    except Exception as e:
        log.warning(f"⚠️ [AI AUDITOR] Exceção durante auditoria de {icao} {nome_carta} ({e}). Prosseguindo com dados do motor vetorial.")
        return transitions_dict, extracted_base
