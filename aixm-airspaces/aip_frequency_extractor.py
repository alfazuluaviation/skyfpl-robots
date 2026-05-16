import os
import json
import requests
import pdfplumber
import sys
import re

# Mapeamento do catálogo do DECEA para puxar a seção Completa do AIP
AISWEB_API = "https://aisweb.decea.gov.br/api"

def get_aip_url():
    """
    Tenta obter a URL do AIP Brasil atualizado do repositório/API
    Por enquanto usaremos um PDF estático de exemplo para extração.
    """
    print("Buscando link oficial do AIP Brasil (ENR 2.1)...")
    return "https://aisweb.decea.gov.br/download/?public=60b181e0-2a0e-4728-9b562fa4ff340fb0.pdf"

def download_pdf(url, output_path):
    print(f"Iniciando download do AIP ({url})...")
    response = requests.get(url, stream=True, verify=False)
    
    if response.status_code in [301, 302]:
        url = response.headers.get('Location')
        response = requests.get(url, stream=True, verify=False)

    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                done = int(50 * downloaded / (total_size if total_size > 0 else 100000000))
                sys.stdout.write(f"\r[{'=' * done}{' ' * (50-done)}] {downloaded//1024} KB")
                sys.stdout.flush()
    print("\nDownload concluído.")

def classify_service_type(text):
    """
    Classifica o tipo de serviço COM BASE NO CONTEÚDO SEMÂNTICO.
    NÃO depende da posição/ordem no PDF.
    """
    upper = text.upper()
    
    # FIS: Identificação por palavras-chave
    if 'FIS' in upper and ('INFORMAÇÃO' in upper or 'INFORMACAO' in upper or 'DLY' in upper or 'INFORMATION' in upper):
        return 'FIS'
    # Checar se o bloco é explicitamente FIS
    if re.search(r'\bFIS\b', upper) and 'ACC' not in upper and 'METEORO' not in upper:
        return 'FIS'
    
    # METEORO: Identificação por palavras-chave
    if 'METEORO' in upper or 'VOLMET' in upper or 'METEOROLOG' in upper:
        return 'METEORO'
    
    # ACC: Identificação por palavras-chave (fallback padrão para controle)
    if any(kw in upper for kw in ['CENTRO', 'CENTER', 'ACC', 'CONTROL', 'CONTROLE']):
        return 'ACC'
    
    # Fallback: se contém frequência aeronáutica sem classificação clara, assume ACC
    return 'ACC'

def extract_priority(freq_text):
    """
    Extrai a prioridade da frequência a partir do texto.
    Ex: '134.575 MHZ PRI' → 'PRI'
    Ex: '121.500 MHZ EMERG' → 'EMERG'
    """
    upper = freq_text.upper()
    if 'EMERG' in upper:
        return 'EMERG'
    elif 'SRY' in upper or 'SECONDARY' in upper or 'SEC' in upper:
        return 'SRY'
    else:
        return 'PRI'

def extract_sector_frequencies(pdf_path):
    """
    Extrai frequências do AIP ENR 2.1 organizadas POR SETOR e POR TIPO DE SERVIÇO.
    Retorna uma lista de dicts prontos para inserção no Supabase.
    """
    print("\nIniciando varredura com pdfplumber (Extração por Setor)...")
    
    freq_pattern = re.compile(r'(1[0-2]\d\.\d{3}|13[0-7]\.\d{3})')
    # Regex melhorada para capturar sub-setores como 16AL, 16AU, 01B
    sector_pattern = re.compile(r'SECT?\s*(\d+[A-Z]{0,2})', re.IGNORECASE)
    fir_pattern = re.compile(r'FIR\s+(\w+)', re.IGNORECASE)
    fir_ident_pattern = re.compile(r'(SB[A-Z]{2})', re.IGNORECASE)
    
    all_records = []
    
    # Mapeamento de nomes de FIR para ICAO
    FIR_NAME_TO_ICAO = {
        'BRASILIA': 'SBBS', 'BRASÍLIA': 'SBBS',
        'RECIFE': 'SBRE',
        'AMAZONICA': 'SBAZ', 'AMAZÔNICA': 'SBAZ',
        'CURITIBA': 'SBCW',
        'ATLANTICO': 'SBAO', 'ATLÂNTICO': 'SBAO',
    }
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            print(f"Total de páginas no AIP: {total_pages}")
            
            # Identificar páginas com setores de FIR
            fir_sector_pages = []
            for i in range(total_pages):
                page = pdf.pages[i]
                text = page.extract_text() or ""
                if 'FIR' in text and ('SECT' in text.upper() or 'SETOR' in text.upper()):
                    fir_sector_pages.append(i)
            
            print(f"Encontradas {len(fir_sector_pages)} páginas com setores de FIR")
            
            # Processar cada página
            for page_idx in fir_sector_pages:
                page = pdf.pages[page_idx]
                tables = page.extract_tables()
                
                if not tables:
                    continue
                
                for table in tables:
                    if not table:
                        continue
                    
                    current_fir = None
                    current_sector = None
                    
                    for row in table:
                        if not row or len(row) < 2:
                            continue
                        
                        col0 = str(row[0] or "").strip()
                        col0_upper = col0.upper()
                        
                        # Detectar FIR + Setor (ex: "FIR Brasília SECT 16AL")
                        if 'FIR' in col0_upper:
                            # Extrair nome da FIR
                            for name, icao in FIR_NAME_TO_ICAO.items():
                                if name.upper() in col0_upper:
                                    current_fir = icao
                                    break
                            
                            # Se não achou por nome, tentar ICAO direto
                            if not current_fir:
                                ident_match = fir_ident_pattern.search(col0)
                                if ident_match:
                                    current_fir = ident_match.group(1).upper()
                            
                            # Extrair número do setor (como string para manter letras)
                            sector_match = sector_pattern.search(col0)
                            if sector_match:
                                current_sector = sector_match.group(1).upper()
                        
                        if not current_fir or current_sector is None:
                            continue
                        
                        # Processar colunas para extrair frequências
                        full_row_text = " ".join([str(c or "").replace('\n', ' ') for c in row])
                        
                        # Extrair frequências da linha
                        freqs = freq_pattern.findall(full_row_text)
                        if not freqs:
                            continue
                        
                        # Classificar tipo de serviço PELA SEMÂNTICA
                        service_type = classify_service_type(full_row_text)
                        
                        # Extrair callsign (nome do órgão) - Busca avançada
                        callsign = ""
                        for col in row:
                            col_str = str(col or "").strip().replace('\n', ' ')
                            # Se contém o nome do órgão (CENTRO, METEORO, etc)
                            if any(kw in col_str.upper() for kw in ['CENTRO', 'CENTER', 'METEORO', 'VOLMET', 'INFORMAÇÃO']):
                                callsign = col_str
                                break
                        
                        # Extrair horário
                        schedule = "H24"
                        if 'DLY' in full_row_text.upper():
                            dly_match = re.search(r'DLY\s*(\d{4})\s*[-–]\s*(\d{4})', full_row_text, re.IGNORECASE)
                            if dly_match:
                                schedule = f"DLY {dly_match.group(1)}-{dly_match.group(2)}"
                        elif 'HJ' in full_row_text.upper():
                            schedule = "HJ"
                        
                        # Extrair idioma
                        language = "POR/ENG"
                        if 'ENG' in full_row_text.upper():
                            language = "POR/ENG" if 'POR' in full_row_text.upper() else "ENG"
                        
                        # Criar um registro para cada frequência encontrada
                        for freq in freqs:
                            # Extrair prioridade a partir do texto ao redor da frequência
                            priority = extract_priority(full_row_text)
                            
                            record = {
                                'fir_id': current_fir,
                                'sector_number': current_sector,
                                'service_type': service_type,
                                'frequency': freq,
                                'priority': priority,
                                'callsign': callsign,
                                'schedule': schedule,
                                'language': language,
                            }
                            
                            # Evitar duplicatas no mesmo lote
                            is_dup = any(
                                r['fir_id'] == record['fir_id'] and
                                r['sector_number'] == record['sector_number'] and
                                r['service_type'] == record['service_type'] and
                                r['frequency'] == record['frequency']
                                for r in all_records
                            )
                            
                            if not is_dup:
                                all_records.append(record)
                                print(f"  → {current_fir} S-{current_sector:04s} | {service_type:7s} | {freq} MHz | {priority:5s} | {schedule}")
            
            print(f"\n📊 Total: {len(all_records)} frequências por setor extraídas")
            
    except Exception as e:
        print(f"Erro ao processar PDF: {e}")
        import traceback
        traceback.print_exc()
    
    return all_records

def save_to_json(records, output_path='sector_frequencies.json'):
    """Salva os registros em JSON local para backup"""
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=4, ensure_ascii=False)
    print(f"\n📦 Backup salvo em {output_path} ({len(records)} registros)")

def upload_to_supabase(records):
    """Envia os registros para a tabela sector_frequencies no Supabase"""
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or os.environ.get('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        print("⚠️ Variáveis SUPABASE_URL/KEY não encontradas. Pulando upload.")
        return
    
    from supabase import create_client
    supabase = create_client(supabase_url, supabase_key)
    
    # Limpar tabela antes de inserir (upsert completo)
    print("\n🗑️ Limpando tabela sector_frequencies...")
    supabase.table('sector_frequencies').delete().neq('id', '00000000-0000-0000-0000-000000000000').execute()
    
    # Inserir em lotes de 50
    batch_size = 50
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        result = supabase.table('sector_frequencies').insert(batch).execute()
        print(f"  ✅ Lote {i//batch_size + 1}: {len(batch)} registros inseridos")
    
    print(f"\n🚀 Upload completo! {len(records)} registros no Supabase.")

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings()
    
    pdf_file = "temp_aip.pdf"
    
    aip_url = get_aip_url()
    
    if not os.path.exists(pdf_file):
        download_pdf(aip_url, pdf_file)
    else:
        print("Arquivo AIP já existe localmente, pulando download.")
    
    # Extrair frequências por setor
    records = extract_sector_frequencies(pdf_file)
    
    # Salvar backup JSON local
    save_to_json(records)
    
    # Upload para Supabase (se variáveis de ambiente disponíveis)
    if os.environ.get('SUPABASE_URL'):
        upload_to_supabase(records)
    else:
        print("\n⚠️ SUPABASE_URL não definida. Execute com variáveis de ambiente para upload automático.")
    
    # COMPATIBILIDADE: Gerar também o aip_frequencies.json antigo para o structural_extractor.mjs
    # Isso garante que o robô antigo continua funcionando
    legacy_data = {}
    for r in records:
        key = f"FIR {r['fir_id']} SECT {r['sector_number']}"
        if key not in legacy_data:
            legacy_data[key] = {'frequencias': [], 'horario': r['schedule'], 'observacoes': ''}
        legacy_data[key]['frequencias'].append(f"{r['frequency']} MHz")
    
    with open('aip_frequencies.json', 'w', encoding='utf-8') as f:
        json.dump(legacy_data, f, indent=4, ensure_ascii=False)
    
    print(f"\n[SUCESSO] Extração por setor concluída! {len(records)} frequências mapeadas.")
