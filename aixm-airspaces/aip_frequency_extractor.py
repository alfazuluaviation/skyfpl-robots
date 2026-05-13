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

def extract_frequencies(pdf_path):
    print("\nIniciando varredura com pdfplumber...")
    extracted_data = {}
    
    # Regex para frequências VHF aeronáuticas (108.000 a 137.000 MHz)
    freq_pattern = re.compile(r'(1[0-2]\d\.\d{3}|13[0-7]\.\d{3})')
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            print(f"Total de páginas no AIP: {total_pages}")
            
            # Fase 1: Identificar páginas que contêm ENR 2.1 e ENR 2.2
            enr_pages = []
            for i in range(total_pages):
                page = pdf.pages[i]
                text = page.extract_text() or ""
                # Capturar ENR 2.1 (TMA/FIR/CTR) e ENR 2.2 (Outros espaços, incl. ATZ)
                if 'ENR 2.1' in text or 'ENR 2.2' in text:
                    enr_pages.append(i)
            
            print(f"Encontradas {len(enr_pages)} páginas com ENR 2.1/2.2")
            
            if not enr_pages:
                # Fallback: procurar páginas com tabelas que contenham TMA/CTR/FIR/ATZ
                print("Buscando por fallback (páginas com TMA/CTR/FIR/ATZ em tabelas)...")
                for i in range(total_pages):
                    page = pdf.pages[i]
                    text = page.extract_text() or ""
                    if any(kw in text for kw in ['TMA', 'CTR ', 'FIR ', 'APP ', 'ACC ', 'ATZ ']):
                        tables = page.extract_tables()
                        if tables and len(tables) > 0:
                            for table in tables:
                                if table and len(table) > 0:
                                    for row in table:
                                        if row and len(row) >= 4:
                                            col0 = str(row[0] or "")
                                            if any(kw in col0.upper() for kw in ['TMA', 'CTR', 'FIR', 'CTA', 'ATZ']):
                                                enr_pages.append(i)
                                                break
                                    if i in enr_pages:
                                        break
                
                enr_pages = list(set(enr_pages))
                enr_pages.sort()
                print(f"Fallback: Encontradas {len(enr_pages)} páginas candidatas")
            
            # Fase 2: Extrair tabelas das páginas identificadas
            total_extracted = 0
            for page_idx in enr_pages:
                page = pdf.pages[page_idx]
                tables = page.extract_tables()
                
                if not tables:
                    continue
                
                for table in tables:
                    if not table:
                        continue
                    
                    for row in table:
                        if not row or len(row) < 3: # ATZs as vezes tem menos colunas
                            continue
                        
                        col0 = str(row[0]) if row[0] else ""
                        col0_upper = col0.upper()
                        
                        # Filtra apenas linhas que sejam áreas de controle ou zonas de tráfego
                        if not any(kw in col0_upper for kw in ['TMA', 'CTR', 'FIR', 'CTA', 'ATZ']):
                            continue
                        
                        # O nome: pegar a primeira linha da coluna 0 (antes da quebra de linha)
                        name_lines = col0.split('\n')
                        name = name_lines[0].strip().upper()
                        
                        # Se o nome está vazio, tentar as próximas linhas
                        if len(name) < 3 and len(name_lines) > 1:
                            name = name_lines[1].strip().upper()
                        
                        # Se o nome ainda for curto (ex: "ATZ"), tentar concatenar com a segunda linha
                        if len(name) < 5 and len(name_lines) > 1:
                            name = f"{name} {name_lines[1].strip().upper()}"

                        # Coluna de Frequências (geralmente col 3 ou col com MHz)
                        freqs = []
                        for col_idx in range(min(len(row), 6)):
                            col_text = str(row[col_idx]) if row[col_idx] else ""
                            if 'MHZ' in col_text.upper() or 'MHz' in col_text:
                                found = freq_pattern.findall(col_text)
                                freqs.extend(found)
                        
                        # Se não encontrou por 'MHZ', tenta em todas as colunas
                        if not freqs:
                            for col in row:
                                if col:
                                    found = freq_pattern.findall(str(col))
                                    freqs.extend(found)
                        
                        # Observações (geralmente a última coluna significativa)
                        obs = ""
                        if len(row) >= 4:
                            obs = str(row[-1]).replace('\n', ' ').strip()
                        
                        # Horário
                        hours = ""
                        for col in row:
                            col_text = str(col or "").upper()
                            if "H24" in col_text: hours = "H24"; break
                            elif "HJ" in col_text: hours = "HJ"; break
                            elif "OTR" in col_text: hours = "OTR"; break
                        
                        if freqs or obs:
                            # Formatar frequências padronizadas
                            formatted_freqs = []
                            for f in list(set(freqs)):
                                formatted_freqs.append(f"{f} MHz")
                            
                            # Limpar o nome para indexação (Remover TMA, CTR do início para facilitar match)
                            clean_name = name.replace('TMA ', '').replace('CTR ', '').replace('ATZ ', '').strip()

                            print(f"  -> Indexando AIP: {clean_name} ({len(formatted_freqs)} freqs)")
                            
                            # Se já existe essa chave, merge as frequências e obs
                            if clean_name in extracted_data:
                                existing_freqs = extracted_data[clean_name].get('frequencias', [])
                                all_freqs = list(set(existing_freqs + formatted_freqs))
                                extracted_data[clean_name]['frequencias'] = all_freqs
                                if obs and len(obs) > len(extracted_data[clean_name].get('observacoes', '')):
                                    extracted_data[clean_name]['observacoes'] = obs
                            else:
                                extracted_data[clean_name] = {
                                    'frequencias': formatted_freqs,
                                    'horario': hours,
                                    'observacoes': obs
                                }
                            total_extracted += len(formatted_freqs)
            
            print(f"\n📊 Total: {len(extracted_data)} órgãos indexados com {total_extracted} frequências")
                    
    except Exception as e:
        print(f"Erro ao processar PDF: {e}")
        import traceback
        traceback.print_exc()
        
    return extracted_data

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings()
    
    # 1. Definir o caminho temporário
    pdf_file = "temp_aip.pdf"
    
    # 2. Obter URL
    aip_url = get_aip_url()
    
    # 3. Baixar se não existir
    if not os.path.exists(pdf_file):
        download_pdf(aip_url, pdf_file)
    else:
        print("Arquivo AIP já existe localmente, pulando download.")
        
    # 4. Processar e extrair
    data = extract_frequencies(pdf_file)
    
    # 5. Salvar output estruturado
    with open('aip_frequencies.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    # 6. Debug: mostrar chaves do JSON gerado
    print(f"\n📦 Chaves no JSON: {list(data.keys())[:20]}...")
    print(f"\n[SUCESSO] aip_frequencies.json gerado com {len(data)} órgãos!")
