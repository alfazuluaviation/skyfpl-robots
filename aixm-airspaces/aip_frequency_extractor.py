import os
import json
import requests
import pdfplumber
import sys

# Mapeamento do catálogo do DECEA para puxar a seção Completa do AIP
# Usamos a mesma Edge Function que o Dashboard para manter a consistência de autenticação (se houver no futuro)
AISWEB_API = "https://aisweb.decea.gov.br/api" # Mock url or edge function wrapper

def get_aip_url():
    """
    Tenta obter a URL do AIP Brasil atualizado do repositório/API
    Por enquanto usaremos um PDF estático de exemplo para extração.
    """
    # A lógica final usará a mesma rota do DeceaCatalogueService para obter a URL dinâmica
    print("Buscando link oficial do AIP Brasil (ENR 2.1)...")
    return "https://aisweb.decea.gov.br/download/?public=60b181e0-2a0e-4728-9b562fa4ff340fb0.pdf"

def download_pdf(url, output_path):
    print(f"Iniciando download do AIP ({url})...")
    # Ignorando verificacao de SSL caso necessario, e tratando redirecionamentos
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
                # Print progress
                done = int(50 * downloaded / (total_size if total_size > 0 else 100000000))
                sys.stdout.write(f"\r[{'=' * done}{' ' * (50-done)}] {downloaded//1024} KB")
                sys.stdout.flush()
    print("\nDownload concluído.")

def extract_frequencies(pdf_path):
    print("\\nIniciando varredura com pdfplumber...")
    extracted_data = {}
    import re
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            print(f"Total de páginas no AIP: {total_pages}")
            
            for i in range(min(total_pages, 500)):
                page = pdf.pages[i]
                text = page.extract_text()
                if text and 'ENR 2.1' in text:
                    tables = page.extract_tables()
                    for table in tables:
                        if not table: continue
                        for row in table:
                            if not row or len(row) < 4: continue
                            
                            col0 = str(row[0]) if row[0] else ""
                            # Filtra apenas linhas que sejam áreas de controle
                            if 'TMA' in col0 or 'CTR' in col0 or 'FIR' in col0 or 'CTA' in col0:
                                # O nome geralmente fica na primeira linha da coluna 0
                                name = col0.split('\\n')[0].strip().upper()
                                
                                freqs_raw = str(row[3]) if len(row) > 3 and row[3] else ""
                                freqs = re.findall(r'1\\d{2}\\.\\d{3}', freqs_raw)
                                
                                obs = str(row[4]).replace('\\n', ' ').strip() if len(row) > 4 and row[4] else ""
                                hours_col = str(row[2]) if len(row) > 2 and row[2] else ""
                                hours = "H24" if "H24" in hours_col else ""
                                
                                if freqs:
                                    print(f"  -> Indexando AIP: {name} ({len(freqs)} freqs)")
                                    extracted_data[name] = {
                                        'frequencias': freqs,
                                        'horario': hours,
                                        'observacoes': obs
                                    }
    except Exception as e:
        print(f"Erro ao processar PDF: {e}")
        
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
        
    print("\n[SUCESSO] aip_frequencies.json gerado com sucesso!")
