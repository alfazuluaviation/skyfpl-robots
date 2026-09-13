import csv
import json

csv_file = "tabela-de-categorias-de-homologacao-por-modelo-de-aeronave.csv"
json_file = "src/assets/data/aircraft_models.json"

aircraft_models = []

try:
    with open(csv_file, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            # Normalizar os campos para camelCase para o JS
            model = {
                "id": row.get('#', ''),
                "model": row.get('MODELO', ''),
                "manufacturerCode": row.get('CÓD FABRICANT', ''),
                "manufacturer": row.get('NOME FABRICANTE', ''),
                "class": row.get('CLASSE', ''),
                "category": row.get('Categoria de Homologação', '')
            }
            aircraft_models.append(model)
            
    with open(json_file, mode='w', encoding='utf-8') as f:
        json.dump(aircraft_models, f, ensure_ascii=False, indent=2)
        
    print(f"✅ Sucesso: {len(aircraft_models)} modelos convertidos para {json_file}")

except Exception as e:
    print(f"❌ Erro ao converter CSV: {e}")
