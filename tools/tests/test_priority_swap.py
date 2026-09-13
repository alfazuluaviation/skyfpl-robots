import requests
import os

WMS_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms"

# Ordem A: Atual (2 no topo)
layers_a = "ICA:ENRC_L9,ICA:ENRC_L8,ICA:ENRC_L7,ICA:ENRC_L4,ICA:ENRC_L6,ICA:ENRC_L5,ICA:ENRC_L3,ICA:ENRC_L1,ICA:ENRC_L2"
# Ordem B: Proposta (3 no topo)
layers_b = "ICA:ENRC_L9,ICA:ENRC_L8,ICA:ENRC_L7,ICA:ENRC_L4,ICA:ENRC_L6,ICA:ENRC_L5,ICA:ENRC_L1,ICA:ENRC_L2,ICA:ENRC_L3"

# Pontos de Teste
test_points = {
    "VOLMET_AREA": "-41.0,-14.0,-39.0,-12.0", # Onde o quadro VOLMET costuma ficar
    "TRES_MARIAS": "-46.5,-19.5,-44.5,-17.5"  # Região de Três Marias (MG)
}

print(f"🧪 Iniciando Comparação de Prioridade (Swap 2 vs 3)...")

for pt_name, bbox in test_points.items():
    for order_name, layers in [("A_2_TOP", layers_a), ("B_3_TOP", layers_b)]:
        print(f"   - Testando {pt_name} com Ordem {order_name}...")
        params = {
            "SERVICE": "WMS",
            "VERSION": "1.1.1",
            "REQUEST": "GetMap",
            "LAYERS": layers,
            "STYLES": "",
            "BBOX": bbox,
            "WIDTH": "1024",
            "HEIGHT": "1024",
            "SRS": "EPSG:4326",
            "FORMAT": "image/png",
            "TRANSPARENT": "TRUE"
        }
        try:
            response = requests.get(WMS_URL, params=params, timeout=45)
            if response.status_code == 200:
                filename = f"scripts/audit_{pt_name}_{order_name}.png"
                with open(filename, "wb") as f:
                    f.write(response.content)
            else:
                print(f"      ❌ Erro {response.status_code}")
        except Exception as e:
            print(f"      🔥 Falha: {e}")

print("\n🏁 Comparação concluída.")
