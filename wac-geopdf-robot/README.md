# 🚀 SkyFPL WAC High-Definition Engine (GeoPDF / GDAL)

Robô de última geração para processamento de cartas aeronáuticas **WAC (World Aeronautical Charts)** do DECEA a partir dos arquivos mestres **GeoPDF vetoriais**.

---

## 🎯 Por que este motor foi criado?

O robô legado (`wac-robot/`) consumia tiles 256x256 do GeoServer DECEA via requisições WMS `GetMap`. Em níveis de zoom reduzidos (Z5 a Z8), o GeoServer aplicava downsampling matemático que borrava textos, frequências e aerovias.

Este novo motor (`wac-geopdf-robot/`):
1. **Rasterização Vetorial Direta:** Renderiza os arquivos GeoPDF oficiais do AISWEB em **300 a 600 DPI** nativos.
2. **Recorte Geográfico Cirúrgico:** Aplica o BBOX oficial com `-te minLon minLat maxLon maxLat -te_srs EPSG:4326`, descartando 100% de bordas brancas e molduras de papel.
3. **Reamostragem Lanczos:** Interpolação de altíssima definição em todas as pirâmides de zoom (Z5 a Z12).
4. **Compressão Moderna WebP:** Reduz o tamanho final dos MBTiles em até 60% comparado a PNG, com qualidade visual impecável e total suporte a transparência RGBA.
5. **Conversão Nativa para PMTiles (0.4s):** Gera instantaneamente a versão `.pmtiles` para o site Web via Protomaps Go CLI.
6. **Upload Híbrido Duplo:** Envia para o Cloudflare R2 (`wac/staging/`):
   * `WAC{code}_HD.mbtiles` para o SkyFPL Native (iOS e Android).
   * `WAC{code}.pmtiles` para o SkyFPL Web (HTTP Range Requests).
7. **Telemetria ao Vivo:** Registra em streaming no R2 (`wac_geopdf_progress.json`) os logs de terminal e a porcentagem em tempo real para o Dashboard Admin.

---

## 🛠️ Variáveis de Ambiente & Parâmetros

| Variável | Padrão | Descrição |
| :--- | :---: | :--- |
| `CHART_CODES` | `(vazio)` | Códigos separados por vírgula (ex: `WAC3141` ou `WAC3141,WAC3262`) ou `ALL` para todas as 46 folhas. |
| `DPI` | `600` | Resolução de rasterização do GeoPDF (300, 450 ou 600 DPI). |
| `RESAMPLING` | `cubic` | Algoritmo de interpolação no GDAL (`cubic`, `lanczos`, `bilinear`). |
| `TILE_FORMAT` | `webp` | Formato dos tiles gravados (`webp` ou `png`). |
| `WEBP_QUALITY` | `85` | Qualidade de compressão do WebP (1 a 100). |
| `MIN_ZOOM` | `5` | Zoom mínimo gerado. |
| `MAX_ZOOM` | `12` | Zoom máximo gerado. |
| `R2_PREFIX` | `wac/staging` | Pasta de destino no Cloudflare R2 (mantém quarentena). |
| `PROGRESS_KEY` | `wac_geopdf_progress.json` | Arquivo JSON de progresso lido pelo Dashboard Admin. |

---

## 📦 Como Rodar Localmente (Teste de Bancada)

```bash
# 1. Instalar dependências Python e CLI pmtiles
pip install -r requirements.txt

# 2. Executar teste com uma carta piloto (ex: Salvador)
CHART_CODES=WAC3141 DPI=600 RESAMPLING=cubic TILE_FORMAT=webp python build_wac_geopdf.py
```

---

## ☁️ Execução no GitHub Actions (Nuvem)

O workflow `.github/workflows/process-wac-geopdf.yml` executa todo o processo em runners Linux no GitHub com GDAL pré-instalado e faz o upload automático dos arquivos `.mbtiles` gerados para o Cloudflare R2.
