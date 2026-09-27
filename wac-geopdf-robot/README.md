# 🚀 SkyFPL WAC High-Definition Engine (GeoPDF / GDAL)

Robô de última geração para processamento de cartas aeronáuticas **WAC (World Aeronautical Charts)** do DECEA a partir dos arquivos mestres **GeoPDF vetoriais**.

---

## 🎯 Por que este motor foi criado?

O robô legado (`wac-robot/`) consome tiles 256x256 do GeoServer DECEA via requisições WMS `GetMap`. Em níveis de zoom reduzidos (Z5 a Z8), o GeoServer aplica downsampling matemático que borra textos, frequências e aerovias.

Este novo motor (`wac-geopdf-robot/`):
1. **Rasterização Vetorial Direta:** Renderiza os arquivos GeoPDF oficiais do AISWEB em **254 a 300 DPI** nativos.
2. **Eliminação Automática de Borda (`NEATLINE`):** O GDAL reconhece a tag `NEATLINE` oficial do DECEA, aplicando máscara Alpha precisa na área útil e descartando bordas brancas e legendas sem cortar dados da carta.
3. **Reamostragem Lanczos:** Interpolação de altíssima definição em todas as pirâmides de zoom.
4. **Compressão Moderna WebP:** Reduz o tamanho final dos MBTiles em até 60% comparado a PNG, com qualidade visual impecável e total suporte a transparência RGBA.
5. **Isolamento Total:** Salva os arquivos de teste no prefixo `wac-test/` do Cloudflare R2, mantendo a produção (`wac/`) 100% segura e inalterada.

---

## 🛠️ Variáveis de Ambiente & Parâmetros

| Variável | Padrão | Descrição |
| :--- | :---: | :--- |
| `CHART_CODES` | `WAC3140` | Códigos separados por vírgula (ex: `WAC3140,WAC3262`) ou `ALL` para todas as 46 folhas. |
| `DPI` | `254` | Resolução de rasterização do GeoPDF (254 ou 300 DPI). |
| `RESAMPLING` | `lanczos` | Algoritmo de interpolação (`lanczos`, `cubic`, `bilinear`). |
| `TILE_FORMAT` | `webp` | Formato dos tiles gravados no MBTiles (`webp` ou `png`). |
| `WEBP_QUALITY` | `85` | Qualidade de compressão do WebP (1 a 100). |
| `MIN_ZOOM` | `5` | Zoom mínimo gerado. |
| `MAX_ZOOM` | `11` | Zoom máximo gerado. |
| `R2_PREFIX` | `wac-test` | Pasta de destino no Cloudflare R2 (mantém isolamento). |
| `PROGRESS_KEY` | `wac_geopdf_progress.json` | Arquivo JSON de progresso lido pelo Dashboard Admin. |

---

## 📦 Como Rodar Localmente (Teste de Bancada)

```bash
# 1. Instalar dependências Python
pip install -r requirements.txt

# 2. Executar teste com uma carta piloto (ex: Brasília)
CHART_CODES=WAC3140 DPI=254 RESAMPLING=lanczos TILE_FORMAT=webp python build_wac_geopdf.py
```

---

## ☁️ Execução no GitHub Actions (Nuvem)

O workflow `.github/workflows/process-wac-geopdf.yml` executa todo o processo em runners Linux no GitHub com GDAL pré-instalado e faz o upload automático dos arquivos `.mbtiles` gerados para o Cloudflare R2.
