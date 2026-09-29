# ✈️ SkyFPL — Motor de Cartas ENRC LOW em Alta Definição (GeoPDF / GDAL) v2.0

Motor autônomo de alta resolução para processamento, fatiamento e empacotamento das Cartas Aeronáuticas de Rota Inferiores (**ENRC LOW - L1 a L9 e Brasil Full**) do DECEA / AISWEB.

---

## 🎯 Diferenciais Técnicos

1. **Resolução de 254, 300 e 600 DPI (Padrão: 600 DPI):**
   * Resolução ultra-HD idêntica à das Cartas WAC atuais. Textos de aerovias, altitudes mínimas de setor (MSA), limites verticais de FIR e frequências nítidos e cristalinos em qualquer nível de zoom (38.21 m/px).
2. **Pirâmides Lanczos (Z5 a Z11):**
   * Interpolação matemática de 3 lóbulos para evitar o aspecto borrado em zooms afastados.
3. **Compressão Nativa WebP RGBA (Qualidade 85):**
   * Reduz em 50% a 60% o peso dos arquivos MBTiles comparado ao PNG legado.
4. **Respeito Rigoroso ao Threshold de 1700 Bytes:**
   * Expurgamento automático de blocos oceânicos vazios ou nulos, prevenindo frestas (*seams*) no aplicativo móvel.
5. **Pipeline Híbrido Unificado:**
   * Gera simultaneamente `.mbtiles` (para SkyFPL Native iOS e Android) e `.pmtiles` v3 (para SkyFPL Web via HTTP Range Requests).
6. **Quarentena e Promoção Server-Side:**
   * Gera artefatos inicialmente em `enrc/staging/` e permite promoção atômica para `enrc/` via `promote_enrc_to_prod.py`.

---

## 🚀 Execução Local ou em Runner

```bash
# Instalar dependências
pip install -r requirements.txt

# Processar carta L2 (Sudeste/Centro)
CHART_CODES=L2 DPI=300 RESAMPLING=lanczos TILE_FORMAT=webp python build_enrc_geopdf.py

# Processar todas as cartas
CHART_CODES=ALL python build_enrc_geopdf.py

# Promover cartas em staging para produção
python promote_enrc_to_prod.py
```

---

## 📦 Estrutura de Arquivos

* `build_enrc_geopdf.py`: Motor de fatiamento GDAL, overviews Lanczos e conversão PMTiles.
* `promote_enrc_to_prod.py`: Script server-side de cópia atômica para produção no Cloudflare R2.
* `enrc_catalog.json`: Metadados, nomes e BBOXes oficiais das 9 cartas ENRC L + Brasil Full.
* `requirements.txt`: Dependências mínimas de execução.
