# 🏔️ SkyFPL - SVS DEM Robot (Robô de Relevo 3D & Telemetria)

Este robô é o componente responsável pelo processamento, compilação em formato Terrain-RGB e distribuição de malhas topográficas 3D (SRTM/Copernicus 30m) para o sistema de Visão Sintética (SVS / PFD) do SkyFPL.

---

## 🎯 Atribuições do Robô

1. **Codificação Padrão Terrain-RGB:**
   - Codifica cotas de altitude em triplas RGB de 24-bits com precisão de 0.1 metros.
   - Fórmula matemática internacional:
     $$\text{Altitude (m)} = -10000 + ((R \times 65536 + G \times 256 + B) \times 0.1)$$
2. **Empacotamento MBTiles SQLite (TMS):**
   - Gera a pirâmide contínua de zoom (Z0 a Z8) cobrindo 100% das 5 FIRs brasileiras.
   - Converte coordenadas XYZ para padrão TMS (`Y_TMS = 2^zoom - 1 - Y_XYZ`).
3. **Distribuição Soberana no Cloudflare R2:**
   - Publica o artefato em `skyfpl-charts/terrain/relevo_brasil_dem.mbtiles`.
   - Gera o manifesto de integridade com checksum SHA-256 no `skyfpl-brain-vault`.
4. **Alinhamento com TAWS (FAA TSO-C151b):**
   - Calibra as faixas de alerta para evitar falsos alarmes CFIT (Controlled Flight Into Terrain).

---

## 🚀 Execução Local

```bash
pip install -r requirements.txt
python build_svs_dem.py
```

## ⚙️ Variáveis de Ambiente

- `R2_ACCESS_KEY_ID`: Chave de acesso da API do Cloudflare R2
- `R2_SECRET_ACCESS_KEY`: Chave secreta da API do Cloudflare R2
- `R2_ENDPOINT`: Endpoint S3 do Cloudflare R2
- `AIRAC_CYCLE`: Ciclo AIRAC vigente (padrão: `2611`)
- `MIN_ZOOM`: Zoom mínimo (padrão: `0`)
- `MAX_ZOOM`: Zoom máximo (padrão: `8`)

