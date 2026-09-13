# 🛰️ SkyFPL — ENRC Chart Processor Robot (LOW & HIGH)

Robô oficial para geração, download e empacotamento das Cartas de Rota ENRC (Inferior e Superior) do DECEA em formato SQLite MBTiles de alto desempenho com upload automático para o Cloudflare R2.

## Funcionalidades
- **Suporte Duplo:** Processamento de cartas de Rota Inferior (`LOW` / L1 a L8) e Superior (`HIGH` / H1 a H4).
- **Modos Flexíveis:** Geração individual por folha ou arquivo consolidado (`Single File / Brasil Full`).
- **Telemetria e Progresso:** Atualização contínua de status em `enrcl_progress.json` e `enrch_progress.json` consumidos pelo Admin Dashboard.

## Execução Local
```bash
LAYER_TYPE=LOW CHART_CODES=ALL python build_enrc.py
LAYER_TYPE=HIGH CHART_CODES=H1,H2 python build_enrc.py
```
