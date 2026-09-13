# 🛰️ SkyFPL — WAC Chart Processor Robot

Robô oficial para processamento paralelo das Cartas Aeronáuticas Mundiais (WAC / World Aeronautical Charts) do DECEA na escala 1:1.000.000, com empacotamento em SQLite MBTiles e sincronização com Cloudflare R2.

## Funcionalidades
- **Varredura Completa:** Suporte a todas as folhas WAC do território brasileiro (WAC 3060 a WAC 3440).
- **Mapeamento Inteligente de BBOX:** Alinhamento geográfico exato com as publicações AISWEB.
- **Telemetria em Tempo Real:** Atualização de progresso consumida pelo Dashboard Admin em `wac_progress.json`.

## Execução Local
```bash
CHART_CODES=ALL python build_wac.py
CHART_CODES=WAC3140,WAC3263 python build_wac.py
```
