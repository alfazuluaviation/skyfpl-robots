# 🤖 SkyFPL — AIP Master Auditor Robot

Robô Centralizador de Inteligência e Reconciliação Documental do DECEA (AISWeb).

## 📌 Funcionalidades
1. Consulta a API oficial do AISWeb (`area=publicacoes&classe=amdt` e `classe=aic`).
2. Identifica com precisão de 100% (Tripla Trava) a emenda oficial do ciclo AIRAC vigente ou futuro (D-14).
3. Extrai e segmenta todas as seções: Aeródromos/Helipontos, Waypoints RNAV, VOR/NDB, Cartas IFR e Aerovias.
4. Publica a base consolidada no Cloudflare R2: `aip/cycles/{cycle}/aip_amdt_{cycle}.json`.
5. Aciona o Webhook de Homologação no Supabase (`airac-aip-ingest`) para alimentar o Painel Central 360° e os módulos individuais.
