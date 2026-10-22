# 🧭 ARINC 424 Procedures Robot

Robô especializado na extração e estruturação relacional de **Tabelas de Codificação (Coding Tables)** do DECEA AISWEB conforme a especificação aeronáutica **ARINC 424**.

## 🚀 Funcionalidades
1. **Descoberta DECEA:** Varre a API AISWEB para aeródromos selecionados ou lotes homologados (TOP4, RJSP, NORDESTE).
2. **Decodificação ARINC 424:** Identifica pernas (IF, TF, CF, DF, HM, FA), restrições de altitude, limites de velocidade e órbitas PANS-OPS.
3. **Persistência Relacional:** Alimenta as tabelas procedure_headers, procedure_legs, holding_patterns e enroute_fixes no Supabase.
4. **Compilação Automática para Staging:** Compila automaticamente o pacote JSON canônico procedures_arinc424_staging.json e envia para a Quarentena / Staging no Cloudflare R2 com manifesto 
ext_standby.
5. **Telemetria & Alertas:** Transmite telemetria ao vivo via R2 (procedures/arinc424/telemetry.json) e despacha relatórios ricos e alertas de falha no Telegram.
