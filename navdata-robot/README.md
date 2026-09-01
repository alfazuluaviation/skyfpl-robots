# 🛰️ SkyFPL — Robô de Dados de Navegação (NavData)

Robô oficial responsável pela extração, validação, georreferenciamento e consolidação de toda a malha aeronáutica brasileira (GeoServer DECEA / ICA) para o ecossistema SkyFPL.

---

## 📁 Estrutura de Arquivos

| Arquivo | Descrição |
|---|---|
| `sync_navdata.py` | Script principal em Python que executa a contagem, download paginado via Proxy Supabase, cálculo de centróides, deduplicação, upload duplo no Cloudflare R2 e chamada de webhook de staging. |
| `calendar.json` | Calendário oficial de ciclos AIRAC (28 dias) para cálculo sincronizado de vigência, publicação e expiração. |
| `requirements.txt` | Dependências Python necessárias para execução (`requests`, `boto3`). |
| `README.md` | Este manual de documentação técnica e operacional. |

---

## 🏗️ Arquitetura de Processamento

```
1. Início (Cron Diário às 03:00 UTC ou Disparo Manual no Dashboard)
   │
2. Verificação AIRAC (Lê calendar.json -> identifica ciclo e janela de D-14)
   │
3. Contagem Rápida (resultType=hits no GeoServer DECEA)
   │
4. Download Paginado (1000 features/página via Proxy Edge Function Supabase)
   ├─ ICA:airport   (Aeroportos públicos e privados)
   ├─ ICA:heliport  (Helipontos homologados)
   ├─ ICA:vor       (Estações VOR com frequências)
   ├─ ICA:ndb       (Radiofaróis NDB)
   ├─ ICA:waypoint  (Fixos de navegação e RNAV)
   └─ ICA:runway    (Pistas com centróide geométrico — apenas auditoria interna)
   │
5. Processamento & Normalização
   ├─ Limpeza e formatação de coordenadas geográficas
   ├─ Filtragem de dados e deduplicação de identificadores únicos
   └─ Atualização contínua de telemetria em tempo real (`navdata/telemetry.json`)
   │
6. Publicação no Cloudflare R2 (Bucket: skyfpl-charts)
   ├─ 📦 Versionado: `navdata/cycles/{cycle}/navdata_{cycle}.json` (Staging/Histórico)
   └─ 🚀 Produção: `latest_navdata.json` (Consumido pelo App móvel)
   │
7. Webhook & Staging (Supabase Edge Function `airac-navdata-ingest`)
   ├─ Auditoria de integridade e contagem por camada
   ├─ Cálculo de Diff automático (Adicionados, Removidos, Modificados)
   └─ Disparo de Notificações para Administrador (Telegram / WhatsApp)
```

---

## ⚙️ Variáveis de Ambiente (Secrets)

- `R2_ACCESS_KEY_ID` / `CLOUDFLARE_R2_ACCESS_KEY_ID`: Chave de acesso S3 ao Cloudflare R2.
- `R2_SECRET_ACCESS_KEY` / `CLOUDFLARE_R2_SECRET_ACCESS_KEY`: Chave secreta S3 do R2.
- `R2_ENDPOINT` / `CLOUDFLARE_R2_ENDPOINT`: Endpoint S3 do Cloudflare R2.
- `SUPABASE_URL`: URL do projeto Supabase.
- `SUPABASE_SERVICE_ROLE_KEY`: Chave de serviço para autenticar na Edge Function de ingestão.
- `SUPABASE_ANON_KEY`: Chave pública para utilização do proxy de geodados.

---

## 🛡️ Resiliência e Self-Healing

O workflow do GitHub Actions possui autorrecuperação automática: em caso de instabilidade temporária no GeoServer do DECEA ou cancelamento de sessão, o workflow detecta a falha e re-dispara a execução automaticamente.
