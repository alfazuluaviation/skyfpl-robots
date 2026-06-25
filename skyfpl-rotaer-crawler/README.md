# skyfpl-rotaer-crawler

Robô autônomo de sincronização do **ROTAER** (Roteiro Aeronáutico Brasileiro) para o ecossistema SkyFPL.

## Como Funciona

Este script é executado automaticamente pelo **GitHub Actions** todo dia às 02:00 UTC. Internamente, ele verifica se hoje é um dos 2 dias antes de um novo **Ciclo AIRAC** entrar em vigor. Se não for a hora certa, ele encerra imediatamente (custo zero). Se for a janela correta, ele inicia a raspagem completa.

## Fluxo de Execução

```
[GitHub Actions - Todo dia 02:00 UTC]
         │
         ▼
[Verificação AIRAC]
  ├─ Lê calendar.json (Calendário Mestre Oficial do SkyFPL)
  ├─ Calcula quantos dias faltam para o próximo ciclo
  └─ Faltam ≤ 2 dias? 
       ├─ NÃO → Encerra (sem custo)
       └─ SIM → Continua ↓

[Busca Aeródromos]
  └─ Supabase: tabela `aerodromes` (4405 AD + 1596 HP)

[Raspagem em Batch]
  └─ Chama Edge Function `fetch-rotaer` (3s de delay entre chamadas)
  └─ Cada chamada extrai: Header, Pistas, COM, RDONAV, RFFS, Obstáculos, METAR, TAF, NOTAMs

[Upload para Cloudflare R2]
  ├─ rotaer/rotaer_{CICLO}_snapshot.json  (versionado por ciclo)
  └─ rotaer/rotaer_snapshot_latest.json   (sempre aponta para o último ciclo)
```

## Arquivos

| Arquivo | Descrição |
|---|---|
| `index.js` | Cérebro do robô (inteligência AIRAC + raspagem + upload R2) |
| `calendar.json` | Calendário AIRAC oficial (cópia de `check-airac-cycle/calendar.json`) |
| `package.json` | Dependências Node.js |

## Dependências

- `@supabase/supabase-js` — Acesso ao banco de aeródromos e Edge Functions
- `@aws-sdk/client-s3` — Upload para Cloudflare R2 (API S3 compatível)
- `dotenv` — Carregamento de variáveis de ambiente locais (apenas em dev)

## Secrets Necessários (GitHub)

Já configuradas no repositório `skyfpl-robots`:

| Secret | Descrição |
|---|---|
| `SUPABASE_URL` | URL do projeto Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | Service Role Key (ignora RLS) |
| `R2_ACCESS_KEY_ID` | ID da chave de acesso do Cloudflare R2 |
| `R2_SECRET_ACCESS_KEY` | Chave secreta do Cloudflare R2 |
| `R2_ENDPOINT` | Endpoint S3 do Cloudflare R2 |

## Output (Cloudflare R2)

Os dados ficam disponíveis via CDN em:
- `https://cartas.skyfpl.com/rotaer/rotaer_snapshot_latest.json` (sempre atualizado)
- `https://cartas.skyfpl.com/rotaer/rotaer_{CICLO}_snapshot.json` (versionado)

## Ciclos AIRAC Programados (2026)

| Ciclo | Efetividade | Robô Roda Em |
|---|---|---|
| 2606 | 11/06/2026 | ✅ Passou |
| **2607** | **09/07/2026** | **07/07/2026** ← Próxima execução |
| 2608 | 06/08/2026 | 04/08/2026 |
| 2609 | 03/09/2026 | 01/09/2026 |
| 2610 | 01/10/2026 | 29/09/2026 |
| 2611 | 29/10/2026 | 27/10/2026 |
| 2612 | 26/11/2026 | 24/11/2026 |
| 2613 | 24/12/2026 | 22/12/2026 |
