# Airway Navigation Robot

Este robô extrai dados de aerovias (Alta e Baixa) do DECEA via WFS (Web Feature Service) e gera um banco de dados SQLite otimizado para o aplicativo SkyFPL.

## Funcionamento (Pipeline Unificado & Cloud Guardrail — Fase 7)

1.  Resolve o ciclo AIRAC vigente/D-14 (`resolve_airac_cycle`), consulta `airways_telemetry.json` prévio no R2 e realiza auto-incremento de versão (`<AIRAC>.<N>`).
2.  Consome as camadas WFS do DECEA (`ICA:vw_aerovia_baixa_v2`, `ICA:vw_aerovia_alta_v2`) e as planilhas de Rotas Preferenciais (Playbook DECEA / `openpyxl`).
3.  Gera o banco unificado `airways_v1.db` com tabela `app_metadata`, executa `VACUUM` + `PRAGMA integrity_check` e calcula checksum SHA-256 + manifesto `airways_telemetry.json`.
4.  Executa a trava atômica `validate_cloud_guardrail()` (exige `PRAGMA integrity_check == 'ok'`, `airway_segments >= 3500` e `preferred_routes (em_vigor=1) >= 300`), gravando `guardrail_report.json` e abortando com `sys.exit(2)` em caso de falha sem sobrescrever produção.
5.  Realiza upload atômico para o Cloudflare R2 e reporta telemetria ao vivo (`robot-airways-playbook`), mensagens em `#airac-audits` / `#data-integrity` e tarefas em `agent_tasks` via `MASTER_BRAIN_URL` (`dov-master-agent`).

## Agendamento e Disparo (`.github/workflows/build-airways.yml`)

- **Cron semanal:** `'0 4 * * 4'` (toda quinta-feira às 04:00 UTC).
- **Disparo remoto Master Supervisor (`repository_dispatch`):** evento `trigger-airways-sync` com inputs opcionais (`airac_cycle`, `dry_run`, `requested_by`).
- **Disparo manual (`workflow_dispatch`):** suporta `airac_cycle`, `package_version` e `dry_run`.

## Instalação

```bash
pip install -r requirements.txt
```

## Execução

```bash
# Variáveis de ambiente necessárias:
# R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, SUPABASE_ANON_KEY

python build_airways.py
```
