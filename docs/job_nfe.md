# Job `nfe_job`: criar pela UI (marco 6a) e depois por código (marco 6b)

```
               ┌► bronze ─► tem_arquivo_novo? ──true──► silver ─► gold ─┐
 file arrival ─┤                                                         ├─► reconcilia ─► ctrl_lote
               └► pipeline nfe_ldp ──────────────────────────────────────┘     (ALL_DONE)
```

## 6a · Pela UI (aprender a tela)

**Jobs & Pipelines → Create → Job**, nome `nfe_job`. Compute: deixe **Serverless** em todas as tasks.
Caminhos: os notebooks ficam em `src/06_jobs/` do Git folder.

| # | Task (Task name) | Type | Origem | Depends on | Ajustes |
|---|---|---|---|---|---|
| 1 | `bronze` | Notebook | `j01_bronze` | — | Retries: 1 (intervalo 1 min) |
| 2 | `tem_arquivo_novo` | **If/else condition** | `{{tasks.bronze.values.arquivos_novos}}` `>` `0` | bronze | — |
| 3 | `silver` | Notebook | `j02_silver` | tem_arquivo_novo (**true**) | Retries: 1 |
| 4 | `gold` | Notebook | `j03_gold` | silver | — |
| 5 | `pipeline` | **Pipeline** | `nfe_ldp` | — (raiz: roda em paralelo) | — |
| 6 | `reconcilia` | Notebook | `j04_reconcilia` | gold, pipeline | — |
| 7 | `ctrl_lote` | Notebook | `j05_ctrl_lote` | bronze, silver, gold, pipeline, reconcilia | **Run if: All done** · Parameter `run_id` = `{{job.run_id}}` |

> ⚠️ Testado: se a `ctrl_lote` depender só de `reconcilia`, ela é **excluída** junto com o ramo falso, mesmo com
> *All done*. Ela precisa de pelo menos uma dependência que sempre executa (bronze, pipeline).
> Não coloque a condição `tem_arquivo_novo` como dependência: a API exige `outcome` (true/false) em toda
> dependência de If/else, e a `ctrl_lote` precisa rodar nos dois casos (erro visto no `bundle deploy`).

**Job details (painel direito):**
- **Max concurrent runs: 1**: duas execuções simultâneas brigariam pelos mesmos checkpoints
- **Trigger → File arrival**: `/Volumes/workspace/nfe_bronze/landing/xml/` · em **Advanced**:
  *Wait after last change* 120 s (espera o upload do lote inteiro) e *Minimum time between triggers* 60 s.
  Sem isso, um upload longo gera disparo extra (testado). Deixe **Paused** até o primeiro teste manual dar certo

### Testes (nesta ordem)

1. **Run now** sem lote novo → bronze ✔ (0 arquivos) → condição **false** → silver, gold e reconcilia
   **Excluded/Skipped** → pipeline ✔ → ctrl_lote ✔ com status `SEM_ARQUIVO_NOVO`
2. Gerar `lote_005`, subir os arquivos, **Run now** → todas ✔ → ctrl_lote com `OK`
3. **Unpause** o gatilho, gerar `lote_006`, subir: o Job dispara sozinho ~2 min depois do upload

Em cada run: clique numa task para ver saída, duração, tentativas e o task value.

### O que observar (prova, seção 4)

| Conceito | Onde aparece |
|---|---|
| DAG e dependências | grafo da run; tasks sem dependência rodam em paralelo |
| Task types | Notebook, Pipeline, If/else condition |
| Task values | `dbutils.jobs.taskValues.set/get`; referência `{{tasks.<task>.values.<chave>}}` |
| Run if | `All succeeded` (padrão) × `All done` × `At least one failed`... |
| Retries | tentativa 2 aparece na run com o motivo da 1ª falha |
| Triggers | scheduled (cron) × **file arrival** × table update × continuous |
| Parâmetros dinâmicos | `{{job.run_id}}`, `{{job.start_time.iso_date}}`... |
| Repair run | numa run com falha: **Repair run** reexecuta só as tasks que falharam e as dependentes |

## 6b · Por código (Asset Bundle)

O mesmo Job está em `resources/nfe_job.yml`; o projeto em `databricks.yml`. O pipeline `nfe_ldp` é
**referenciado** (lookup por nome), não recriado: o bundle não toma posse do que foi criado na UI.

```powershell
winget install Databricks.DatabricksCLI          # uma vez
databricks auth login --host https://<workspace>.cloud.databricks.com   # perfil local (OAuth)
$env:DATABRICKS_CONFIG_PROFILE = "free"          # perfil padrão da sessão
cd D:\lambda4\nfe
databricks bundle validate                       # YAML, caminhos, variáveis (local)
databricks bundle deploy                         # target dev: "[dev <usuário>] nfe_job", gatilho pausado
databricks bundle summary                        # o que o bundle gerencia
databricks bundle run nfe_job                    # dispara e acompanha no terminal
```

### Targets

| | `dev` (padrão) | `prod` |
|---|---|---|
| Nome do Job | `[dev <usuário>] nfe_job` | `nfe_job` |
| Gatilho | pausado | **UNPAUSED** (variável `pausa_gatilho`) |
| Pasta implantada | `~/.bundle/<bundle>/dev` | `/Workspace/Shared/.bundle/<bundle>/prod` |
| Exigências | nenhuma | `run_as` + `permissions` explícitos (sem service principal) |

### Virada para uma fonte só

```powershell
databricks bundle deploy -t prod     # cria "nfe_job" gerenciado pelo bundle, gatilho ligado
# UI: apagar o nfe_job criado à mão (⋮ → Delete job) depois de conferir o novo
databricks bundle destroy -t dev     # remove a cópia [dev] (pede confirmação)
```

### CI no GitHub Actions (`.github/workflows/bundle.yml`)

- **push/PR** → `bundle validate` em dev e prod: YAML quebrado aparece no PR
- **Run workflow (manual)** → validate + `bundle deploy -t prod`
- Secrets necessários: `DATABRICKS_HOST` (URL do workspace) e `DATABRICKS_TOKEN`
  (User settings → Developer → Access tokens → Generate; validade curta, ex.: 90 dias)
