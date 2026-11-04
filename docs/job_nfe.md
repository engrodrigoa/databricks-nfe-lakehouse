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
databricks auth login                            # abre o navegador; informe a URL do workspace
cd D:\lambda4\nfe
databricks bundle validate                       # checa YAML, caminhos e variáveis
databricks bundle deploy                         # target dev: cria "[dev <usuário>] nfe_job", gatilho pausado
databricks bundle run nfe_job                    # dispara uma execução
```

Compare o Job criado pela UI com o do bundle: na página do Job, **⋮ → View as code / YAML**.
Depois, apague o da UI e fique só com o do bundle (o Git vira a fonte da verdade).
