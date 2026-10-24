# Guia de estudos — Databricks Data Engineer Associate

Linha do tempo do projeto: cada marco adiciona uma peça e liga um conceito da prova à prática.
Exame vigente (desde 4/mai/2026): **45 questões, 90 min, 7 seções**. Confira sempre o exam guide oficial.

## Linhagem evolutiva

```
MARCO 1  descoberta ───────────────────────────────────────────────────────────────── ✔
  gerador local ─► Volume (UC) ─► spark.read / read_files ─► perguntas (nada gravado)
                                                                    │
MARCO 2  bronze: 3 formas de ingerir ──────────────────────────────────────────────── ◐
  COPY INTO ──────────────┐                                         │
  Auto Loader (xml) ──────┼─► Delta bronze ─► comparação ─► escolha da bronze oficial
  Auto Loader (text) ─────┘   (_metadata, lote)   (anti join com inventário)
                                                                    │
MARCO 3  silver ─────────────────────────────────────────────────────────────────────
  bronze ─► from_xml(schema do contrato) ─► dedup reenvio ─► MERGE ─► nota·item·pag·dup
                                         └─► quarentena (corrompido, chave inválida)
                                                                    │
MARCO 4  gold ───────────────────────────────────────────────────────────────────────
  silver + referências ─► dimensões ─► ft_nfe_item · ft_nfe_documento ─► ag_ibscbs_mensal
                                                                    │
MARCO 5  Lakeflow Declarative Pipelines ─────────────────────────────────────────────
  o mesmo fluxo como STREAMING TABLE + MATERIALIZED VIEW + EXPECTATIONS
                                                                    │
MARCO 6  operação ───────────────────────────────────────────────────────────────────
  Lakeflow Jobs (DAG, retries, file arrival) · ctrl_lote (D008) · Asset Bundles · CLI
                                                                    │
MARCO 7  consumo e governança ───────────────────────────────────────────────────────
  GRANT/REVOKE · máscara de coluna · row filter · lineage · AI/BI Dashboard · 50k notas
```

## Seções da prova × marcos

| # | Seção do exame | Marcos |
|---|---|---|
| 1 | Databricks Intelligence Platform (arquitetura, Delta, UC, compute) | 1, 2 |
| 2 | Data Ingestion and Loading (COPY INTO, Auto Loader, semi-estruturado) | **2** |
| 3 | Data Transformation and Modeling (limpeza, joins, dedup, gold, qualidade) | 3, 4, 5 |
| 4 | Working with Lakeflow Jobs (DAG, retries, triggers) | 6 |
| 5 | Implementing CI/CD (Git UI, Bundles, variáveis por ambiente, CLI) | 6 |
| 6 | Troubleshooting, Monitoring, Optimization (run history, Spark UI, Liquid Clustering) | 6, 7 |
| 7 | Governance and Security (managed × external, GRANT/DENY, masking, ABAC) | 1, 7 |

---

## Marco 1 — descoberta

**Conceitos**
- Hierarquia do Unity Catalog: `catálogo.schema.objeto` (tabela, view, volume, função)
- **Volume**: arquivos não tabulares governados pelo UC. Caminho `/Volumes/<cat>/<schema>/<vol>/...`
- **Managed** × **external**: no managed, o UC controla o armazenamento e o `DROP` apaga os dados;
  no external, o `DROP` remove só o metadado
- `_metadata`: coluna oculta de qualquer leitura de arquivo (`file_name`, `file_path`, `file_size`,
  `file_modification_time`)
- Git folder: notebooks versionados como `.py`/`.sql` com `# COMMAND ----------`

**Pegadinhas**
- `spark.read` + `inferSchema=false` → **struct** com folhas string
- `read_files` / Auto Loader + `inferColumnTypes=false` → colunas de 1º nível como **STRING bruta** (D007)
- Consultar **só** `_corrupt_record` de arquivo bruto é proibido (`QUERY_ONLY_CORRUPT_RECORD_COLUMN`)
- Inferência de tipos destrói códigos fiscais: `000` → `0`, CEP/CPF perdem zero à esquerda

---

## Marco 2 — bronze: três formas de ingerir

| Critério | `COPY INTO` | Auto Loader (`xml`) | Auto Loader (`text`, `wholeText`) |
|---|---|---|---|
| Interface | SQL | Python/SQL (stream) | Python/SQL (stream) |
| Onde guarda "o que já carreguei" | estado da tabela de destino | checkpoint | checkpoint |
| Reexecutar sem arquivo novo | 0 linhas (idempotente) | 0 linhas | 0 linhas |
| Forçar recarga | `COPY_OPTIONS('force'='true')` | apagar checkpoint | apagar checkpoint |
| Escala indicada | milhares de arquivos | milhões (listagem incremental / notificação) | idem |
| Evolução de schema | `mergeSchema` | `schemaLocation` + `schemaEvolutionMode` | não se aplica (1 coluna) |
| Dados fora do schema | — | `_rescued_data` | — |
| **Observado: tipo de `NFe`** | _anotar_ | _anotar_ | n/a (`xml` = documento inteiro) |
| **Observado: truncados (lote_002 = 2)** | _anotar_ | _anotar_ | _anotar_ |
| **Observado: linhas lote_001 / lote_002** | _anotar_ | _anotar_ | _anotar_ |
| **Observado: lote_003 carregou só o novo?** | _anotar_ | _anotar_ | _anotar_ |
| `operation` no `DESCRIBE HISTORY` | `COPY INTO` | `STREAMING UPDATE` | `STREAMING UPDATE` |

**Conceitos**
- Delta: `_delta_log` (JSON por commit + checkpoints Parquet), ACID, versões
- `DESCRIBE HISTORY`, `DESCRIBE DETAIL`, time travel (`VERSION AS OF`, `TIMESTAMP AS OF`)
- Structured Streaming: `readStream` → `writeStream.toTable`, `checkpointLocation`, exactly-once
- `trigger(availableNow=True)`: batch incremental, processa o pendente e para. É o modo do serverless
- Checagem de completude: inventário (`binaryFile`) `LEFT ANTI JOIN` tabela

**Quando usar o quê (pergunta clássica)**
- Poucos milhares de arquivos, carga em SQL, sem stream → `COPY INTO`
- Volume grande ou contínuo, evolução de schema, notebook/pipeline → Auto Loader
- Dentro de Lakeflow Declarative Pipelines → Auto Loader (`STREAMING TABLE ... read_files`)

**Pegadinhas**
- Apagar a tabela e manter o checkpoint → o stream não recarrega nada
- `COPY INTO` ignora arquivo já carregado **mesmo se ele mudar**
- Glob de pasta: `xml/*/*.xml` desce um nível só (o erro da pasta aninhada no marco 1)

**Notas pessoais**
- _registrar aqui as conclusões da comparação_

---

## Marcos 3 a 7

_A preencher conforme o projeto avança. Mesmo formato: conceitos · pegadinhas · notas._
