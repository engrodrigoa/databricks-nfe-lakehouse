# Guia de estudos — Databricks Data Engineer Associate

Linha do tempo do projeto: cada marco adiciona uma peça e liga um conceito da prova à prática.
Exame vigente (desde 4/mai/2026): **45 questões, 90 min, 7 seções**. Confira sempre o exam guide oficial.

## Linhagem evolutiva

```
MARCO 1  descoberta ───────────────────────────────────────────────────────────────── ✔
  gerador local ─► Volume (UC) ─► spark.read / read_files ─► perguntas (nada gravado)
                                                                    │
MARCO 2  bronze: 3 formas de ingerir ──────────────────────────────────────────────── ✔
  COPY INTO ──────────────┐                                         │
  Auto Loader (xml) ──────┼─► Delta bronze ─► comparação ─► escolha da bronze oficial
  Auto Loader (text) ─────┘   (_metadata, lote)   (anti join com inventário) ─► D010: text
                                                                    │
MARCO 3  silver ──────────────────────────────────────────────────────────────────── ◐
  bronze ─stream─► foreachBatch: from_xml ─► nfe_doc ─► vw_nfe_* (SQL) ─dedup─► MERGE ─► nota·item·pag·dup
                                        └─► quarentena (corrompido, DV, cStat, CNPJ)   vw_dq_alertas (aviso)
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
| **Observado: tipo de `NFe`** | struct (folhas string) | string (XML bruto) | n/a (`xml` = documento inteiro) |
| **Observado: truncados (2 + 3)** | linha toda NULL, sem marca | linha toda NULL, `_rescued_data` vazio | conteúdo preservado; `_corrupt_record` no `from_xml` |
| **Observado: linhas lote_001 / 002 / 003** | 50 / 305 / 503 | 50 / 305 / 503 | 50 / 305 / 503 |
| **Observado: lote_003 carregou só o novo?** | sim | sim (503) | sim (503) |
| **Observado: `_metadata.file_path`** | `dbfs:/Volumes/...` | `/Volumes/...` | `/Volumes/...` |
| **Observado: 2ª execução sem arquivo novo** | _anotar `num_affected_rows` e se criou versão_ | _anotar_ | _anotar_ |
| `operation` no `DESCRIBE HISTORY` | `COPY INTO` | `STREAMING UPDATE` | `STREAMING UPDATE` |

**Conceitos**
- Delta: `_delta_log` (JSON por commit + checkpoints Parquet), ACID, versões
- `DESCRIBE HISTORY`, `DESCRIBE DETAIL`, time travel (`VERSION AS OF`, `TIMESTAMP AS OF`)
- Structured Streaming: `readStream` → `writeStream.toTable`, `checkpointLocation`, exactly-once
- `trigger(availableNow=True)`: batch incremental, processa o pendente e para. É o modo do serverless
- Contar o que um stream escreveu: versão Delta antes × depois + `operationMetrics.numOutputRows`
  (`query.lastProgress` veio `None` no serverless após `availableNow`)
- Tabela **managed** no Free Edition: `DESCRIBE DETAIL` mostra `location` vazio; o UC escolhe e esconde o caminho
- Checagem de completude: inventário (`binaryFile`) `LEFT ANTI JOIN` tabela

**Quando usar o quê (pergunta clássica)**
- Poucos milhares de arquivos, carga em SQL, sem stream → `COPY INTO`
- Volume grande ou contínuo, evolução de schema, notebook/pipeline → Auto Loader
- Dentro de Lakeflow Declarative Pipelines → Auto Loader (`STREAMING TABLE ... read_files`)

**Pegadinhas**
- Apagar a tabela e manter o checkpoint → o stream não recarrega nada
- `COPY INTO` ignora arquivo já carregado **mesmo se ele mudar**
- Glob de pasta: `xml/*/*.xml` desce um nível só (o erro da pasta aninhada no marco 1)
- **Doc × prática:** com rescued data, XML malformado deveria ser "descartado ou gerar erro"; observado:
  entra como linha NULL silenciosa (COPY INTO e Auto Loader xml)
- `_metadata.file_path` muda de formato entre leitores (`dbfs:` ou não): normalizar antes de comparar

**Notas pessoais**
- Os três métodos carregaram tudo e são incrementais/idempotentes; a escolha foi decidida pela **integridade**
  do conteúdo, não pela contagem
- Parse na entrada (xml) esconde o truncado; texto bruto + parse na silver preserva e expõe → **D010**
- Dois controles distintos: **completude** (inventário `binaryFile` × tabela) e **integridade**
  (`_corrupt_record`). "Linha existe" ≠ "documento existe"
- Completude final (02c seção 2): 0 arquivos faltando nos 3 métodos, 858 arquivos em 3 lotes
- `DESCRIBE HISTORY` da `nfe_xml_raw`: cada execução = 1 `STREAMING UPDATE` com `epochId` crescente
  (o epoch é o micro-batch registrado no checkpoint). Conferir `operationMetrics.numOutputRows` das versões 1 e 2
- Lote_003 conferido com o gerador: 500 notas = 497 válidas + 3 truncadas; 503 arquivos = 500 + 3 reenvios

---

## Marco 3 — silver

**Conceitos**
- `from_xml(col, schema, opções)`: parse de string XML com schema explícito; `PERMISSIVE` +
  `columnNameOfCorruptRecord` separa o malformado. `ARRAY<...>` no schema aceita 1 ou N elementos
- Delta como fonte de stream: `readStream.table()` lê só commits novos (fonte append-only)
- `foreachBatch(func)`: cada micro-batch é um DataFrame; permite MERGE e vários destinos
- Escrita idempotente no `foreachBatch`: `txnAppId` + `txnVersion` (= `batch_id`)
- `MERGE` (`DeltaTable.merge` / `MERGE INTO`): `whenMatchedUpdateAll(condition=...)`, `whenNotMatchedInsertAll`.
  A origem **não pode** ter 2 linhas para a mesma chave do destino (erro). Por isso o dedup antes
- Dedup com janela: `row_number() OVER (PARTITION BY chave ORDER BY ... DESC)` e `rn = 1`
- `explode` / `posexplode` / `LATERAL VIEW`: de array para linhas (grão do item)
- Funções de ordem superior: `aggregate`, `transform`, `sequence` (DV módulo 11 e soma de volumes)
- SQL UDF no Unity Catalog: `CREATE FUNCTION cat.schema.fn(...) RETURNS ... RETURN expr`
- Constraints Delta: `NOT NULL` e `CHECK`; violação **falha a escrita inteira**
- Liquid Clustering: `CLUSTER BY (col)` em vez de `PARTITION BY` + `ZORDER`
- CTAS vazio (`AS SELECT * FROM vw WHERE false`): schema da tabela derivado da view

**Pegadinhas**
- Serverless = Spark Connect: sem `cache()`/`persist()`; dentro do `foreachBatch`, use `batch_df.sparkSession`
- ANSI ligado (padrão): `CAST('abc' AS DECIMAL)` dá **erro**; `try_cast` devolve NULL
- `DESCRIBE HISTORY` não é subconsulta em SQL; em Python, `spark.sql("DESCRIBE HISTORY t")` é DataFrame
- `operationMetrics` é `map<string,string>`: `col("operationMetrics")["chave"]` e faça cast
- Data local de `dhEmi` (`-03:00`): `left(dhEmi, 10)`. Convertendo para timestamp, a sessão em UTC pode
  jogar 23h de um dia para o dia seguinte
- Campo XML com nome de palavra reservada (`IS`): crase no DDL e no acesso (`` imposto.`IS`.vIS ``)

**Validado localmente antes da plataforma (Spark 4.0)**
- DV módulo 11 em SQL = gerador em 600 DVs de CNPJ (metade alfanuméricos) + 360 chaves
- Regras de quarentena: 3 casos plantados (DV, cStat, CNPJ) + 6 truncados, 0 falso positivo
- Origem do MERGE sem chave duplicada nas 4 entidades; reenvio mais novo venceu
- Gabarito do gerador: 20/20 anomalias detectadas, 0 falso positivo

**Notas pessoais**
- _registrar aqui o resultado na plataforma (log por lote, métricas do MERGE, gabarito)_

---

## Marcos 4 a 7

_A preencher conforme o projeto avança. Mesmo formato: conceitos · pegadinhas · notas._
