# Databricks notebook source
# MAGIC %md
# MAGIC # j05 · Job: ctrl_lote (D008)
# MAGIC Roda **sempre** (`run_if: ALL_DONE`): registra também as execuções com falha, que são as que mais importam.
# MAGIC Uma linha por lote por execução = histórico. Completude (Volume × bronze) e integridade (quarentena) juntas.
# MAGIC
# MAGIC Task values de tasks que não rodaram (falha ou condição falsa) voltam como `default` (-1).
# MAGIC
# MAGIC Marco 7: recebe `inicio_ms` (`{{job.start_time.timestamp_ms}}`) e grava `inicio_run`: duração = `executado_em − inicio_run`.
# MAGIC Fronteiras vigiadas (D019): origem → Volume (`_lote.json` × inventário) e nfe_doc → nfe_nota (`chaves_sem_nota`, global).

# COMMAND ----------

dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("inicio_ms", "")
run_id = dbutils.widgets.get("run_id")
inicio_ms = dbutils.widgets.get("inicio_ms").strip()
INICIO = f"timestamp_millis({int(inicio_ms)})" if inicio_ms else "CAST(NULL AS TIMESTAMP)"

arquivos_novos = dbutils.jobs.taskValues.get(taskKey="bronze", key="arquivos_novos", default=-1, debugValue=-1)
diferencas = dbutils.jobs.taskValues.get(taskKey="reconcilia", key="diferencas", default=-1, debugValue=-1)
print(f"run_id={run_id} arquivos_novos={arquivos_novos} diferencas={diferencas}")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.nfe_bronze.ctrl_lote (
  run_id                 STRING    COMMENT 'job run id ({{job.run_id}}) ou manual',
  executado_em           TIMESTAMP,
  lote                   STRING,
  arquivos_volume        BIGINT    COMMENT 'inventário do Volume (fonte da verdade)',
  arquivos_bronze        BIGINT,
  faltando_na_bronze     BIGINT    COMMENT 'completude',
  quarentena             BIGINT    COMMENT 'integridade',
  docs_validos           BIGINT,
  reenvios               BIGINT,
  notas_vigentes         BIGINT,
  arquivos_novos_execucao BIGINT   COMMENT 'task value da bronze; -1 = não informado',
  diferencas_nb_x_ldp    BIGINT    COMMENT 'task value da reconciliação; -1 = não rodou',
  status                 STRING
)
COMMENT 'Log de carga por lote e por execução do Job (D008)'
""")

# Colunas acrescentadas no marco 7. Tabela já existe com dados: entram por ALTER (linhas antigas ficam NULL)
NOVAS = {
    "inicio_run":         "TIMESTAMP COMMENT 'início da run ({{job.start_time}}): duração = executado_em - inicio_run'",
    "arquivos_esperados": "BIGINT COMMENT 'declarado pela origem no _lote.json (NULL = lote sem arquivo de controle)'",
    "chaves_sem_nota":    "BIGINT COMMENT 'global: chaves válidas no nfe_doc que não chegaram à nfe_nota'",
}
existentes = spark.table("workspace.nfe_bronze.ctrl_lote").columns
for coluna, tipo in NOVAS.items():
    if coluna not in existentes:
        spark.sql(f"ALTER TABLE workspace.nfe_bronze.ctrl_lote ADD COLUMN {coluna} {tipo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquivo de controle (D019)
# MAGIC A origem grava `_lote.json` com a quantidade de arquivos enviados. Lotes antigos não têm: ficam sem a
# MAGIC conferência (NULL), sem alarme. Só a ausência de **qualquer** `_lote.json` é tratada; outro erro de leitura sobe.

# COMMAND ----------

from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

try:
    (spark.read.json("/Volumes/workspace/nfe_bronze/landing/xml/*/_lote.json")
     .select(F.regexp_extract(F.col("_metadata.file_path"), r"/(lote_[^/]+)/", 1).alias("lote"),
             F.col("arquivos").cast("bigint").alias("arquivos_esperados"))
     .createOrReplaceTempView("esperado"))
except AnalysisException as e:
    if "PATH_NOT_FOUND" not in str(e) and "UNABLE_TO_INFER_SCHEMA" not in str(e):
        raise
    print("nenhum _lote.json no Volume ainda")
    spark.sql("CREATE OR REPLACE TEMP VIEW esperado AS "
              "SELECT CAST(NULL AS STRING) AS lote, CAST(NULL AS BIGINT) AS arquivos_esperados WHERE false")

# global: a nota pode pertencer ao lote de um reenvio posterior, então a conferência nfe_doc → nfe_nota é por chave
chaves_sem_nota = spark.sql("""
  SELECT count(DISTINCT d.chave_acesso)
  FROM workspace.nfe_silver.nfe_doc d LEFT ANTI JOIN workspace.nfe_silver.nfe_nota n USING (chave_acesso)
""").first()[0]
print("chaves_sem_nota:", chaves_sem_nota)

# COMMAND ----------

spark.sql(f"""
INSERT INTO workspace.nfe_bronze.ctrl_lote
  (run_id, executado_em, lote, arquivos_volume, arquivos_bronze, faltando_na_bronze, quarentena, docs_validos,
   reenvios, notas_vigentes, arquivos_novos_execucao, diferencas_nb_x_ldp, status, inicio_run,
   arquivos_esperados, chaves_sem_nota)
WITH inv AS (
  SELECT regexp_extract(path, '/(lote_[^/]+)/', 1) AS lote, count(*) AS arquivos_volume
  FROM read_files('/Volumes/workspace/nfe_bronze/landing/xml/*/*.xml', format => 'binaryFile')
  GROUP BY 1),
b AS (SELECT lote, count(*) AS arquivos_bronze FROM workspace.nfe_bronze.nfe_xml_raw GROUP BY lote),
q AS (SELECT lote, count(*) AS quarentena FROM workspace.nfe_silver.nfe_quarentena GROUP BY lote),
d AS (SELECT lote, count(*) AS docs_validos, count(DISTINCT chave_acesso) AS notas_distintas
      FROM workspace.nfe_silver.nfe_doc GROUP BY lote),
n AS (SELECT lote, count(*) AS notas_vigentes FROM workspace.nfe_silver.nfe_nota GROUP BY lote),
base AS (
  SELECT inv.lote, inv.arquivos_volume, e.arquivos_esperados,
         coalesce(b.arquivos_bronze, 0)                             AS arquivos_bronze,
         inv.arquivos_volume - coalesce(b.arquivos_bronze, 0)       AS faltando_na_bronze,
         coalesce(q.quarentena, 0)                                  AS quarentena,
         coalesce(d.docs_validos, 0)                                AS docs_validos,
         coalesce(d.docs_validos, 0) - coalesce(d.notas_distintas, 0) AS reenvios,
         coalesce(n.notas_vigentes, 0)                              AS notas_vigentes
  FROM inv LEFT JOIN b USING (lote) LEFT JOIN q USING (lote) LEFT JOIN d USING (lote) LEFT JOIN n USING (lote)
           LEFT JOIN esperado e USING (lote))
SELECT '{run_id}', current_timestamp(), lote, arquivos_volume, arquivos_bronze, faltando_na_bronze,
       quarentena, docs_validos, reenvios, notas_vigentes,
       {int(arquivos_novos)}, {int(diferencas)},
       -- lote faltando na origem vem primeiro: é verdade mesmo numa run sem arquivo novo
       CASE WHEN arquivos_esperados > arquivos_volume                    THEN 'INCOMPLETO'
            WHEN {int(arquivos_novos)} = 0                               THEN 'SEM_ARQUIVO_NOVO'
            WHEN {int(diferencas)} = -1                                  THEN 'FALHA_OU_PULADO'
            WHEN {int(diferencas)} > 0                                   THEN 'DIVERGENTE'
            WHEN faltando_na_bronze > 0
              OR arquivos_bronze - quarentena - docs_validos <> 0
              OR {int(chaves_sem_nota)} > 0                              THEN 'INCOMPLETO'
            ELSE 'OK' END,
       {INICIO},
       arquivos_esperados,
       {int(chaves_sem_nota)}
FROM base
""")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM workspace.nfe_bronze.ctrl_lote WHERE run_id = '{run_id}' ORDER BY lote"))
