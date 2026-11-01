# Databricks notebook source
# MAGIC %md
# MAGIC # j05 · Job: ctrl_lote (D008)
# MAGIC Roda **sempre** (`run_if: ALL_DONE`): registra também as execuções com falha, que são as que mais importam.
# MAGIC Uma linha por lote por execução = histórico. Completude (Volume × bronze) e integridade (quarentena) juntas.
# MAGIC
# MAGIC Task values de tasks que não rodaram (falha ou condição falsa) voltam como `default` (-1).

# COMMAND ----------

dbutils.widgets.text("run_id", "manual")
run_id = dbutils.widgets.get("run_id")

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

# COMMAND ----------

spark.sql(f"""
INSERT INTO workspace.nfe_bronze.ctrl_lote
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
  SELECT inv.lote, inv.arquivos_volume,
         coalesce(b.arquivos_bronze, 0)                             AS arquivos_bronze,
         inv.arquivos_volume - coalesce(b.arquivos_bronze, 0)       AS faltando_na_bronze,
         coalesce(q.quarentena, 0)                                  AS quarentena,
         coalesce(d.docs_validos, 0)                                AS docs_validos,
         coalesce(d.docs_validos, 0) - coalesce(d.notas_distintas, 0) AS reenvios,
         coalesce(n.notas_vigentes, 0)                              AS notas_vigentes
  FROM inv LEFT JOIN b USING (lote) LEFT JOIN q USING (lote) LEFT JOIN d USING (lote) LEFT JOIN n USING (lote))
SELECT '{run_id}', current_timestamp(), lote, arquivos_volume, arquivos_bronze, faltando_na_bronze,
       quarentena, docs_validos, reenvios, notas_vigentes,
       {int(arquivos_novos)}, {int(diferencas)},
       CASE WHEN {int(arquivos_novos)} = 0                               THEN 'SEM_ARQUIVO_NOVO'
            WHEN {int(diferencas)} = -1                                  THEN 'FALHA_OU_PULADO'
            WHEN {int(diferencas)} > 0                                   THEN 'DIVERGENTE'
            WHEN faltando_na_bronze > 0
              OR arquivos_bronze - quarentena - docs_validos <> 0        THEN 'INCOMPLETO'
            ELSE 'OK' END
FROM base
""")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM workspace.nfe_bronze.ctrl_lote WHERE run_id = '{run_id}' ORDER BY lote"))
