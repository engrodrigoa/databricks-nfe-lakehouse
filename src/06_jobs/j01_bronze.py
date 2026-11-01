# Databricks notebook source
# MAGIC %md
# MAGIC # j01 · Job: bronze
# MAGIC Caminho de carga do 02b (B2, D010), sem células de estudo. **Mesmo checkpoint e mesmo schemaLocation**:
# MAGIC continua de onde o 02b parou (não recarrega os lotes antigos).
# MAGIC
# MAGIC Saída para as próximas tasks (task value): `arquivos_novos`. A task de condição do Job usa esse valor
# MAGIC para decidir se a silver roda.

# COMMAND ----------

from pyspark.sql import functions as F

ORIGEM = "/Volumes/workspace/nfe_bronze/landing/xml/"
CKPT = "/Volumes/workspace/nfe_bronze/checkpoints"
TABELA = "workspace.nfe_bronze.nfe_xml_raw"


def com_controle(df):
    return df.select(
        "*",
        F.col("_metadata.file_name").alias("arquivo"),
        F.col("_metadata.file_path").alias("caminho"),
        F.col("_metadata.file_size").alias("tamanho_bytes"),
        F.col("_metadata.file_modification_time").alias("modificado_em"),
        F.regexp_extract("_metadata.file_path", r"/(lote_[^/]+)/", 1).alias("lote"),
        F.current_timestamp().alias("carregado_em"),
    )


def versao(tabela):
    if not spark.catalog.tableExists(tabela):
        return -1
    return spark.sql(f"DESCRIBE HISTORY {tabela} LIMIT 1").first().version


def linhas_escritas_desde(tabela, v_antes):
    hist = spark.sql(f"DESCRIBE HISTORY {tabela}").filter(F.col("version") > v_antes)
    return hist.select(F.sum(F.col("operationMetrics")["numOutputRows"].cast("long"))).first()[0] or 0

# COMMAND ----------

v_antes = versao(TABELA)
q = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("wholeText", "true")
    .option("pathGlobFilter", "*.xml")
    .option("cloudFiles.schemaLocation", f"{CKPT}/nfe_xml_raw/_schema")
    .load(ORIGEM)
    .withColumnRenamed("value", "xml")
    .transform(com_controle)
    .writeStream
    .option("checkpointLocation", f"{CKPT}/nfe_xml_raw/_checkpoint")
    .trigger(availableNow=True)
    .toTable(TABELA)
)
q.awaitTermination()   # se o stream falhar, a exceção sobe aqui e a TASK falha (é o que queremos)

novos = int(linhas_escritas_desde(TABELA, v_antes))
dbutils.jobs.taskValues.set(key="arquivos_novos", value=novos)
print(f"{novos} arquivos novos na bronze")
