# Databricks notebook source
# MAGIC %md
# MAGIC # 02b · Bronze com Auto Loader
# MAGIC
# MAGIC Duas variações, para comparar com o `COPY INTO`:
# MAGIC
# MAGIC ```
# MAGIC                         ┌─ B1  cloudFiles.format = xml   ──► nfe_xml_autoloader   (1 linha por nfeProc)
# MAGIC  landing/xml/lote_*/ ───┤
# MAGIC                         └─ B2  cloudFiles.format = text  ──► nfe_xml_raw          (1 linha por ARQUIVO,
# MAGIC                                wholeText = true                                     documento inteiro)
# MAGIC        estado de cada stream: Volume checkpoints/<tabela>/_schema e /_checkpoint
# MAGIC ```
# MAGIC
# MAGIC **Conceitos**
# MAGIC - `readStream.format("cloudFiles")`: descobre arquivos novos de forma incremental
# MAGIC - `cloudFiles.schemaLocation`: onde o schema inferido fica salvo (e evolui)
# MAGIC - `checkpointLocation`: quais arquivos já foram processados → **exactly-once**
# MAGIC - `trigger(availableNow=True)`: processa tudo que está pendente e **para** (batch incremental).
# MAGIC   No serverless é o modo suportado; `processingTime` contínuo não é.

# COMMAND ----------

from pyspark.sql import functions as F

ORIGEM = "/Volumes/workspace/nfe_bronze/landing/xml/"
CKPT = "/Volumes/workspace/nfe_bronze/checkpoints"


def com_controle(df):
    """Colunas de rastreabilidade, iguais nas três variações da bronze."""
    return df.select(
        "*",
        F.col("_metadata.file_name").alias("arquivo"),
        F.col("_metadata.file_path").alias("caminho"),
        F.col("_metadata.file_size").alias("tamanho_bytes"),
        F.col("_metadata.file_modification_time").alias("modificado_em"),
        F.regexp_extract("_metadata.file_path", r"/(lote_[^/]+)/", 1).alias("lote"),
        F.current_timestamp().alias("carregado_em"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## B1 · Auto Loader lendo XML
# MAGIC Observe: tipo de `NFe`, a coluna `_rescued_data` (criada automaticamente) e o que acontece com os truncados.
# MAGIC Segundo a documentação, com rescued data ativo, registros malformados são **descartados ou geram erro**.
# MAGIC Se a célula falhar por XML malformado, isso também é um resultado: anote a mensagem e siga para o B2.

# COMMAND ----------

tabela_b1 = "workspace.nfe_bronze.nfe_xml_autoloader"

q1 = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "xml")
    .option("rowTag", "nfeProc")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.schemaLocation", f"{CKPT}/nfe_xml_autoloader/_schema")
    .option("pathGlobFilter", "*.xml")
    .load(ORIGEM)
    .transform(com_controle)
    .writeStream
    .option("checkpointLocation", f"{CKPT}/nfe_xml_autoloader/_checkpoint")
    .trigger(availableNow=True)
    .toTable(tabela_b1)
)
q1.awaitTermination()
print(q1.lastProgress["numInputRows"] if q1.lastProgress else "sem progresso", "linhas nesta execução")

# COMMAND ----------

display(spark.sql(f"""
    SELECT lote, count(*) AS linhas, count_if(NFe IS NULL) AS nfe_nula,
           count_if(_rescued_data IS NOT NULL) AS com_rescued, any_value(typeof(NFe)) AS tipo_nfe
    FROM {tabela_b1} GROUP BY lote ORDER BY lote
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## B2 · Auto Loader lendo o arquivo inteiro como texto
# MAGIC Hipótese (testada localmente com Spark 4.0): sem parse na entrada, **nenhum arquivo se perde**.
# MAGIC O XML bruto vai para a coluna `xml`; quem separa os truncados é o `from_xml` (PERMISSIVE), depois.
# MAGIC É o equivalente ao `pg_read_file` do datalabs.

# COMMAND ----------

tabela_b2 = "workspace.nfe_bronze.nfe_xml_raw"

q2 = (
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
    .toTable(tabela_b2)
)
q2.awaitTermination()
print(q2.lastProgress["numInputRows"] if q2.lastProgress else "sem progresso", "linhas nesta execução")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parse "atrasado": o mesmo resultado do perfil do marco 1, agora sobre a tabela Delta
# MAGIC WITH p AS (
# MAGIC   SELECT lote,
# MAGIC          from_xml(xml, 'NFe STRING, protNFe STRING, _corrupt_record STRING',
# MAGIC                   map('mode', 'PERMISSIVE', 'columnNameOfCorruptRecord', '_corrupt_record')) AS doc
# MAGIC   FROM workspace.nfe_bronze.nfe_xml_raw
# MAGIC )
# MAGIC SELECT lote,
# MAGIC        count(*)                                                                        AS arquivos,
# MAGIC        count_if(doc._corrupt_record IS NOT NULL)                                       AS corrompidos,
# MAGIC        count(DISTINCT xpath_string(concat('<r>', doc.NFe, '</r>'), '/r/infNFe/@Id'))   AS notas_distintas
# MAGIC FROM p
# MAGIC GROUP BY lote
# MAGIC ORDER BY lote

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotência
# MAGIC Rode B1 e B2 de novo: `numInputRows` deve ser **0**. Quem garante é o checkpoint, não a tabela.
# MAGIC Pergunta de prova: *apagar a tabela e manter o checkpoint* → o stream não recarrega nada. Por quê?
