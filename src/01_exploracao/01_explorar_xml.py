# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Exploração do XML da NF-e
# MAGIC
# MAGIC **Objetivo:** entender como o Spark enxerga o XML antes de desenhar a bronze.
# MAGIC Nada é gravado aqui: só leitura (batch) e observação.
# MAGIC
# MAGIC ```
# MAGIC  Volume landing/xml/lote_*/   ──►  spark.read.format("xml")  ──►  DataFrame  ──►  perguntas
# MAGIC                                      rowTag = nfeProc
# MAGIC ```
# MAGIC
# MAGIC Perguntas que este notebook responde:
# MAGIC 1. Quantos arquivos chegaram e quantos viraram linha?
# MAGIC 2. O que a **inferência de schema** faz com códigos fiscais?
# MAGIC 3. Como aparecem arquivos **truncados** e **reenvios**?
# MAGIC 4. Onde estão os valores de **IBS/CBS** e como chegar no nível de item?

# COMMAND ----------

CAMINHO_XML = "/Volumes/workspace/nfe_bronze/landing/xml/*/*.xml"   # glob: todas as pastas de lote

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. O que tem no Volume
# MAGIC `dbutils.fs` navega no armazenamento. Em Volumes, o caminho é sempre `/Volumes/<catálogo>/<schema>/<volume>/...`

# COMMAND ----------

lotes = dbutils.fs.ls("/Volumes/workspace/nfe_bronze/landing/xml/")
for lote in lotes:
    print(lote.name, len(dbutils.fs.ls(lote.path)), "arquivos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura com inferência de tipos (o jeito "ingênuo")
# MAGIC Observe no schema: `CST`, `cClassTrib`, `NCM`, `CEP`, `CPF` viram `long`; valores viram `double`.

# COMMAND ----------

df_inferido = (
    spark.read.format("xml")
    .option("rowTag", "nfeProc")
    .load(CAMINHO_XML)
)
df_inferido.select("NFe.infNFe.det").printSchema()

# COMMAND ----------

# CST "000" virou 0: informação perdida
display(
    df_inferido.selectExpr(
        "NFe.infNFe.det[0].imposto.IBSCBS.CST        as cst_ibscbs",
        "NFe.infNFe.det[0].imposto.IBSCBS.cClassTrib as cclasstrib",
        "NFe.infNFe.dest.enderDest.CEP               as cep_dest",
        "NFe.infNFe.total.IBSCBSTot.gCBS.vCBS        as v_cbs",
    ).limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura como a bronze vai ler (decisão D003)
# MAGIC - `inferSchema = false`: toda folha vira **string**, nada se perde
# MAGIC - `mode = PERMISSIVE` + `columnNameOfCorruptRecord`: arquivo quebrado não derruba a carga, vira linha marcada
# MAGIC - `_metadata`: coluna oculta com nome, caminho, tamanho e data do arquivo

# COMMAND ----------

from pyspark.sql import functions as F

df = (
    spark.read.format("xml")
    .option("rowTag", "nfeProc")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .load(CAMINHO_XML)
    .select(
        "*",
        F.col("_metadata.file_name").alias("arquivo"),
        F.regexp_extract("_metadata.file_path", r"/(lote_[^/]+)/", 1).alias("lote"),
        F.col("_metadata.file_modification_time").alias("modificado_em"),
    )
)
df.createOrReplaceTempView("vw_nfe_xml")
print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Perfil da carga por lote
# MAGIC Esta consulta é o embrião do **log de carga**: arquivos, válidos, truncados e reenvios por lote.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC   SELECT lote,
# MAGIC          arquivo,
# MAGIC          _corrupt_record IS NOT NULL         AS corrompido,
# MAGIC          substring(NFe.infNFe._Id, 4)       AS chave_acesso
# MAGIC   FROM vw_nfe_xml
# MAGIC )
# MAGIC SELECT lote,
# MAGIC        count(*)                                        AS arquivos,
# MAGIC        count_if(corrompido)                            AS corrompidos,
# MAGIC        count(DISTINCT chave_acesso)                    AS notas_distintas,
# MAGIC        count_if(NOT corrompido) - count(DISTINCT chave_acesso) AS reenvios
# MAGIC FROM base
# MAGIC GROUP BY lote
# MAGIC ORDER BY lote

# COMMAND ----------

# Como um arquivo truncado aparece.
# Pegadinha: o Spark PROÍBE consultar só a `_corrupt_record` de arquivo bruto
# (erro UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN). Referenciar outra coluna do XML resolve;
# em produção a solução é gravar a bronze primeiro e consultar a tabela Delta.
display(
    df.filter("_corrupt_record IS NOT NULL")
      .select("lote", "arquivo", "_versao", F.length("_corrupt_record").alias("bytes_lidos"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Do cabeçalho ao item
# MAGIC `det` é um **array de structs** (um elemento por item). `explode` gera uma linha por item: é o grão da `nfe_item`.
# MAGIC Repare que tudo ainda é string: o `CAST` para `DECIMAL` é trabalho da silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT substring(NFe.infNFe._Id, 4)                   AS chave_acesso,
# MAGIC        item._nItem                                     AS n_item,
# MAGIC        item.prod.NCM                                   AS ncm,
# MAGIC        item.prod.CFOP                                  AS cfop,
# MAGIC        item.imposto.IBSCBS.CST                         AS cst_ibscbs,
# MAGIC        item.imposto.IBSCBS.cClassTrib                  AS cclasstrib,
# MAGIC        item.imposto.IBSCBS.gIBSCBS.vBC                 AS vbc_ibscbs,
# MAGIC        item.imposto.IBSCBS.gIBSCBS.gCBS.vCBS           AS v_cbs
# MAGIC FROM vw_nfe_xml
# MAGIC LATERAL VIEW explode(NFe.infNFe.det) AS item
# MAGIC WHERE _corrupt_record IS NULL
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Primeira visão fiscal (só para validar a massa)
# MAGIC Atenção: as somas ainda incluem os reenvios (valor duplicado). A dedup é trabalho da silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NFe.infNFe.emit.enderEmit.UF                                    AS uf_emitente,
# MAGIC        count(DISTINCT substring(NFe.infNFe._Id, 4))                     AS notas,
# MAGIC        round(sum(CAST(NFe.infNFe.total.ICMSTot.vNF          AS DECIMAL(15,2))), 2) AS v_nf,
# MAGIC        round(sum(CAST(NFe.infNFe.total.IBSCBSTot.gCBS.vCBS  AS DECIMAL(15,2))), 2) AS v_cbs,
# MAGIC        round(sum(CAST(NFe.infNFe.total.IBSCBSTot.gIBS.vIBS  AS DECIMAL(15,2))), 2) AS v_ibs
# MAGIC FROM vw_nfe_xml
# MAGIC WHERE _corrupt_record IS NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY v_nf DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. A mesma leitura em SQL puro: `read_files`
# MAGIC Função de tabela que lê arquivos sem criar nada. É a base do `COPY INTO` e das streaming tables (marco 2).
# MAGIC
# MAGIC **Diferença importante (D007):** em `read_files` e no Auto Loader, `inferColumnTypes => false` não gera
# MAGIC struct com folhas string: as colunas de 1º nível (`NFe`, `protNFe`) viram **STRING com o XML bruto**.
# MAGIC Por isso `NFe.infNFe.ide.nNF` falha (`INVALID_EXTRACT_BASE_FIELD_TYPE`); o caminho certo é XPath sobre o texto.
# MAGIC O `<r>...</r>` envolve o conteúdo porque `NFe` tem duas raízes (`infNFe` e `Signature`).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_name                                                AS arquivo,
# MAGIC        typeof(NFe)                                                        AS tipo_nfe,
# MAGIC        length(NFe)                                                        AS tamanho_xml,
# MAGIC        xpath_string(concat('<r>', NFe, '</r>'),     '/r/infNFe/ide/nNF')  AS nnf,
# MAGIC        xpath_string(concat('<r>', protNFe, '</r>'), '/r/infProt/cStat')  AS c_stat
# MAGIC FROM read_files(
# MAGIC   '/Volumes/workspace/nfe_bronze/landing/xml/*/*.xml',
# MAGIC   format => 'xml',
# MAGIC   rowTag => 'nfeProc',
# MAGIC   inferColumnTypes => false
# MAGIC )
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anote os achados (vão para `docs/decisoes.md`)
# MAGIC - Quantas linhas × arquivos × notas distintas por lote?
# MAGIC - Os truncados apareceram em `_corrupt_record`? Com quantos bytes lidos?
# MAGIC - `det` veio como `array` em todos os lotes? (risco da D004)
# MAGIC - Leitura `spark.read` (struct) × `read_files` (string bruta): qual serve melhor para a bronze?
# MAGIC - Algo inesperado no schema (campo faltando, nome diferente)?
