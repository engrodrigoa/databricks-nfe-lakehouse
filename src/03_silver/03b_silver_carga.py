# Databricks notebook source
# MAGIC %md
# MAGIC # 03b · Silver: carga incremental (PySpark)
# MAGIC
# MAGIC ```
# MAGIC  bronze.nfe_xml_raw ──readStream.table──► foreachBatch(processar_lote)          trigger availableNow
# MAGIC                                            │
# MAGIC                                            ├─ 1. from_xml (schema lido de nfe_doc)
# MAGIC                                            ├─ 2. regras de entrada ──► motivo IS NOT NULL ──► nfe_quarentena
# MAGIC                                            ├─ 3. válidos ──────────────────────────────────► nfe_doc   (append)
# MAGIC                                            ├─ 4. dedup: 1 arquivo vencedor por chave (reenvio)
# MAGIC                                            └─ 5. MERGE vw_nfe_* (só os vencedores) ───────► nfe_nota · nfe_item
# MAGIC                                                                                           nfe_pagamento · nfe_duplicata
# MAGIC ```
# MAGIC
# MAGIC **Conceitos de prova**
# MAGIC - **Delta como fonte de stream**: `readStream.table(...)` lê só os commits novos da bronze (append-only)
# MAGIC - **`foreachBatch`**: cada micro-batch vira um DataFrame comum, onde vale MERGE, múltiplos destinos, etc.
# MAGIC - **Escrita idempotente** com `txnAppId` + `txnVersion`: se o micro-batch for reexecutado (falha e retry),
# MAGIC   o Delta ignora o append repetido
# MAGIC - **MERGE** (upsert) com condição: o reenvio só sobrescreve se for mais recente
# MAGIC - Serverless (Spark Connect): **sem `cache()`/`persist()`**; dentro do `foreachBatch`, use `batch_df.sparkSession`

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

S = "workspace.nfe_silver"
BRONZE = "workspace.nfe_bronze.nfe_xml_raw"
CKPT = "/Volumes/workspace/nfe_bronze/checkpoints/silver_nfe/_checkpoint"

# Contrato: o schema do from_xml vem da própria tabela (03a). Uma única fonte da verdade.
DOC_SCHEMA = spark.table(f"{S}.nfe_doc").schema["doc"].dataType
PARSE_SCHEMA = StructType(DOC_SCHEMA.fields + [StructField("_corrupt_record", StringType())])
OPCOES_XML = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}

# Entidades: view de mapeamento (SQL, 03a) → tabela destino + chave do MERGE
ENTIDADES = [
    ("vw_nfe_nota",      "nfe_nota",      ["chave_acesso"]),
    ("vw_nfe_item",      "nfe_item",      ["chave_acesso", "n_item"]),
    ("vw_nfe_pagamento", "nfe_pagamento", ["chave_acesso", "seq"]),
    ("vw_nfe_duplicata", "nfe_duplicata", ["chave_acesso", "n_dup"]),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras de entrada (quarentena)
# MAGIC Só o que torna o documento **inutilizável ou não confiável**. Inconsistência fiscal (total ≠ Σ itens,
# MAGIC cClassTrib × CST) **não** barra: vira alerta no 03c, como no datalabs.
# MAGIC A primeira regra que falhar define o `motivo` (ordem importa).

# COMMAND ----------

def motivo_quarentena():
    chave = F.col("chave_acesso")
    cnpj = F.col("p.NFe.infNFe.emit.CNPJ")
    dv = lambda c: F.expr(f"{S}.fn_dv_mod11({c})")  # noqa: E731  (SQL UDF do Unity Catalog)
    return (
        F.when(F.col("p._corrupt_record").isNotNull() | F.col("p.NFe").isNull(), "xml_corrompido")
        .when((F.length(chave) != 44) | (F.substring(chave, 44, 1) != dv("substr(chave_acesso, 1, 43)")),
              "chave_invalida")
        .when(chave != F.col("p.protNFe.infProt.chNFe"), "chave_divergente_protocolo")
        .when(F.coalesce(F.col("p.protNFe.infProt.cStat"), F.lit("")) != "100", "nao_autorizada")
        .when(~cnpj.rlike("^[0-9A-Z]{12}[0-9]{2}$")
              | (F.substring(cnpj, 13, 1) != dv("substr(p.NFe.infNFe.emit.CNPJ, 1, 12)"))
              | (F.substring(cnpj, 14, 1) != dv("substr(p.NFe.infNFe.emit.CNPJ, 1, 13)")),
              "cnpj_emitente_invalido")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dedup de reenvio e MERGE
# MAGIC 1. Entre os arquivos do micro-batch, **1 vencedor por chave**: o mais recente
# MAGIC    (`modificado_em`, depois `carregado_em`, depois `arquivo`): `row_number()` sobre uma janela
# MAGIC 2. Cada entidade usa **só as linhas do arquivo vencedor** (semi join): itens, pagamentos e duplicatas
# MAGIC    de um mesmo documento nunca se misturam com os de outro reenvio
# MAGIC 3. `MERGE`: se a chave já existe, só atualiza quando o arquivo novo é **mais recente** que o gravado
# MAGIC
# MAGIC Limite conhecido (D012): se um reenvio tiver **menos** itens que o anterior, os itens a mais permanecem.

# COMMAND ----------

def vencedores(sess, caminhos_lote):
    """(chave_acesso, caminho) do arquivo mais recente de cada chave neste micro-batch."""
    w = Window.partitionBy("chave_acesso").orderBy(
        F.col("modificado_em").desc(), F.col("carregado_em").desc(), F.col("arquivo").desc()
    )
    return (
        sess.table(f"{S}.nfe_doc")
        .join(caminhos_lote, "caminho", "left_semi")
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .select("chave_acesso", "caminho")
    )


def merge_entidade(sess, view, tabela, chaves, venc):
    origem = sess.table(f"{S}.{view}").join(venc, ["chave_acesso", "caminho"], "left_semi")
    cond = " AND ".join(f"t.{k} = s.{k}" for k in chaves)
    (
        DeltaTable.forName(sess, f"{S}.{tabela}").alias("t")
        .merge(origem.alias("s"), cond)
        .whenMatchedUpdateAll(condition="s.modificado_em > t.modificado_em")
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def processar_lote(batch_df, batch_id):
    sess = batch_df.sparkSession  # Spark Connect: use a sessão do batch, não a global

    # 1. parse + 2. regras
    classificado = (
        batch_df
        .withColumn("p", F.from_xml("xml", PARSE_SCHEMA, OPCOES_XML))
        .withColumn("chave_acesso", F.substring("p.NFe.infNFe._Id", 4, 44))
        .withColumn("motivo", motivo_quarentena())
    )

    # quarentena: guarda o XML bruto para investigar/reprocessar
    (
        classificado.filter("motivo IS NOT NULL")
        .select("lote", "arquivo", "caminho", "modificado_em", "chave_acesso", "motivo",
                F.col("xml").alias("conteudo"), F.current_timestamp().alias("detectado_em"))
        .write.format("delta").mode("append")
        .option("txnAppId", "nfe_silver_quarentena").option("txnVersion", batch_id)
        .saveAsTable(f"{S}.nfe_quarentena")
    )

    # 3. válidos → nfe_doc (o struct sem a coluna _corrupt_record, no formato exato da tabela)
    (
        classificado.filter("motivo IS NULL")
        .select("lote", "arquivo", "caminho", "tamanho_bytes", "modificado_em", "carregado_em", "chave_acesso",
                F.struct(*[F.col(f"p.{f.name}") for f in DOC_SCHEMA.fields]).alias("doc"),
                F.current_timestamp().alias("processado_em"))
        .write.format("delta").mode("append")
        .option("txnAppId", "nfe_silver_doc").option("txnVersion", batch_id)
        .saveAsTable(f"{S}.nfe_doc")
    )

    # 4. dedup + 5. MERGE das entidades (lendo nfe_doc já gravado: sem recalcular o parse)
    caminhos_lote = batch_df.select("caminho").distinct()
    venc = vencedores(sess, caminhos_lote)
    for view, tabela, chaves in ENTIDADES:
        merge_entidade(sess, view, tabela, chaves, venc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução
# MAGIC Primeira vez: processa as 858 linhas da bronze. Rodar de novo sem arquivo novo na bronze: nada acontece
# MAGIC (o checkpoint deste stream é independente dos checkpoints da bronze).

# COMMAND ----------

def versoes(tabela):
    return spark.sql(f"DESCRIBE HISTORY {S}.{tabela} LIMIT 1").first().version

antes = {t: versoes(t) for t in ["nfe_doc", "nfe_quarentena", "nfe_nota", "nfe_item"]}

q = (
    spark.readStream.table(BRONZE)
    .writeStream
    .foreachBatch(processar_lote)
    .option("checkpointLocation", CKPT)
    .trigger(availableNow=True)
    .start()
)
q.awaitTermination()

depois = {t: versoes(t) for t in antes}
print({t: f"v{antes[t]} → v{depois[t]}" for t in antes})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'nfe_doc'        AS tabela, count(*) AS linhas FROM workspace.nfe_silver.nfe_doc        UNION ALL
# MAGIC SELECT 'nfe_quarentena',            count(*)          FROM workspace.nfe_silver.nfe_quarentena UNION ALL
# MAGIC SELECT 'nfe_nota',                  count(*)          FROM workspace.nfe_silver.nfe_nota       UNION ALL
# MAGIC SELECT 'nfe_item',                  count(*)          FROM workspace.nfe_silver.nfe_item       UNION ALL
# MAGIC SELECT 'nfe_pagamento',             count(*)          FROM workspace.nfe_silver.nfe_pagamento  UNION ALL
# MAGIC SELECT 'nfe_duplicata',             count(*)          FROM workspace.nfe_silver.nfe_duplicata

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que o MERGE fez
# MAGIC `operationMetrics` do MERGE: `numTargetRowsInserted`, `numTargetRowsUpdated`, `numSourceRows`.
# MAGIC Na 1ª carga: só inserts. Reenvio vindo em lote posterior: aparece como update.

# COMMAND ----------

# DESCRIBE HISTORY não pode ser subconsulta em SQL: em Python ele é um DataFrame como outro qualquer
display(
    spark.sql(f"DESCRIBE HISTORY {S}.nfe_nota")
    .select("version", "timestamp", "operation",
            F.col("operationMetrics")["numSourceRows"].alias("origem"),
            F.col("operationMetrics")["numTargetRowsInserted"].alias("inseridas"),
            F.col("operationMetrics")["numTargetRowsUpdated"].alias("atualizadas"))
    .orderBy("version")
)
