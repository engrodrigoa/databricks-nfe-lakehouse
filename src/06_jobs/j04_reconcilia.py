# Databricks notebook source
# MAGIC %md
# MAGIC # j04 · Job: reconciliação notebooks × pipeline
# MAGIC Junta os dois ramos do Job. Se qualquer camada divergir, a task **falha de propósito** (`raise`):
# MAGIC um Job verde tem que significar "dado consistente", não só "código sem erro".
# MAGIC Saída: task value `diferencas` (nº de camadas divergentes).
# MAGIC
# MAGIC **Corte comum (D017).** Os dois ramos leem o Volume em momentos diferentes. Se arquivos chegam durante a
# MAGIC run, um ramo vê mais que o outro e a comparação acusa divergência falsa (visto no upload pela CLI).
# MAGIC Por isso só entra na comparação o que os dois ramos **com certeza** leram:
# MAGIC ```
# MAGIC  corte = início da run ({{job.start_time.timestamp_ms}})
# MAGIC  camadas por arquivo (bronze, quarentena, nfe_doc) ... modificado_em <= corte
# MAGIC  camadas por nota (nota, item, fato, v_cbs) ........... chaves cujo ÚLTIMO arquivo (reenvio) é <= corte
# MAGIC ```
# MAGIC O que chegou depois do corte fica para a próxima run. Rodando à mão (sem parâmetro): corte = agora.

# COMMAND ----------

dbutils.widgets.text("corte_ms", "")
corte_ms = dbutils.widgets.get("corte_ms").strip()
CORTE = f"timestamp_millis({int(corte_ms)})" if corte_ms else "current_timestamp()"
print("corte:", spark.sql(f"SELECT {CORTE}").first()[0])

# chave estável = todos os arquivos dela (inclusive reenvios) chegaram antes do corte, em qualquer ramo
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW chaves_estaveis AS
SELECT chave_acesso
FROM (SELECT chave_acesso, modificado_em FROM workspace.nfe_silver.nfe_doc
      UNION ALL
      SELECT chave_acesso, modificado_em FROM workspace.nfe_ldp_silver.nfe_doc)
GROUP BY chave_acesso
HAVING max(modificado_em) <= {CORTE}
""")

POR_ARQUIVO = f"modificado_em <= {CORTE}"
POR_CHAVE = "chave_acesso IN (SELECT chave_acesso FROM chaves_estaveis)"

# COMMAND ----------

PARES = [
    # camada        medida        filtro       notebook                                pipeline
    ("bronze",      "count(*)",   POR_ARQUIVO, "workspace.nfe_bronze.nfe_xml_raw",     "workspace.nfe_ldp_bronze.nfe_xml_raw"),
    ("quarentena",  "count(*)",   POR_ARQUIVO, "workspace.nfe_silver.nfe_quarentena",  "workspace.nfe_ldp_silver.nfe_quarentena"),
    ("nfe_doc",     "count(*)",   POR_ARQUIVO, "workspace.nfe_silver.nfe_doc",         "workspace.nfe_ldp_silver.nfe_doc"),
    ("nfe_nota",    "count(*)",   POR_CHAVE,   "workspace.nfe_silver.nfe_nota",        "workspace.nfe_ldp_silver.nfe_nota"),
    ("nfe_item",    "count(*)",   POR_CHAVE,   "workspace.nfe_silver.nfe_item",        "workspace.nfe_ldp_silver.nfe_item"),
    ("ft_nfe_item", "count(*)",   POR_CHAVE,   "workspace.nfe_gold.ft_nfe_item",       "workspace.nfe_ldp_gold.ft_nfe_item"),
    # o agregado mensal não tem chave: a soma de CBS é conferida na fato que o alimenta
    ("v_cbs",       "sum(v_cbs)", POR_CHAVE,   "workspace.nfe_gold.ft_nfe_item",       "workspace.nfe_ldp_gold.ft_nfe_item"),
]

linhas = []
for camada, medida, filtro, nb, ldp in PARES:
    v_nb = spark.sql(f"SELECT {medida} FROM {nb} WHERE {filtro}").first()[0]
    v_ldp = spark.sql(f"SELECT {medida} FROM {ldp} WHERE {filtro}").first()[0]
    linhas.append((camada, str(v_nb), str(v_ldp), v_nb == v_ldp))

display(spark.createDataFrame(linhas, "camada string, notebook string, pipeline string, ok boolean"))

divergentes = [c for c, _, _, ok in linhas if not ok]
dbutils.jobs.taskValues.set(key="diferencas", value=len(divergentes))

if divergentes:
    # Com o corte comum, divergência aqui é real: um ramo processou diferente o MESMO conjunto de arquivos
    raise AssertionError(f"Notebooks e pipeline divergem em: {divergentes}")
