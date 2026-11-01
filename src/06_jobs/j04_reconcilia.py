# Databricks notebook source
# MAGIC %md
# MAGIC # j04 · Job: reconciliação notebooks × pipeline
# MAGIC Junta os dois ramos do Job. Se qualquer camada divergir, a task **falha de propósito** (`raise`):
# MAGIC um Job verde tem que significar "dado consistente", não só "código sem erro".
# MAGIC Saída: task value `diferencas` (nº de camadas divergentes).

# COMMAND ----------

PARES = [
    ("bronze",        "count(*)",   "workspace.nfe_bronze.nfe_xml_raw",       "workspace.nfe_ldp_bronze.nfe_xml_raw"),
    ("quarentena",    "count(*)",   "workspace.nfe_silver.nfe_quarentena",    "workspace.nfe_ldp_silver.nfe_quarentena"),
    ("nfe_doc",       "count(*)",   "workspace.nfe_silver.nfe_doc",           "workspace.nfe_ldp_silver.nfe_doc"),
    ("nfe_nota",      "count(*)",   "workspace.nfe_silver.nfe_nota",          "workspace.nfe_ldp_silver.nfe_nota"),
    ("nfe_item",      "count(*)",   "workspace.nfe_silver.nfe_item",          "workspace.nfe_ldp_silver.nfe_item"),
    ("ft_nfe_item",   "count(*)",   "workspace.nfe_gold.ft_nfe_item",         "workspace.nfe_ldp_gold.ft_nfe_item"),
    ("v_cbs_mensal",  "sum(v_cbs)", "workspace.nfe_gold.ag_nfe_ibscbs_mensal", "workspace.nfe_ldp_gold.ag_nfe_ibscbs_mensal"),
]

linhas = []
for camada, medida, nb, ldp in PARES:
    v_nb = spark.sql(f"SELECT {medida} FROM {nb}").first()[0]
    v_ldp = spark.sql(f"SELECT {medida} FROM {ldp}").first()[0]
    linhas.append((camada, str(v_nb), str(v_ldp), v_nb == v_ldp))

display(spark.createDataFrame(linhas, "camada string, notebook string, pipeline string, ok boolean"))

divergentes = [c for c, _, _, ok in linhas if not ok]
dbutils.jobs.taskValues.set(key="diferencas", value=len(divergentes))

if divergentes:
    # Causa comum: arquivo chegou ENTRE a leitura de um ramo e a do outro. A próxima execução reconcilia.
    raise AssertionError(f"Notebooks e pipeline divergem em: {divergentes}")
