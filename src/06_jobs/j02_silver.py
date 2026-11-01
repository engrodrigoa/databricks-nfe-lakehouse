# Databricks notebook source
# MAGIC %md
# MAGIC # j02 · Job: silver
# MAGIC Reaproveita o 03b inteiro com `%run` (o 03b não tem células de estudo que quebrem um Job).
# MAGIC `%run` executa o outro notebook **no mesmo contexto**: variáveis e funções dele ficam disponíveis aqui
# MAGIC (diferente de `dbutils.notebook.run`, que roda isolado e só devolve o texto do `exit`).

# COMMAND ----------

# MAGIC %run ../03_silver/03b_silver_carga

# COMMAND ----------

# `antes` e `depois` vêm do 03b (versões das tabelas antes/depois do stream)
mudou = {t: depois[t] - antes[t] for t in antes}
dbutils.jobs.taskValues.set(key="commits_silver", value=sum(mudou.values()))
dbutils.jobs.taskValues.set(key="notas_vigentes", value=spark.table(f"{S}.nfe_nota").count())
print(mudou)
