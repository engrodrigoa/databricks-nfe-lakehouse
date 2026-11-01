-- Databricks notebook source
-- MAGIC %md
-- MAGIC # j03 · Job: gold
-- MAGIC Ordem importa: o agregado lê a fato (no pipeline essa ordem é automática; aqui é explícita).

-- COMMAND ----------

REFRESH MATERIALIZED VIEW workspace.nfe_gold.ft_nfe_item;
REFRESH MATERIALIZED VIEW workspace.nfe_gold.ft_nfe_documento;
REFRESH MATERIALIZED VIEW workspace.nfe_gold.ag_nfe_ibscbs_mensal;
