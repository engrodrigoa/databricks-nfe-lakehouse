-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup do Unity Catalog
-- MAGIC
-- MAGIC Cria a hierarquia **catálogo → schema → volume/tabela** do projeto.
-- MAGIC
-- MAGIC ```
-- MAGIC workspace                      (catálogo padrão do Free Edition)
-- MAGIC ├── nfe_bronze
-- MAGIC │   ├── landing      VOLUME    xml/lote_NNN/*.xml  ·  ref/*.csv
-- MAGIC │   └── checkpoints  VOLUME    estado do Auto Loader (schemaLocation, checkpoint)
-- MAGIC ├── nfe_silver
-- MAGIC └── nfe_gold
-- MAGIC ```
-- MAGIC
-- MAGIC Idempotente (`IF NOT EXISTS`): pode rodar quantas vezes quiser.
-- MAGIC Para a prova: **managed** volume/tabela = Unity Catalog gerencia o armazenamento e o ciclo de vida;
-- MAGIC `DROP` apaga os dados. **External** = aponta para um caminho que você controla; `DROP` remove só o metadado.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.nfe_bronze COMMENT 'NF-e: dados brutos (XML como chegou)';
CREATE SCHEMA IF NOT EXISTS workspace.nfe_silver COMMENT 'NF-e: dados tipados, 1 linha por chave';
CREATE SCHEMA IF NOT EXISTS workspace.nfe_gold   COMMENT 'NF-e: modelo dimensional para BI';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.nfe_bronze.landing
  COMMENT 'Zona de chegada: xml/lote_NNN/ e ref/';
CREATE VOLUME IF NOT EXISTS workspace.nfe_bronze.checkpoints
  COMMENT 'Estado do Auto Loader (schema e checkpoint)';

-- COMMAND ----------

-- Conferência: o que existe no catálogo
SHOW SCHEMAS IN workspace LIKE 'nfe*';

-- COMMAND ----------

SHOW VOLUMES IN workspace.nfe_bronze;
