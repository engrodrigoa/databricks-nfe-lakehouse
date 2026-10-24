-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02a · Bronze com `COPY INTO`
-- MAGIC
-- MAGIC ```
-- MAGIC  landing/xml/lote_*/*.xml ──► COPY INTO (SQL, batch) ──► nfe_bronze.nfe_xml_copyinto (Delta)
-- MAGIC                               └─ lembra quais arquivos já carregou (no log da própria tabela)
-- MAGIC ```
-- MAGIC
-- MAGIC **O que observar**
-- MAGIC 1. Tabela "schemaless": o `COPY INTO` define as colunas no 1º load (`mergeSchema`)
-- MAGIC 2. Qual o tipo de `NFe` com `inferSchema = false`? (struct ou string? compare com a D007)
-- MAGIC 3. Os 2 arquivos truncados do `lote_002` entram, viram linha marcada ou somem?
-- MAGIC 4. **Idempotência:** rodar de novo carrega 0 linhas
-- MAGIC
-- MAGIC Anote os resultados na tabela comparativa de `docs/guia_estudos.md`.

-- COMMAND ----------

-- Tabela sem colunas: só um "placeholder" Delta. O schema nasce no primeiro COPY INTO.
CREATE TABLE IF NOT EXISTS workspace.nfe_bronze.nfe_xml_copyinto;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Carga
-- MAGIC - `FROM (SELECT ...)`: permite transformar na entrada e trazer a coluna oculta `_metadata`
-- MAGIC - `PATTERN`: glob relativo ao caminho do `FROM`
-- MAGIC - `FORMAT_OPTIONS`: opções do leitor XML · `COPY_OPTIONS`: comportamento do COPY (`mergeSchema`, `force`)
-- MAGIC - O resultado mostra `num_affected_rows`, `num_inserted_rows`, `num_skipped_corrupt_files`

-- COMMAND ----------

COPY INTO workspace.nfe_bronze.nfe_xml_copyinto
FROM (
  SELECT NFe,
         protNFe,
         _metadata.file_name                                    AS arquivo,
         _metadata.file_path                                    AS caminho,
         _metadata.file_size                                    AS tamanho_bytes,
         _metadata.file_modification_time                       AS modificado_em,
         regexp_extract(_metadata.file_path, '/(lote_[^/]+)/', 1) AS lote,
         current_timestamp()                                    AS carregado_em
  FROM '/Volumes/workspace/nfe_bronze/landing/xml/'
)
FILEFORMAT = XML
PATTERN = 'lote_*/*.xml'
FORMAT_OPTIONS ('rowTag' = 'nfeProc', 'inferSchema' = 'false', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## O que foi criado

-- COMMAND ----------

DESCRIBE TABLE workspace.nfe_bronze.nfe_xml_copyinto;

-- COMMAND ----------

SELECT lote,
       count(*)                     AS linhas,
       count_if(NFe IS NULL)        AS nfe_nula,
       any_value(typeof(NFe))       AS tipo_nfe
FROM workspace.nfe_bronze.nfe_xml_copyinto
GROUP BY lote
ORDER BY lote;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Idempotência
-- MAGIC Rode a célula de carga **de novo** e volte aqui. Esperado: `num_affected_rows = 0`.
-- MAGIC O `COPY INTO` guarda a lista de arquivos carregados no estado da tabela.
-- MAGIC Para recarregar à força: `COPY_OPTIONS ('force' = 'true')` (duplica linhas!).

-- COMMAND ----------

DESCRIBE HISTORY workspace.nfe_bronze.nfe_xml_copyinto;
