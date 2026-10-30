-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02c · Comparação: COPY INTO × Auto Loader (xml) × Auto Loader (text)
-- MAGIC
-- MAGIC ```
-- MAGIC  inventário do Volume (read_files binaryFile) ──┐
-- MAGIC  nfe_xml_copyinto ─────────────────────────────┤
-- MAGIC  nfe_xml_autoloader ───────────────────────────┼──► quem perdeu arquivo? · schema · histórico · incremental
-- MAGIC  nfe_xml_raw ──────────────────────────────────┘
-- MAGIC ```
-- MAGIC Rode depois de 02a e 02b. Cada resultado vai para a tabela comparativa de `docs/guia_estudos.md`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Contagem lado a lado

-- COMMAND ----------

SELECT 'copy_into' AS metodo, lote, count(*) AS linhas FROM workspace.nfe_bronze.nfe_xml_copyinto GROUP BY lote
UNION ALL
SELECT 'autoloader_xml', lote, count(*) FROM workspace.nfe_bronze.nfe_xml_autoloader GROUP BY lote
UNION ALL
SELECT 'autoloader_text', lote, count(*) FROM workspace.nfe_bronze.nfe_xml_raw GROUP BY lote
ORDER BY lote, metodo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Quem perdeu arquivo?
-- MAGIC O **inventário** é a fonte da verdade: lista os arquivos do Volume sem interpretar o conteúdo
-- MAGIC (`binaryFile`). Um `LEFT ANTI JOIN` mostra o que existe no Volume e não chegou na tabela.
-- MAGIC Essa técnica vira a checagem de completude do log de carga (D008).

-- COMMAND ----------

-- `_metadata.file_path` NÃO tem formato único: COPY INTO e binaryFile devolvem `dbfs:/Volumes/...`,
-- o Auto Loader devolve `/Volumes/...`. Sem normalizar, o anti join acusa tudo como faltando.
CREATE OR REPLACE TEMP VIEW inventario AS
SELECT regexp_replace(path, '^dbfs:', '') AS caminho, length AS tamanho_bytes
FROM read_files('/Volumes/workspace/nfe_bronze/landing/xml/*/*.xml', format => 'binaryFile');

SELECT metodo, regexp_extract(caminho, '/(lote_[^/]+)/', 1) AS lote, count(*) AS faltando
FROM (
  SELECT 'copy_into' AS metodo, i.caminho FROM inventario i
  LEFT ANTI JOIN workspace.nfe_bronze.nfe_xml_copyinto t   ON regexp_replace(t.caminho, '^dbfs:', '') = i.caminho
  UNION ALL
  SELECT 'autoloader_xml', i.caminho FROM inventario i
  LEFT ANTI JOIN workspace.nfe_bronze.nfe_xml_autoloader t ON regexp_replace(t.caminho, '^dbfs:', '') = i.caminho
  UNION ALL
  SELECT 'autoloader_text', i.caminho FROM inventario i
  LEFT ANTI JOIN workspace.nfe_bronze.nfe_xml_raw t        ON regexp_replace(t.caminho, '^dbfs:', '') = i.caminho
)
GROUP BY ALL
ORDER BY metodo, lote;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Diagnóstico usado no marco 2: o formato do caminho no inventário × na tabela do Auto Loader.

-- COMMAND ----------

SELECT (SELECT caminho FROM inventario LIMIT 1)                               AS inventario,
       (SELECT caminho FROM workspace.nfe_bronze.nfe_xml_raw LIMIT 1)        AS tabela;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Schema que cada método produziu

-- COMMAND ----------

DESCRIBE TABLE workspace.nfe_bronze.nfe_xml_copyinto;

-- COMMAND ----------

DESCRIBE TABLE workspace.nfe_bronze.nfe_xml_autoloader;

-- COMMAND ----------

DESCRIBE TABLE workspace.nfe_bronze.nfe_xml_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Delta: histórico e time travel
-- MAGIC Toda escrita é uma **versão** no `_delta_log`. Compare a coluna `operation`:
-- MAGIC `COPY INTO` × `STREAMING UPDATE`. `operationMetrics` mostra linhas e arquivos escritos.

-- COMMAND ----------

DESCRIBE HISTORY workspace.nfe_bronze.nfe_xml_raw;

-- COMMAND ----------

-- Time travel: como a tabela estava na versão 1 (logo após a 1ª carga)
SELECT count(*) AS linhas_v1 FROM workspace.nfe_bronze.nfe_xml_raw VERSION AS OF 1;

-- COMMAND ----------

DESCRIBE DETAIL workspace.nfe_bronze.nfe_xml_raw;   -- formato, localização, nº de arquivos, tamanho

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Teste incremental (lote novo)
-- MAGIC 1. No Windows: `python gerador\gen_nfe.py --inbox data\inbox\lote_003 --n 500 --sujo --manifesto data\manifesto_lote_003.csv`
-- MAGIC 2. Suba os **arquivos** para `landing/xml/lote_003/`
-- MAGIC 3. Rode a célula de carga do 02a, o B1 e o B2 do 02b
-- MAGIC 4. Volte e rode as seções 1 e 2: cada método deve ter carregado **só** o `lote_003`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reset (só se precisar refazer o experimento)
-- MAGIC Para o Auto Loader, apagar a tabela **não basta**: o checkpoint lembra os arquivos. Apague os dois.
-- MAGIC ```sql
-- MAGIC DROP TABLE IF EXISTS workspace.nfe_bronze.nfe_xml_autoloader;
-- MAGIC ```
-- MAGIC ```python
-- MAGIC dbutils.fs.rm("/Volumes/workspace/nfe_bronze/checkpoints/nfe_xml_autoloader", recurse=True)
-- MAGIC ```
