-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03c · Silver: qualidade, log por lote e gabarito
-- MAGIC
-- MAGIC ```
-- MAGIC  1. log por lote        bronze × quarentena × nfe_doc × nfe_nota        (embrião da ctrl_lote, D008)
-- MAGIC  2. quarentena          por motivo, com o XML guardado
-- MAGIC  3. vw_dq_alertas       regras fiscais que NÃO barram a carga (aviso)
-- MAGIC  4. gabarito            manifesto do gerador × o que o pipeline detectou
-- MAGIC  5. constraints         o que acontece quando a regra é da tabela (falha a escrita)
-- MAGIC ```
-- MAGIC Dois níveis de qualidade, como no datalabs: **erro** = quarentena (03b), **aviso** = alerta (aqui).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Log por lote
-- MAGIC Fechamento esperado: `arquivos = quarentena + docs_validos` e `docs_validos − notas_distintas = reenvios`.

-- COMMAND ----------

WITH b AS (SELECT lote, count(*) AS arquivos FROM workspace.nfe_bronze.nfe_xml_raw GROUP BY lote),
     q AS (SELECT lote, count(*) AS quarentena FROM workspace.nfe_silver.nfe_quarentena GROUP BY lote),
     d AS (SELECT lote, count(*) AS docs_validos, count(DISTINCT chave_acesso) AS notas_distintas
           FROM workspace.nfe_silver.nfe_doc GROUP BY lote),
     n AS (SELECT lote, count(*) AS notas_vigentes FROM workspace.nfe_silver.nfe_nota GROUP BY lote)
SELECT b.lote,
       b.arquivos,
       coalesce(q.quarentena, 0)                                        AS quarentena,
       coalesce(d.docs_validos, 0)                                      AS docs_validos,
       coalesce(d.docs_validos, 0) - coalesce(d.notas_distintas, 0)     AS reenvios,
       coalesce(n.notas_vigentes, 0)                                    AS notas_vigentes,
       b.arquivos - coalesce(q.quarentena, 0) - coalesce(d.docs_validos, 0) AS nao_processados
FROM b
LEFT JOIN q USING (lote)
LEFT JOIN d USING (lote)
LEFT JOIN n USING (lote)
ORDER BY b.lote;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Quarentena

-- COMMAND ----------

SELECT lote, motivo, count(*) AS arquivos, min(length(conteudo)) AS menor_xml, max(length(conteudo)) AS maior_xml
FROM workspace.nfe_silver.nfe_quarentena
GROUP BY ALL
ORDER BY lote, motivo;

-- COMMAND ----------

-- O fim do XML truncado: onde o arquivo foi cortado
SELECT arquivo, right(conteudo, 120) AS final_do_arquivo
FROM workspace.nfe_silver.nfe_quarentena
WHERE motivo = 'xml_corrompido'
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Alertas fiscais (não barram)
-- MAGIC View persistente: a gold e o dashboard vão poder usar. Uma linha por (chave, regra).

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.nfe_silver.vw_dq_alertas
COMMENT 'Alertas de qualidade (aviso): inconsistências fiscais e códigos fora das referências'
AS
WITH soma AS (
  SELECT chave_acesso, sum(v_cbs) AS soma_cbs, sum(vbc_ibscbs) AS soma_vbc
  FROM workspace.nfe_silver.nfe_item
  GROUP BY chave_acesso
)
SELECT n.chave_acesso, 'tot_cbs_divergente' AS regra,
       concat('total ', n.v_cbs, ' x soma itens ', s.soma_cbs) AS detalhe
FROM workspace.nfe_silver.nfe_nota n JOIN soma s USING (chave_acesso)
WHERE n.v_cbs <> s.soma_cbs
UNION ALL
SELECT n.chave_acesso, 'vbc_ibscbs_divergente',
       concat('total ', n.v_bc_ibscbs, ' x soma itens ', s.soma_vbc)
FROM workspace.nfe_silver.nfe_nota n JOIN soma s USING (chave_acesso)
WHERE n.v_bc_ibscbs <> s.soma_vbc
UNION ALL
SELECT i.chave_acesso, 'cclasstrib_incoerente',
       concat('item ', i.n_item, ': CST ', i.cst_ibscbs, ' x cClassTrib ', i.cclasstrib)
FROM workspace.nfe_silver.nfe_item i
LEFT ANTI JOIN workspace.nfe_silver.ref_classtrib r
  ON r.cst = i.cst_ibscbs AND r.cclasstrib = i.cclasstrib
UNION ALL
SELECT i.chave_acesso, 'ncm_fora_referencia', concat('item ', i.n_item, ': NCM ', i.ncm)
FROM workspace.nfe_silver.nfe_item i
LEFT ANTI JOIN workspace.nfe_silver.ref_ncm r ON r.ncm = i.ncm
UNION ALL
SELECT i.chave_acesso, 'cfop_fora_referencia', concat('item ', i.n_item, ': CFOP ', i.cfop)
FROM workspace.nfe_silver.nfe_item i
LEFT ANTI JOIN workspace.nfe_silver.ref_cfop r ON r.cfop = i.cfop
UNION ALL
SELECT n.chave_acesso, 'municipio_emit_fora_referencia', concat('cMun ', n.emit_cod_municipio)
FROM workspace.nfe_silver.nfe_nota n
LEFT ANTI JOIN workspace.nfe_silver.ref_municipio r ON r.cod_municipio = n.emit_cod_municipio;

-- COMMAND ----------

SELECT regra, count(DISTINCT chave_acesso) AS notas, count(*) AS ocorrencias
FROM workspace.nfe_silver.vw_dq_alertas
GROUP BY regra
ORDER BY notas DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Gabarito: o gerador sabe o que injetou
-- MAGIC Suba os manifestos (`data\manifesto_lote_002.csv`, `manifesto_lote_003.csv`) para `landing/manifesto/`.
-- MAGIC Para cada anomalia injetada: o pipeline pegou? (**recall**). E alertou algo que não foi injetado? (**falso positivo**)

-- COMMAND ----------

-- A coluna `lote` do manifesto é o nº do lote no gerador (1, 2...), não a pasta: a pasta vem da bronze, pelo arquivo
CREATE OR REPLACE TEMP VIEW manifesto AS
SELECT b.lote, m.arquivo, m.chave, m.anomalia
FROM read_files('/Volumes/workspace/nfe_bronze/landing/manifesto/*.csv',
                format => 'csv', header => true, inferColumnTypes => false) AS m
JOIN workspace.nfe_bronze.nfe_xml_raw b ON b.arquivo = m.arquivo;

-- COMMAND ----------

WITH reenviadas AS (
  SELECT chave_acesso FROM workspace.nfe_silver.nfe_doc GROUP BY chave_acesso HAVING count(*) > 1
)
SELECT m.anomalia,
       count(*) AS injetadas,
       count_if(
         CASE m.anomalia
           WHEN 'xml_corrompido' THEN q.arquivo IS NOT NULL
           WHEN 'duplicada'      THEN r.chave_acesso IS NOT NULL
           ELSE a.chave_acesso IS NOT NULL
         END) AS detectadas
FROM manifesto m
LEFT JOIN (SELECT DISTINCT arquivo FROM workspace.nfe_silver.nfe_quarentena WHERE motivo = 'xml_corrompido') q
       ON q.arquivo = m.arquivo
LEFT JOIN reenviadas r
       ON r.chave_acesso = m.chave
LEFT JOIN (SELECT DISTINCT chave_acesso, regra FROM workspace.nfe_silver.vw_dq_alertas) a
       ON a.chave_acesso = m.chave AND a.regra = m.anomalia
GROUP BY m.anomalia
ORDER BY m.anomalia;

-- COMMAND ----------

-- Falsos positivos: alerta numa nota cuja anomalia NÃO estava no manifesto
-- (só faz sentido para os lotes que têm manifesto)
SELECT a.regra, count(DISTINCT a.chave_acesso) AS notas_sem_gabarito
FROM workspace.nfe_silver.vw_dq_alertas a
JOIN workspace.nfe_silver.nfe_nota n USING (chave_acesso)
LEFT ANTI JOIN manifesto m ON m.chave = a.chave_acesso AND m.anomalia = a.regra
WHERE n.lote IN (SELECT DISTINCT lote FROM manifesto)
GROUP BY a.regra;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Constraint em ação (rode à mão, uma célula por vez)
-- MAGIC Esperado: erro `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES` (ou `CHECK_CONSTRAINT_VIOLATION`) e **nenhuma** linha gravada.
-- MAGIC Comparação para a prova:
-- MAGIC | | Constraint Delta | Quarentena (03b) | Expectation (marco 5) |
-- MAGIC |---|---|---|---|
-- MAGIC | Violou | a escrita inteira falha | a linha vai para outra tabela | warn / drop / fail, por regra |
-- MAGIC | Onde vive | na tabela | no código do pipeline | na definição do pipeline |
-- MAGIC
-- MAGIC ```sql
-- MAGIC INSERT INTO workspace.nfe_silver.nfe_item (chave_acesso, n_item, ncm)
-- MAGIC VALUES ('52261022969653000116550010000308811327540941', 1, '2202');   -- NCM com 4 dígitos
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Idempotência da silver
-- MAGIC Rode o 03b de novo **sem** arquivo novo: as versões das tabelas não mudam (o stream não acha commit novo
-- MAGIC na bronze). Depois, rode a seção 1 aqui: os números são os mesmos.
