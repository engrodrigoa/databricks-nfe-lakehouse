-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04c · Gold: validação e refresh de ponta a ponta
-- MAGIC
-- MAGIC ```
-- MAGIC  1. reconciliação   silver × gold: mesmas linhas, mesmos totais (a gold não pode criar nem perder valor)
-- MAGIC  2. órfãos          chave da fato sem linha na dimensão (integridade referencial: o Delta não impõe FK)
-- MAGIC  3. lote_004        bronze → silver → REFRESH das MVs → reconciliação de novo
-- MAGIC  4. event log       como o Databricks atualizou a MV (incremental ou completo)
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Reconciliação silver × gold
-- MAGIC Toda linha deve ter `diferenca = 0`. Só entram medidas **aditivas** (contagem de linhas, somas de valor).
-- MAGIC `qtd_notas` do agregado **não** soma entre linhas: uma nota com itens de 2 CSTs aparece em 2 linhas.
-- MAGIC Contagem distinta e percentual não são aditivos (pergunta clássica de modelagem).

-- COMMAND ----------

SELECT medida, silver, gold, silver - gold AS diferenca
FROM (
  SELECT 'itens' AS medida, (SELECT count(*) FROM workspace.nfe_silver.nfe_item) AS silver,
         (SELECT count(*) FROM workspace.nfe_gold.ft_nfe_item) AS gold
  UNION ALL
  SELECT 'notas', (SELECT count(*) FROM workspace.nfe_silver.nfe_nota),
         (SELECT count(*) FROM workspace.nfe_gold.ft_nfe_documento)
  UNION ALL
  SELECT 'v_cbs (silver x fato)', (SELECT sum(v_cbs) FROM workspace.nfe_silver.nfe_item),
         (SELECT sum(v_cbs) FROM workspace.nfe_gold.ft_nfe_item)
  UNION ALL
  SELECT 'v_cbs (fato x agregado)', (SELECT sum(v_cbs) FROM workspace.nfe_gold.ft_nfe_item),
         (SELECT sum(v_cbs) FROM workspace.nfe_gold.ag_nfe_ibscbs_mensal)
  UNION ALL
  SELECT 'v_nf (silver x fato doc)', (SELECT sum(v_nf) FROM workspace.nfe_silver.nfe_nota),
         (SELECT sum(v_nf) FROM workspace.nfe_gold.ft_nfe_documento)
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Chaves órfãs
-- MAGIC Esperado: **0** em tudo, exceto `sk_tributacao`. Os itens com `cclasstrib_incoerente` (alerta da silver)
-- MAGIC formam um par CST × cClassTrib que não existe na dimensão: o aviso da silver reaparece como FK órfã na gold.

-- COMMAND ----------

SELECT 'sk_data' AS chave, count(*) AS orfaos
FROM workspace.nfe_gold.ft_nfe_item f LEFT ANTI JOIN workspace.nfe_gold.dm_nfe_tempo d USING (sk_data)
UNION ALL
SELECT 'sk_operacao', count(*)
FROM workspace.nfe_gold.ft_nfe_item f LEFT ANTI JOIN workspace.nfe_gold.dm_nfe_operacao d USING (sk_operacao)
UNION ALL
SELECT 'sk_tributacao', count(*)
FROM workspace.nfe_gold.ft_nfe_item f LEFT ANTI JOIN workspace.nfe_gold.dm_nfe_tributacao d USING (sk_tributacao)
UNION ALL
SELECT 'ncm', count(*)
FROM workspace.nfe_gold.ft_nfe_item f LEFT ANTI JOIN workspace.nfe_gold.dm_nfe_ncm d USING (ncm)
UNION ALL
SELECT 'cfop', count(*)
FROM workspace.nfe_gold.ft_nfe_item f LEFT ANTI JOIN workspace.nfe_gold.dm_nfe_cfop d USING (cfop)
UNION ALL
SELECT 'cod_mun_emitente', count(*)
FROM workspace.nfe_gold.ft_nfe_item f LEFT ANTI JOIN workspace.nfe_gold.dm_nfe_municipio d
  ON d.cod_municipio = f.cod_mun_emitente;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Lote novo de ponta a ponta
-- MAGIC ```
-- MAGIC  1. Windows:  python gerador\gen_nfe.py --inbox data\inbox\lote_004 --n 500 --sujo --data-ini 2026-08-01 --data-fim 2026-09-30 --manifesto data\manifesto_lote_004.csv
-- MAGIC  2. Volume:   landing/xml/lote_004/  (os ARQUIVOS)  ·  landing/manifesto/manifesto_lote_004.csv
-- MAGIC  3. 02b:      células de setup + B2 (bronze oficial)
-- MAGIC  4. 03b:      Run all
-- MAGIC  5. aqui:     REFRESH abaixo, depois seções 1 e 2 de novo
-- MAGIC ```
-- MAGIC `--data-ini/--data-fim` em ago–set: são **notas com data antiga chegando agora**. O agregado precisa reabrir
-- MAGIC meses passados. No datalabs isso exigia lógica de "meses afetados"; na MV é responsabilidade do refresh.
-- MAGIC **Ordem importa:** `ag_nfe_ibscbs_mensal` lê `ft_nfe_item`, então a fato atualiza primeiro.

-- COMMAND ----------

REFRESH MATERIALIZED VIEW workspace.nfe_gold.ft_nfe_item;
REFRESH MATERIALIZED VIEW workspace.nfe_gold.ft_nfe_documento;
REFRESH MATERIALIZED VIEW workspace.nfe_gold.ag_nfe_ibscbs_mensal;

-- COMMAND ----------

-- Meses que mudaram com o lote_004 (compare com a consulta antes do refresh, se tiver anotado)
SELECT ano_mes, sum(qtd_itens) AS itens, sum(v_cbs) AS v_cbs
FROM workspace.nfe_gold.ag_nfe_ibscbs_mensal
GROUP BY ano_mes
ORDER BY ano_mes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Como a MV foi atualizada
-- MAGIC `event_log()` lê o log do pipeline que mantém a MV. O evento `planning_information` diz a técnica
-- MAGIC (incremental × recomputação completa) e o motivo. Se a função não estiver disponível no Free Edition,
-- MAGIC a mesma informação aparece em **Catalog → ft_nfe_item → aba de refresh/pipeline**.

-- COMMAND ----------

SELECT timestamp, event_type, message
FROM event_log(TABLE(workspace.nfe_gold.ag_nfe_ibscbs_mensal))
WHERE event_type IN ('planning_information', 'flow_progress', 'update_progress')
ORDER BY timestamp DESC
LIMIT 20;
