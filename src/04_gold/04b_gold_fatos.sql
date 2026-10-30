-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04b · Gold: fatos (materialized views) e views de consumo
-- MAGIC
-- MAGIC ```
-- MAGIC  silver.nfe_item + nfe_nota ──► ft_nfe_item          MATERIALIZED VIEW  grão: item
-- MAGIC  silver.nfe_nota            ──► ft_nfe_documento     MATERIALIZED VIEW  grão: documento
-- MAGIC  ft_nfe_item + dm_operacao   ──► ag_nfe_ibscbs_mensal MATERIALIZED VIEW  grão: mês × UF × município × CST × natureza
-- MAGIC  ag + dimensões              ──► vw_ibscbs_uf_mes     VIEW               consumo (dashboard)
-- MAGIC ```
-- MAGIC
-- MAGIC | Objeto | Quando o resultado é calculado | Atualização |
-- MAGIC |---|---|---|
-- MAGIC | **VIEW** | a cada consulta | sempre atual, custo em toda leitura |
-- MAGIC | **MATERIALIZED VIEW** | no `REFRESH` (resultado guardado) | `REFRESH MATERIALIZED VIEW`; o Databricks tenta refresh **incremental** (as fontes têm row tracking, visto no 03a) |
-- MAGIC | **TABLE** | quando você escreve | você controla (CTAS, MERGE, INSERT) |
-- MAGIC | **STREAMING TABLE** | a cada arquivo/linha nova | incremental, append (marco 5) |
-- MAGIC
-- MAGIC Por trás, toda MV é mantida por um **pipeline serverless** gerenciado. Se o `CREATE MATERIALIZED VIEW` falhar no
-- MAGIC compute do notebook, anexe este notebook (SQL) ao **SQL Warehouse** (seletor de compute → Serverless Starter Warehouse).
-- MAGIC
-- MAGIC **Participante:** até o marco 5 a fato leva CNPJ/CPF (chave natural). No marco 5 entra `sk_emitente`/`sk_destinatario`
-- MAGIC apontando para a versão SCD2 vigente na data de emissão.

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW workspace.nfe_gold.ft_nfe_item
CLUSTER BY (sk_data)
COMMENT 'Fato no grão ITEM da NF-e. Medidas aditivas, percentuais calculados como soma/soma'
AS
SELECT i.chave_acesso,
       i.n_item,
       CAST(date_format(n.dt_emissao, 'yyyyMMdd') AS INT)                     AS sk_data,
       n.dh_emissao,
       concat(n.tp_nf, n.fin_nfe, n.id_dest, n.ind_final, n.ind_pres)        AS sk_operacao,
       concat(i.cst_ibscbs, i.cclasstrib)                                    AS sk_tributacao,
       n.emit_cnpj,
       n.dest_doc,
       n.emit_uf                                                             AS uf_emitente,
       n.emit_cod_municipio                                                  AS cod_mun_emitente,
       n.dest_uf                                                             AS uf_destinatario,
       n.dest_cod_municipio                                                  AS cod_mun_destinatario,
       i.cfop, i.ncm, i.cst_ibscbs, i.cclasstrib,
       i.qcom, i.vprod, i.v_icms, i.v_icms_deson, i.v_ipi, i.v_pis, i.v_cofins,
       i.vbc_ibscbs, i.v_ibs_uf, i.v_ibs_mun, i.v_ibs, i.v_cbs,
       coalesce(i.v_dif_ibs_uf, 0) + coalesce(i.v_dif_ibs_mun, 0)            AS v_dif_ibs,
       coalesce(i.v_dif_cbs, 0)                                              AS v_dif_cbs,
       coalesce(i.v_devtrib_ibs_uf, 0) + coalesce(i.v_devtrib_ibs_mun, 0)    AS v_devtrib_ibs,
       coalesce(i.v_devtrib_cbs, 0)                                          AS v_devtrib_cbs,
       coalesce(i.v_is, 0)                                                   AS v_is,
       n.lote
FROM workspace.nfe_silver.nfe_item i
JOIN workspace.nfe_silver.nfe_nota n USING (chave_acesso);

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW workspace.nfe_gold.ft_nfe_documento
CLUSTER BY (sk_data)
COMMENT 'Fato no grão DOCUMENTO (1 linha por NF-e vigente)'
AS
SELECT n.chave_acesso,
       CAST(date_format(n.dt_emissao, 'yyyyMMdd') AS INT)                     AS sk_data,
       n.dh_emissao,
       n.dh_autorizacao,
       concat(n.tp_nf, n.fin_nfe, n.id_dest, n.ind_final, n.ind_pres)        AS sk_operacao,
       n.emit_cnpj,
       n.dest_doc,
       n.emit_uf                                                             AS uf_emitente,
       n.emit_cod_municipio                                                  AS cod_mun_emitente,
       n.dest_uf                                                             AS uf_destinatario,
       n.serie, n.nnf, n.chave_referenciada, n.qtd_itens,
       n.v_prod, n.v_nf, n.v_icms, n.v_bc_ibscbs, n.v_ibs, n.v_cbs,
       coalesce(n.v_dif_ibs_uf, 0) + coalesce(n.v_dif_ibs_mun, 0) + coalesce(n.v_dif_cbs, 0)             AS v_diferido_total,
       coalesce(n.v_devtrib_ibs_uf, 0) + coalesce(n.v_devtrib_ibs_mun, 0) + coalesce(n.v_devtrib_cbs, 0) AS v_devtrib_total,
       coalesce(n.v_is, 0)                                                   AS v_is,
       coalesce(p.qtd_pagamentos, 0)                                         AS qtd_pagamentos,
       n.lote
FROM workspace.nfe_silver.nfe_nota n
LEFT JOIN (SELECT chave_acesso, count(*) AS qtd_pagamentos
           FROM workspace.nfe_silver.nfe_pagamento GROUP BY chave_acesso) p USING (chave_acesso);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Agregado mensal
-- MAGIC No datalabs era `delete+insert` por mês afetado (lógica escrita à mão). Aqui é uma MV: o Databricks decide o que
-- MAGIC recalcular no `REFRESH`. `sinal_arrecadacao` (−1 em devolução) dá o IBS/CBS **líquido**.

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW workspace.nfe_gold.ag_nfe_ibscbs_mensal
COMMENT 'Desempenho fiscal IBS/CBS por mês, UF/município do emitente, CST/cClassTrib e natureza'
AS
SELECT f.sk_data DIV 100                                         AS ano_mes,
       f.uf_emitente,
       f.cod_mun_emitente,
       f.cst_ibscbs,
       f.cclasstrib,
       o.natureza,
       count(DISTINCT f.chave_acesso)                            AS qtd_notas,
       count(*)                                                  AS qtd_itens,
       count(DISTINCT f.emit_cnpj)                               AS qtd_emitentes,
       sum(f.vprod)                                              AS v_produtos,
       sum(f.vbc_ibscbs)                                         AS v_base_ibscbs,
       sum(f.v_ibs)                                              AS v_ibs,
       sum(f.v_cbs)                                              AS v_cbs,
       sum(f.v_dif_ibs + f.v_dif_cbs)                            AS v_diferido,
       sum(f.v_devtrib_ibs + f.v_devtrib_cbs)                    AS v_devolucao_tributo,
       sum(f.v_is)                                               AS v_is,
       sum((f.v_ibs + f.v_cbs) * o.sinal_arrecadacao)            AS v_ibscbs_liquido,
       round(100 * sum(f.v_ibs + f.v_cbs) / nullif(sum(f.vbc_ibscbs), 0), 4) AS pct_carga_efetiva
FROM workspace.nfe_gold.ft_nfe_item f
JOIN workspace.nfe_gold.dm_nfe_operacao o USING (sk_operacao)
GROUP BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View de consumo
-- MAGIC Junta o agregado com as dimensões para o dashboard. Não guarda nada: é só SQL nomeado e governado no UC.

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.nfe_gold.vw_ibscbs_uf_mes
COMMENT 'IBS/CBS por UF e mês, com nomes (consumo pelo dashboard)'
AS
SELECT t.ano_mes,
       any_value(t.nome_mes)                                     AS nome_mes,
       a.uf_emitente,
       any_value(m.nome_uf)                                      AS nome_uf,
       any_value(m.regiao)                                       AS regiao,
       sum(a.qtd_notas)                                          AS qtd_notas,
       sum(a.v_base_ibscbs)                                      AS v_base_ibscbs,
       sum(a.v_ibs)                                              AS v_ibs,
       sum(a.v_cbs)                                              AS v_cbs,
       sum(a.v_diferido)                                         AS v_diferido,
       sum(a.v_ibscbs_liquido)                                   AS v_ibscbs_liquido,
       round(100 * sum(a.v_ibs + a.v_cbs) / nullif(sum(a.v_base_ibscbs), 0), 4) AS pct_carga_efetiva
FROM workspace.nfe_gold.ag_nfe_ibscbs_mensal a
JOIN (SELECT DISTINCT ano_mes, nome_mes FROM workspace.nfe_gold.dm_nfe_tempo) t USING (ano_mes)
LEFT JOIN (SELECT DISTINCT sigla_uf, nome_uf, regiao FROM workspace.nfe_gold.dm_nfe_municipio) m
       ON m.sigla_uf = a.uf_emitente
GROUP BY t.ano_mes, a.uf_emitente;

-- COMMAND ----------

SELECT * FROM workspace.nfe_gold.vw_ibscbs_uf_mes ORDER BY ano_mes, v_ibscbs_liquido DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inspecionar uma MV
-- MAGIC `DESCRIBE EXTENDED` mostra o tipo (`MATERIALIZED_VIEW`), a última atualização e o pipeline por trás.

-- COMMAND ----------

DESCRIBE EXTENDED workspace.nfe_gold.ag_nfe_ibscbs_mensal;
