-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04a · Gold: dimensões
-- MAGIC
-- MAGIC ```
-- MAGIC  silver.ref_* ──► dm_nfe_municipio · dm_nfe_cfop · dm_nfe_ncm · dm_nfe_tributacao     (SCD tipo 1)
-- MAGIC  gerado        ──► dm_nfe_tempo (2025–2027) · dm_nfe_operacao (junk dimension)
-- MAGIC  marco 5       ──► dm_nfe_participante (SCD tipo 2, AUTO CDC)
-- MAGIC ```
-- MAGIC **Por que TABLE (e não materialized view)?** São pequenas e mudam só quando a referência muda.
-- MAGIC `CREATE OR REPLACE TABLE ... AS SELECT` reescreve tudo = **SCD tipo 1** (vale o valor atual, sem histórico).
-- MAGIC Chaves: código natural quando ele já é estável (CFOP, NCM, município); chave composta legível nas demais.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.nfe_gold.dm_nfe_tempo
COMMENT 'Calendário 2025-2027. sk_data = AAAAMMDD'
AS
SELECT CAST(date_format(data, 'yyyyMMdd') AS INT)                                 AS sk_data,
       data,
       year(data)                                                                AS ano,
       quarter(data)                                                             AS trimestre,
       month(data)                                                               AS mes,
       CAST(date_format(data, 'yyyyMM') AS INT)                                  AS ano_mes,
       element_at(array('janeiro','fevereiro','marco','abril','maio','junho','julho',
                        'agosto','setembro','outubro','novembro','dezembro'), month(data)) AS nome_mes,
       ((dayofweek(data) + 5) % 7) + 1                                           AS dia_semana_iso,  -- 1 = segunda
       dayofweek(data) IN (1, 7)                                                 AS fl_fim_de_semana
FROM (SELECT explode(sequence(DATE'2025-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS data);

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.nfe_gold.dm_nfe_municipio
COMMENT 'Municípios IBGE com UF e região'
AS
SELECT m.cod_municipio, m.nome_municipio, m.sigla_uf, u.cod_uf, u.nome_uf, u.regiao
FROM workspace.nfe_silver.ref_municipio m
JOIN workspace.nfe_silver.ref_uf u USING (sigla_uf);

CREATE OR REPLACE TABLE workspace.nfe_gold.dm_nfe_cfop COMMENT 'CFOP' AS
SELECT cfop, descricao, sentido, abrangencia, grupo FROM workspace.nfe_silver.ref_cfop;

CREATE OR REPLACE TABLE workspace.nfe_gold.dm_nfe_ncm COMMENT 'NCM' AS
SELECT ncm, descricao_ncm, capitulo, descricao_capitulo FROM workspace.nfe_silver.ref_ncm;

CREATE OR REPLACE TABLE workspace.nfe_gold.dm_nfe_tributacao
COMMENT 'CST x cClassTrib do IBS/CBS. SCD1 por enquanto (datalabs: snapshot dbt)'
AS
SELECT concat(cst, cclasstrib) AS sk_tributacao,
       cst, cclasstrib, descricao_cst, descricao_classtrib, tipo_tributacao, p_reducao_aliquota
FROM workspace.nfe_silver.ref_classtrib;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Junk dimension: `dm_nfe_operacao`
-- MAGIC 5 indicadores de baixa cardinalidade do `ide` (tpNF, finNFe, idDest, indFinal, indPres) viram **uma** chave
-- MAGIC (`sk_operacao` = concatenação). Em vez de 5 colunas soltas na fato, 1 FK e as descrições na dimensão.
-- MAGIC `VALUES ... AS t(colunas)` = tabela literal em SQL; `CROSS JOIN` gera todas as 2 × 6 × 3 × 2 × 7 = 504 combinações.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.nfe_gold.dm_nfe_operacao
COMMENT 'Junk dimension: tpNF|finNFe|idDest|indFinal|indPres'
AS
SELECT concat(tp_nf, fin_nfe, id_dest, ind_final, ind_pres)        AS sk_operacao,
       tp_nf, ds_tp_nf, fin_nfe, ds_finalidade, id_dest, ds_destino,
       ind_final, ds_consumidor, ind_pres, ds_presenca,
       CASE WHEN fin_nfe = '4' THEN 'devolucao' ELSE 'venda' END    AS natureza,
       CASE WHEN fin_nfe = '4' THEN -1 ELSE 1 END                   AS sinal_arrecadacao
FROM       VALUES ('0','Entrada'), ('1','Saida')                                AS a(tp_nf, ds_tp_nf)
CROSS JOIN VALUES ('1','Normal'), ('2','Complementar'), ('3','Ajuste'), ('4','Devolucao'),
                  ('5','Nota de credito'), ('6','Nota de debito')               AS b(fin_nfe, ds_finalidade)
CROSS JOIN VALUES ('1','Interna'), ('2','Interestadual'), ('3','Exterior')     AS c(id_dest, ds_destino)
CROSS JOIN VALUES ('0','Normal'), ('1','Consumidor final')                      AS d(ind_final, ds_consumidor)
CROSS JOIN VALUES ('0','Nao se aplica'), ('1','Presencial'), ('2','Internet'), ('3','Teleatendimento'),
                  ('4','Entrega em domicilio'), ('5','Presencial fora do estabelecimento'), ('9','Outros')
                                                                                AS e(ind_pres, ds_presenca);

-- COMMAND ----------

SELECT 'dm_nfe_tempo' AS dimensao, count(*) AS linhas FROM workspace.nfe_gold.dm_nfe_tempo      UNION ALL
SELECT 'dm_nfe_municipio',         count(*)          FROM workspace.nfe_gold.dm_nfe_municipio  UNION ALL
SELECT 'dm_nfe_cfop',              count(*)          FROM workspace.nfe_gold.dm_nfe_cfop       UNION ALL
SELECT 'dm_nfe_ncm',               count(*)          FROM workspace.nfe_gold.dm_nfe_ncm        UNION ALL
SELECT 'dm_nfe_tributacao',        count(*)          FROM workspace.nfe_gold.dm_nfe_tributacao UNION ALL
SELECT 'dm_nfe_operacao',          count(*)          FROM workspace.nfe_gold.dm_nfe_operacao;
