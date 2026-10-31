-- Lakeflow Declarative Pipeline · GOLD
--
--   dm_nfe_participante_hist ─► dm_nfe_participante (MV, sk por versão)
--   nfe_item + nfe_nota + dm_nfe_participante ─► ft_nfe_item (MV, join point-in-time) ─► ag_nfe_ibscbs_mensal (MV)
--
-- Dimensões estáticas (tempo, operação, tributação...) vêm da gold dos notebooks (workspace.nfe_gold):
-- não mudam com a carga, não precisam estar no grafo.
-- A ordem (fato antes do agregado) que no 04c era manual, aqui o pipeline deduz pelas dependências.

CREATE OR REFRESH MATERIALIZED VIEW workspace.nfe_ldp_gold.dm_nfe_participante
COMMENT 'Participante SCD2: 1 linha por versão, sk_participante estável por versão'
AS
SELECT xxhash64(doc_participante, __START_AT)   AS sk_participante,
       doc_participante, tipo_doc, nome, ie, uf, cod_municipio, municipio, logradouro, numero, bairro, cep,
       __START_AT                               AS vigencia_inicio,
       __END_AT                                 AS vigencia_fim,
       __END_AT IS NULL                         AS fl_vigente
FROM workspace.nfe_ldp_gold.dm_nfe_participante_hist;

-- Expectations em MV: WARN (padrão), DROP ROW ou FAIL UPDATE, por regra
CREATE OR REFRESH MATERIALIZED VIEW workspace.nfe_ldp_gold.ft_nfe_item (
  CONSTRAINT chave_44              EXPECT (length(chave_acesso) = 44) ON VIOLATION FAIL UPDATE,
  CONSTRAINT emitente_resolvido    EXPECT (sk_emitente IS NOT NULL),
  CONSTRAINT destinatario_resolvido EXPECT (sk_destinatario IS NOT NULL),
  CONSTRAINT tributacao_conhecida  EXPECT (fl_tributacao_conhecida)
)
CLUSTER BY (sk_data)
COMMENT 'Fato no grão item, com participante na versão vigente na data de emissão (point-in-time)'
AS
SELECT i.chave_acesso,
       i.n_item,
       CAST(date_format(n.dt_emissao, 'yyyyMMdd') AS INT)                     AS sk_data,
       n.dh_emissao,
       concat(n.tp_nf, n.fin_nfe, n.id_dest, n.ind_final, n.ind_pres)        AS sk_operacao,
       concat(i.cst_ibscbs, i.cclasstrib)                                    AS sk_tributacao,
       t.sk_tributacao IS NOT NULL                                           AS fl_tributacao_conhecida,
       pe.sk_participante                                                    AS sk_emitente,
       pd.sk_participante                                                    AS sk_destinatario,
       n.emit_cnpj, n.dest_doc,
       n.emit_uf AS uf_emitente, n.emit_cod_municipio AS cod_mun_emitente,
       n.dest_uf AS uf_destinatario, n.dest_cod_municipio AS cod_mun_destinatario,
       i.cfop, i.ncm, i.cst_ibscbs, i.cclasstrib,
       i.qcom, i.vprod, i.v_icms, i.v_icms_deson, i.v_ipi, i.v_pis, i.v_cofins,
       i.vbc_ibscbs, i.v_ibs_uf, i.v_ibs_mun, i.v_ibs, i.v_cbs,
       coalesce(i.v_dif_ibs_uf, 0) + coalesce(i.v_dif_ibs_mun, 0)            AS v_dif_ibs,
       coalesce(i.v_dif_cbs, 0)                                              AS v_dif_cbs,
       coalesce(i.v_devtrib_ibs_uf, 0) + coalesce(i.v_devtrib_ibs_mun, 0)    AS v_devtrib_ibs,
       coalesce(i.v_devtrib_cbs, 0)                                          AS v_devtrib_cbs,
       coalesce(i.v_is, 0)                                                   AS v_is,
       n.lote
FROM workspace.nfe_ldp_silver.nfe_item i
JOIN workspace.nfe_ldp_silver.nfe_nota n USING (chave_acesso)
LEFT JOIN workspace.nfe_gold.dm_nfe_tributacao t
       ON t.sk_tributacao = concat(i.cst_ibscbs, i.cclasstrib)
-- point-in-time: a versão do participante vigente no momento da emissão
LEFT JOIN workspace.nfe_ldp_gold.dm_nfe_participante pe
       ON pe.doc_participante = n.emit_cnpj
      AND n.dh_emissao >= pe.vigencia_inicio
      AND (pe.vigencia_fim IS NULL OR n.dh_emissao < pe.vigencia_fim)
LEFT JOIN workspace.nfe_ldp_gold.dm_nfe_participante pd
       ON pd.doc_participante = n.dest_doc
      AND n.dh_emissao >= pd.vigencia_inicio
      AND (pd.vigencia_fim IS NULL OR n.dh_emissao < pd.vigencia_fim);

CREATE OR REFRESH MATERIALIZED VIEW workspace.nfe_ldp_gold.ag_nfe_ibscbs_mensal
COMMENT 'Desempenho fiscal IBS/CBS por mês (pipeline)'
AS
SELECT f.sk_data DIV 100                                         AS ano_mes,
       f.uf_emitente, f.cod_mun_emitente, f.cst_ibscbs, f.cclasstrib, o.natureza,
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
FROM workspace.nfe_ldp_gold.ft_nfe_item f
JOIN workspace.nfe_gold.dm_nfe_operacao o USING (sk_operacao)
GROUP BY ALL;
