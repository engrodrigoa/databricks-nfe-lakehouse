-- Lakeflow Declarative Pipeline · SILVER (entidades)
--
--   nfe_doc ─STREAM─► vw_* (mapeamento, mesmo SQL do 03a) ─AUTO CDC SCD TYPE 1─► nfe_nota · nfe_item
--                                                                                nfe_pagamento · nfe_duplicata
--
-- AUTO CDC SCD TYPE 1 = o MERGE do 03b em forma declarativa:
--   KEYS        → chave do MERGE
--   SEQUENCE BY → quem é "mais recente" (reenvio, D012). Fora de ordem também é tratado: um arquivo
--                 mais antigo que chega depois NÃO sobrescreve o mais novo
--   SCD TYPE 1  → sobrescreve (sem histórico)
-- O mapeamento abaixo é cópia gerada do 03a (D014): manter os dois iguais é o custo da comparação paralela.


-- ============================================================================ nfe_nota
CREATE TEMPORARY VIEW vw_nfe_nota AS
WITH base AS (
  SELECT *, doc.NFe.infNFe AS n, doc.protNFe.infProt AS p FROM STREAM(workspace.nfe_ldp_silver.nfe_doc)
)
SELECT
  chave_acesso,
  n.ide.cUF                                         AS cod_uf,
  n.ide.mod                                         AS modelo,
  CAST(n.ide.serie AS INT)                          AS serie,
  CAST(n.ide.nNF AS BIGINT)                         AS nnf,
  CAST(n.ide.dhEmi AS TIMESTAMP)                    AS dh_emissao,
  CAST(left(n.ide.dhEmi, 10) AS DATE)               AS dt_emissao,     -- data local da emissão
  n.ide.natOp                                       AS nat_op,
  n.ide.tpNF                                        AS tp_nf,
  n.ide.finNFe                                      AS fin_nfe,
  n.ide.idDest                                      AS id_dest,
  n.ide.indFinal                                    AS ind_final,
  n.ide.indPres                                     AS ind_pres,
  n.ide.tpAmb                                       AS tp_amb,
  n.ide.NFref.refNFe                                AS chave_referenciada,
  n.emit.CNPJ                                       AS emit_cnpj,
  n.emit.xNome                                      AS emit_nome,
  n.emit.IE                                         AS emit_ie,
  n.emit.CRT                                        AS emit_crt,
  n.emit.enderEmit.UF                               AS emit_uf,
  n.emit.enderEmit.cMun                             AS emit_cod_municipio,
  n.emit.enderEmit.xMun                             AS emit_municipio,
  n.emit.enderEmit.xLgr                             AS emit_logradouro,
  n.emit.enderEmit.xBairro                          AS emit_bairro,
  n.emit.enderEmit.CEP                              AS emit_cep,
  CASE WHEN n.dest.CNPJ IS NOT NULL THEN 'CNPJ' ELSE 'CPF' END AS dest_tipo_doc,
  coalesce(n.dest.CNPJ, n.dest.CPF)                 AS dest_doc,
  n.dest.xNome                                      AS dest_nome,
  n.dest.IE                                         AS dest_ie,
  n.dest.indIEDest                                  AS dest_ind_ie,
  n.dest.enderDest.UF                               AS dest_uf,
  n.dest.enderDest.cMun                             AS dest_cod_municipio,
  n.dest.enderDest.xMun                             AS dest_municipio,
  n.dest.enderDest.xLgr                             AS dest_logradouro,
  n.dest.enderDest.xBairro                          AS dest_bairro,
  n.dest.enderDest.CEP                              AS dest_cep,
  CAST(n.total.ICMSTot.vProd      AS DECIMAL(15,2)) AS v_prod,
  CAST(n.total.ICMSTot.vBC        AS DECIMAL(15,2)) AS v_bc_icms,
  CAST(n.total.ICMSTot.vICMS      AS DECIMAL(15,2)) AS v_icms,
  CAST(n.total.ICMSTot.vICMSDeson AS DECIMAL(15,2)) AS v_icms_deson,
  CAST(n.total.ICMSTot.vIPI       AS DECIMAL(15,2)) AS v_ipi,
  CAST(n.total.ICMSTot.vPIS       AS DECIMAL(15,2)) AS v_pis,
  CAST(n.total.ICMSTot.vCOFINS    AS DECIMAL(15,2)) AS v_cofins,
  CAST(n.total.ICMSTot.vNF        AS DECIMAL(15,2)) AS v_nf,
  CAST(n.total.IBSCBSTot.vBCIBSCBS                AS DECIMAL(15,2)) AS v_bc_ibscbs,
  CAST(n.total.IBSCBSTot.gIBS.gIBSUF.vIBSUF       AS DECIMAL(15,2)) AS v_ibs_uf,
  CAST(n.total.IBSCBSTot.gIBS.gIBSUF.vDif         AS DECIMAL(15,2)) AS v_dif_ibs_uf,
  CAST(n.total.IBSCBSTot.gIBS.gIBSUF.vDevTrib     AS DECIMAL(15,2)) AS v_devtrib_ibs_uf,
  CAST(n.total.IBSCBSTot.gIBS.gIBSMun.vIBSMun     AS DECIMAL(15,2)) AS v_ibs_mun,
  CAST(n.total.IBSCBSTot.gIBS.gIBSMun.vDif        AS DECIMAL(15,2)) AS v_dif_ibs_mun,
  CAST(n.total.IBSCBSTot.gIBS.gIBSMun.vDevTrib    AS DECIMAL(15,2)) AS v_devtrib_ibs_mun,
  CAST(n.total.IBSCBSTot.gIBS.vIBS                AS DECIMAL(15,2)) AS v_ibs,
  CAST(n.total.IBSCBSTot.gIBS.vCredPres           AS DECIMAL(15,2)) AS v_credpres_ibs,
  CAST(n.total.IBSCBSTot.gCBS.vCBS                AS DECIMAL(15,2)) AS v_cbs,
  CAST(n.total.IBSCBSTot.gCBS.vDif                AS DECIMAL(15,2)) AS v_dif_cbs,
  CAST(n.total.IBSCBSTot.gCBS.vDevTrib            AS DECIMAL(15,2)) AS v_devtrib_cbs,
  CAST(n.total.IBSCBSTot.gCBS.vCredPres           AS DECIMAL(15,2)) AS v_credpres_cbs,
  CAST(n.total.ISTot.vIS                          AS DECIMAL(15,2)) AS v_is,
  CAST(n.total.vNFTot                             AS DECIMAL(15,2)) AS v_nf_tot,
  n.transp.modFrete                                 AS mod_frete,
  n.transp.transporta.CNPJ                          AS transp_cnpj,
  n.transp.transporta.xNome                         AS transp_nome,
  n.transp.transporta.UF                            AS transp_uf,
  CAST(aggregate(n.transp.vol, 0L, (acc, v) -> acc + CAST(v.qVol AS BIGINT)) AS INT) AS qtd_volumes,
  aggregate(n.transp.vol, CAST(0 AS DECIMAL(15,3)),
            (acc, v) -> CAST(acc + CAST(v.pesoB AS DECIMAL(15,3)) AS DECIMAL(15,3)))   AS peso_bruto,
  size(n.det)                                       AS qtd_itens,
  p.nProt                                           AS n_protocolo,
  CAST(p.dhRecbto AS TIMESTAMP)                     AS dh_autorizacao,
  p.cStat                                           AS c_stat,
  lote, arquivo, caminho, modificado_em, carregado_em,
  current_timestamp()                               AS processado_em
FROM base;

CREATE OR REFRESH STREAMING TABLE workspace.nfe_ldp_silver.nfe_nota
COMMENT 'nfe_nota (pipeline): vigente por chave, via AUTO CDC SCD1';

CREATE FLOW nfe_nota_cdc AS AUTO CDC INTO workspace.nfe_ldp_silver.nfe_nota
FROM STREAM(vw_nfe_nota)
KEYS (chave_acesso)
SEQUENCE BY STRUCT(modificado_em, arquivo)
STORED AS SCD TYPE 1;


-- ============================================================================ nfe_item
CREATE TEMPORARY VIEW vw_nfe_item AS
WITH base AS (
  SELECT chave_acesso, lote, arquivo, caminho, modificado_em, carregado_em,
         explode(doc.NFe.infNFe.det) AS det
  FROM STREAM(workspace.nfe_ldp_silver.nfe_doc)
)
SELECT
  chave_acesso,
  CAST(det.`_nItem` AS INT)                                   AS n_item,
  det.prod.cProd                                              AS cprod,
  det.prod.xProd                                              AS xprod,
  det.prod.NCM                                                AS ncm,
  det.prod.CFOP                                               AS cfop,
  det.prod.cBenef                                             AS c_benef,
  det.prod.uCom                                               AS ucom,
  CAST(det.prod.qCom   AS DECIMAL(15,4))                      AS qcom,
  CAST(det.prod.vUnCom AS DECIMAL(21,10))                     AS vuncom,
  CAST(det.prod.vProd  AS DECIMAL(15,2))                      AS vprod,
  -- ICMS: o grupo varia (ICMS00 | ICMS20 | ICMS40): coalesce entre eles
  coalesce(det.imposto.ICMS.ICMS00.CST, det.imposto.ICMS.ICMS20.CST, det.imposto.ICMS.ICMS40.CST) AS cst_icms,
  CAST(det.imposto.ICMS.ICMS20.pRedBC AS DECIMAL(7,4))        AS p_red_bc_icms,
  CAST(coalesce(det.imposto.ICMS.ICMS00.vBC,   det.imposto.ICMS.ICMS20.vBC)   AS DECIMAL(15,2)) AS vbc_icms,
  CAST(coalesce(det.imposto.ICMS.ICMS00.pICMS, det.imposto.ICMS.ICMS20.pICMS) AS DECIMAL(7,4))  AS p_icms,
  CAST(coalesce(det.imposto.ICMS.ICMS00.vICMS, det.imposto.ICMS.ICMS20.vICMS) AS DECIMAL(15,2)) AS v_icms,
  CAST(det.imposto.ICMS.ICMS20.vICMSDeson AS DECIMAL(15,2))   AS v_icms_deson,
  coalesce(det.imposto.IPI.IPITrib.CST, det.imposto.IPI.IPINT.CST)                      AS cst_ipi,
  CAST(det.imposto.IPI.IPITrib.vIPI AS DECIMAL(15,2))                                   AS v_ipi,
  coalesce(det.imposto.PIS.PISAliq.CST, det.imposto.PIS.PISNT.CST, det.imposto.PIS.PISOutr.CST) AS cst_pis,
  CAST(coalesce(det.imposto.PIS.PISAliq.vPIS, det.imposto.PIS.PISOutr.vPIS) AS DECIMAL(15,2))   AS v_pis,
  coalesce(det.imposto.COFINS.COFINSAliq.CST, det.imposto.COFINS.COFINSNT.CST, det.imposto.COFINS.COFINSOutr.CST) AS cst_cofins,
  CAST(coalesce(det.imposto.COFINS.COFINSAliq.vCOFINS, det.imposto.COFINS.COFINSOutr.vCOFINS) AS DECIMAL(15,2)) AS v_cofins,
  -- IBS/CBS (reforma tributária)
  det.imposto.IBSCBS.CST                                                     AS cst_ibscbs,
  det.imposto.IBSCBS.cClassTrib                                              AS cclasstrib,
  CAST(det.imposto.IBSCBS.gIBSCBS.vBC                     AS DECIMAL(15,2))  AS vbc_ibscbs,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSUF.pIBSUF           AS DECIMAL(7,4))   AS p_ibs_uf,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSUF.gRed.pRedAliq    AS DECIMAL(7,4))   AS p_red_aliq,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSUF.gRed.pAliqEfet   AS DECIMAL(7,4))   AS p_aliqefet_ibs_uf,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSUF.gDif.pDif        AS DECIMAL(7,4))   AS p_dif,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSUF.gDif.vDif        AS DECIMAL(15,2))  AS v_dif_ibs_uf,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSUF.gDevTrib.vDevTrib AS DECIMAL(15,2)) AS v_devtrib_ibs_uf,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSUF.vIBSUF           AS DECIMAL(15,2))  AS v_ibs_uf,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSMun.pIBSMun         AS DECIMAL(7,4))   AS p_ibs_mun,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSMun.gDif.vDif       AS DECIMAL(15,2))  AS v_dif_ibs_mun,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSMun.gDevTrib.vDevTrib AS DECIMAL(15,2)) AS v_devtrib_ibs_mun,
  CAST(det.imposto.IBSCBS.gIBSCBS.gIBSMun.vIBSMun         AS DECIMAL(15,2))  AS v_ibs_mun,
  CAST(det.imposto.IBSCBS.gIBSCBS.vIBS                    AS DECIMAL(15,2))  AS v_ibs,
  CAST(det.imposto.IBSCBS.gIBSCBS.gCBS.pCBS               AS DECIMAL(7,4))   AS p_cbs,
  CAST(det.imposto.IBSCBS.gIBSCBS.gCBS.gRed.pAliqEfet     AS DECIMAL(7,4))   AS p_aliqefet_cbs,
  CAST(det.imposto.IBSCBS.gIBSCBS.gCBS.gDif.vDif          AS DECIMAL(15,2))  AS v_dif_cbs,
  CAST(det.imposto.IBSCBS.gIBSCBS.gCBS.gDevTrib.vDevTrib  AS DECIMAL(15,2))  AS v_devtrib_cbs,
  CAST(det.imposto.IBSCBS.gIBSCBS.gCBS.vCBS               AS DECIMAL(15,2))  AS v_cbs,
  -- Imposto Seletivo (só quando há incidência)
  det.imposto.`IS`.CSTIS                                   AS cst_is,
  det.imposto.`IS`.cClassTribIS                            AS cclasstrib_is,
  CAST(det.imposto.`IS`.vBCIS AS DECIMAL(15,2))            AS v_bc_is,
  CAST(det.imposto.`IS`.pIS   AS DECIMAL(7,4))             AS p_is,
  CAST(det.imposto.`IS`.vIS   AS DECIMAL(15,2))            AS v_is,
  CAST(det.vItem AS DECIMAL(15,2))                         AS v_item,
  lote, arquivo, caminho, modificado_em, carregado_em,
  current_timestamp()                                      AS processado_em
FROM base;

CREATE OR REFRESH STREAMING TABLE workspace.nfe_ldp_silver.nfe_item
COMMENT 'nfe_item (pipeline): vigente por chave, via AUTO CDC SCD1';

CREATE FLOW nfe_item_cdc AS AUTO CDC INTO workspace.nfe_ldp_silver.nfe_item
FROM STREAM(vw_nfe_item)
KEYS (chave_acesso, n_item)
SEQUENCE BY STRUCT(modificado_em, arquivo)
STORED AS SCD TYPE 1;


-- ============================================================================ nfe_pagamento
CREATE TEMPORARY VIEW vw_nfe_pagamento AS
SELECT chave_acesso,
       pos + 1                              AS seq,
       pg.indPag                            AS ind_pag,
       pg.tPag                              AS t_pag,
       CAST(pg.vPag AS DECIMAL(15,2))       AS v_pag,
       lote, arquivo, caminho, modificado_em, carregado_em,
       current_timestamp()                  AS processado_em
FROM STREAM(workspace.nfe_ldp_silver.nfe_doc)
LATERAL VIEW posexplode(doc.NFe.infNFe.pag.detPag) AS pos, pg;

CREATE OR REFRESH STREAMING TABLE workspace.nfe_ldp_silver.nfe_pagamento
COMMENT 'nfe_pagamento (pipeline): vigente por chave, via AUTO CDC SCD1';

CREATE FLOW nfe_pagamento_cdc AS AUTO CDC INTO workspace.nfe_ldp_silver.nfe_pagamento
FROM STREAM(vw_nfe_pagamento)
KEYS (chave_acesso, seq)
SEQUENCE BY STRUCT(modificado_em, arquivo)
STORED AS SCD TYPE 1;


-- ============================================================================ nfe_duplicata
CREATE TEMPORARY VIEW vw_nfe_duplicata AS
SELECT chave_acesso,
       dp.nDup                                            AS n_dup,
       CAST(dp.dVenc AS DATE)                             AS d_venc,
       CAST(dp.vDup AS DECIMAL(15,2))                     AS v_dup,
       doc.NFe.infNFe.cobr.fat.nFat                       AS n_fat,
       CAST(doc.NFe.infNFe.cobr.fat.vOrig AS DECIMAL(15,2)) AS v_orig,
       CAST(doc.NFe.infNFe.cobr.fat.vLiq  AS DECIMAL(15,2)) AS v_liq,
       lote, arquivo, caminho, modificado_em, carregado_em,
       current_timestamp()                                AS processado_em
FROM STREAM(workspace.nfe_ldp_silver.nfe_doc)
LATERAL VIEW explode(doc.NFe.infNFe.cobr.dup) AS dp;

CREATE OR REFRESH STREAMING TABLE workspace.nfe_ldp_silver.nfe_duplicata
COMMENT 'nfe_duplicata (pipeline): vigente por chave, via AUTO CDC SCD1';

CREATE FLOW nfe_duplicata_cdc AS AUTO CDC INTO workspace.nfe_ldp_silver.nfe_duplicata
FROM STREAM(vw_nfe_duplicata)
KEYS (chave_acesso, n_dup)
SEQUENCE BY STRUCT(modificado_em, arquivo)
STORED AS SCD TYPE 1;
