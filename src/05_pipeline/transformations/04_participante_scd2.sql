-- Lakeflow Declarative Pipeline · PARTICIPANTE (SCD TYPE 2)
--
--   nfe_doc ─STREAM─► vw_participante_eventos ─AUTO CDC SCD TYPE 2─► dm_nfe_participante_hist
--   (cada nota "fotografa" o cadastro            1 linha por versão: __START_AT / __END_AT
--    do emitente e do destinatário na data)
--
-- KEYS (doc_participante): CNPJ ou CPF (string, D009: CNPJ alfanumérico)
-- SEQUENCE BY dh_emissao: a ordem do histórico é a data do documento, não a de chegada.
--   Nota antiga chegando hoje (lote_004) entra na posição certa da linha do tempo.
-- TRACK HISTORY ON: só mudança de cadastro abre versão nova; tipo_doc nunca muda.
-- Por que STREAMING TABLE e não MV: SCD2 precisa de ESTADO (lembrar a versão anterior para fechá-la).
-- Aqui a fonte (nfe_doc) também guarda todo o histórico, então daria para derivar com janela (03c do
-- marco 5 compara as duas formas). Com fonte que sobrescreve o cadastro, só a forma com estado funciona.

CREATE TEMPORARY VIEW vw_participante_eventos AS
SELECT doc.NFe.infNFe.emit.CNPJ                       AS doc_participante,
       'CNPJ'                                         AS tipo_doc,
       doc.NFe.infNFe.emit.xNome                      AS nome,
       doc.NFe.infNFe.emit.IE                         AS ie,
       doc.NFe.infNFe.emit.enderEmit.UF               AS uf,
       doc.NFe.infNFe.emit.enderEmit.cMun             AS cod_municipio,
       doc.NFe.infNFe.emit.enderEmit.xMun             AS municipio,
       doc.NFe.infNFe.emit.enderEmit.xLgr             AS logradouro,
       doc.NFe.infNFe.emit.enderEmit.nro              AS numero,
       doc.NFe.infNFe.emit.enderEmit.xBairro          AS bairro,
       doc.NFe.infNFe.emit.enderEmit.CEP              AS cep,
       CAST(doc.NFe.infNFe.ide.dhEmi AS TIMESTAMP)    AS dh_emissao
FROM STREAM(workspace.nfe_ldp_silver.nfe_doc)
UNION ALL
SELECT coalesce(doc.NFe.infNFe.dest.CNPJ, doc.NFe.infNFe.dest.CPF),
       CASE WHEN doc.NFe.infNFe.dest.CNPJ IS NOT NULL THEN 'CNPJ' ELSE 'CPF' END,
       doc.NFe.infNFe.dest.xNome,
       doc.NFe.infNFe.dest.IE,
       doc.NFe.infNFe.dest.enderDest.UF,
       doc.NFe.infNFe.dest.enderDest.cMun,
       doc.NFe.infNFe.dest.enderDest.xMun,
       doc.NFe.infNFe.dest.enderDest.xLgr,
       doc.NFe.infNFe.dest.enderDest.nro,
       doc.NFe.infNFe.dest.enderDest.xBairro,
       doc.NFe.infNFe.dest.enderDest.CEP,
       CAST(doc.NFe.infNFe.ide.dhEmi AS TIMESTAMP)
FROM STREAM(workspace.nfe_ldp_silver.nfe_doc);

CREATE OR REFRESH STREAMING TABLE workspace.nfe_ldp_gold.dm_nfe_participante_hist
COMMENT 'Participante (emitente/destinatário) com histórico de cadastro: SCD tipo 2 via AUTO CDC';

CREATE FLOW participante_scd2 AS AUTO CDC INTO workspace.nfe_ldp_gold.dm_nfe_participante_hist
FROM STREAM(vw_participante_eventos)
KEYS (doc_participante)
SEQUENCE BY dh_emissao
COLUMNS * EXCEPT (dh_emissao)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (tipo_doc);
