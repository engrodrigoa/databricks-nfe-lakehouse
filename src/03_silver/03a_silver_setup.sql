-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03a · Silver: setup (referências, funções, tabelas e views de parse)
-- MAGIC
-- MAGIC ```
-- MAGIC  bronze.nfe_xml_raw ──(03b: from_xml)──► nfe_doc ──► vw_nfe_nota ──────► nfe_nota
-- MAGIC        (1 linha/arquivo)      │        (1 linha/      vw_nfe_item ──────► nfe_item
-- MAGIC                               │         doc válido)   vw_nfe_pagamento ─► nfe_pagamento
-- MAGIC                               ▼                       vw_nfe_duplicata ─► nfe_duplicata
-- MAGIC                        nfe_quarentena                  (SQL: este notebook)  (MERGE: 03b)
-- MAGIC ```
-- MAGIC
-- MAGIC **Divisão SQL × PySpark (combinada):** o *mapeamento* XML → colunas é SQL (views abaixo).
-- MAGIC O *processamento* (stream incremental, quarentena, dedup, MERGE) é PySpark (03b).
-- MAGIC
-- MAGIC Idempotente: pode rodar de novo. As tabelas usam `IF NOT EXISTS`; views e funções usam `OR REPLACE`.
-- MAGIC **Antes de rodar:** suba os 5 CSVs de `referencias/` para o Volume em `landing/ref/`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Tabelas de referência
-- MAGIC `inferColumnTypes => false`: tudo string. CFOP, NCM, CST e cClassTrib têm zero à esquerda (D009).

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.nfe_silver.ref_cfop COMMENT 'CFOP (referência sintética do projeto)' AS
SELECT cfop, descricao, sentido, abrangencia, grupo
FROM read_files('/Volumes/workspace/nfe_bronze/landing/ref/nfe_ref_cfop.csv',
                format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REPLACE TABLE workspace.nfe_silver.ref_ncm COMMENT 'NCM usados pelo gerador' AS
SELECT ncm, descricao_ncm, capitulo, descricao_capitulo
FROM read_files('/Volumes/workspace/nfe_bronze/landing/ref/nfe_ref_ncm.csv',
                format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REPLACE TABLE workspace.nfe_silver.ref_classtrib COMMENT 'Pares válidos CST × cClassTrib (IBS/CBS)' AS
SELECT cst, cclasstrib, descricao_cst, descricao_classtrib, tipo_tributacao,
       CAST(p_reducao_aliquota AS DECIMAL(7,4)) AS p_reducao_aliquota
FROM read_files('/Volumes/workspace/nfe_bronze/landing/ref/nfe_ref_classtrib.csv',
                format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REPLACE TABLE workspace.nfe_silver.ref_municipio COMMENT 'Municípios IBGE usados pelo gerador' AS
SELECT cod_municipio, nome_municipio, sigla_uf
FROM read_files('/Volumes/workspace/nfe_bronze/landing/ref/nfe_ref_municipio.csv',
                format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REPLACE TABLE workspace.nfe_silver.ref_uf COMMENT 'UFs (código IBGE)' AS
SELECT cod_uf, sigla_uf, nome_uf, regiao
FROM read_files('/Volumes/workspace/nfe_bronze/landing/ref/nfe_ref_uf.csv',
                format => 'csv', header => true, inferColumnTypes => false);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Função SQL no Unity Catalog: dígito verificador módulo 11
-- MAGIC Uma **SQL UDF** vira objeto do catálogo (`catálogo.schema.função`), com permissão e linhagem como uma tabela.
-- MAGIC Serve para a chave de acesso (43 → 1 DV) e para o CNPJ alfanumérico (12 → DV1, 13 → DV2):
-- MAGIC valor do caractere = ASCII − 48 (dígitos não mudam; `A` = 17), pesos 2..9 da direita para a esquerda.
-- MAGIC `((11 − soma % 11) % 11) % 10` resolve a regra "resto < 2 → 0" numa expressão só.
-- MAGIC Validado localmente contra o gerador: 600 DVs de CNPJ (metade alfanuméricos) + 360 chaves, 100% iguais.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.nfe_silver.fn_dv_mod11(corpo STRING)
RETURNS STRING
COMMENT 'DV módulo 11 (pesos 2..9, valor = ASCII - 48). Chave de acesso NF-e e CNPJ alfanumérico.'
RETURN CASE
  WHEN corpo IS NULL OR length(corpo) = 0 THEN NULL
  ELSE CAST(
    ((11 - aggregate(sequence(1, length(corpo)), 0,
                     (acc, i) -> acc + (ascii(substr(corpo, length(corpo) - i + 1, 1)) - 48) * ((i - 1) % 8 + 2)
                    ) % 11) % 11) % 10
    AS STRING)
END;

-- COMMAND ----------

-- Teste rápido: chave e CNPJ alfanumérico reais da massa
SELECT workspace.nfe_silver.fn_dv_mod11('5226102296965300011655001000030881132754094') AS dv_chave_deve_ser_1,
       workspace.nfe_silver.fn_dv_mod11('12ABC34501DE')                                AS dv1_cnpj_exemplo,
       workspace.nfe_silver.fn_dv_mod11('12ABC34501DE' || workspace.nfe_silver.fn_dv_mod11('12ABC34501DE')) AS dv2_cnpj_exemplo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. `nfe_doc`: o documento parseado (contrato de schema)
-- MAGIC 1 linha por **arquivo válido** (reenvios incluídos). A coluna `doc` é o struct do `from_xml`.
-- MAGIC **Todas as folhas são STRING**: a tipagem acontece nas views. Assim o parse nunca falha por tipo.
-- MAGIC O 03b lê o schema **desta tabela** para fazer o `from_xml`: o contrato fica num lugar só.
-- MAGIC Listas do XSD como `ARRAY`: `det`, `pag.detPag`, `cobr.dup`, `transp.vol` (mesmo quando vem 1 elemento).
-- MAGIC Fora do contrato: `Signature`, `infRespTec` (o `from_xml` ignora o que não está no schema).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.nfe_silver.nfe_doc (
  lote          STRING    COMMENT 'Pasta de lote no Volume',
  arquivo       STRING    COMMENT 'Nome do arquivo XML',
  caminho       STRING    COMMENT 'Caminho completo no Volume',
  tamanho_bytes BIGINT,
  modificado_em TIMESTAMP COMMENT 'Data do arquivo: desempate de reenvio',
  carregado_em  TIMESTAMP COMMENT 'Entrada na bronze',
  chave_acesso  STRING    COMMENT '44 posições, de infNFe/@Id',
  doc STRUCT<
    NFe: STRUCT<
      infNFe: STRUCT<
        `_Id`: STRING,
        `_versao`: STRING,
        cobr: STRUCT<
          dup: ARRAY<STRUCT<
            dVenc: STRING,
            nDup: STRING,
            vDup: STRING
          >>,
          fat: STRUCT<
            nFat: STRING,
            vDesc: STRING,
            vLiq: STRING,
            vOrig: STRING
          >
        >,
        dest: STRUCT<
          CNPJ: STRING,
          CPF: STRING,
          IE: STRING,
          enderDest: STRUCT<
            CEP: STRING,
            UF: STRING,
            cMun: STRING,
            cPais: STRING,
            nro: STRING,
            xBairro: STRING,
            xLgr: STRING,
            xMun: STRING,
            xPais: STRING
          >,
          indIEDest: STRING,
          xNome: STRING
        >,
        det: ARRAY<STRUCT<
          `_nItem`: STRING,
          imposto: STRUCT<
            COFINS: STRUCT<
              COFINSAliq: STRUCT<
                CST: STRING,
                pCOFINS: STRING,
                vBC: STRING,
                vCOFINS: STRING
              >,
              COFINSNT: STRUCT<
                CST: STRING
              >,
              COFINSOutr: STRUCT<
                CST: STRING,
                pCOFINS: STRING,
                vBC: STRING,
                vCOFINS: STRING
              >
            >,
            IBSCBS: STRUCT<
              CST: STRING,
              cClassTrib: STRING,
              gIBSCBS: STRUCT<
                gCBS: STRUCT<
                  gDevTrib: STRUCT<
                    vDevTrib: STRING
                  >,
                  gDif: STRUCT<
                    pDif: STRING,
                    vDif: STRING
                  >,
                  gRed: STRUCT<
                    pAliqEfet: STRING,
                    pRedAliq: STRING
                  >,
                  pCBS: STRING,
                  vCBS: STRING
                >,
                gIBSMun: STRUCT<
                  gDevTrib: STRUCT<
                    vDevTrib: STRING
                  >,
                  gDif: STRUCT<
                    pDif: STRING,
                    vDif: STRING
                  >,
                  gRed: STRUCT<
                    pAliqEfet: STRING,
                    pRedAliq: STRING
                  >,
                  pIBSMun: STRING,
                  vIBSMun: STRING
                >,
                gIBSUF: STRUCT<
                  gDevTrib: STRUCT<
                    vDevTrib: STRING
                  >,
                  gDif: STRUCT<
                    pDif: STRING,
                    vDif: STRING
                  >,
                  gRed: STRUCT<
                    pAliqEfet: STRING,
                    pRedAliq: STRING
                  >,
                  pIBSUF: STRING,
                  vIBSUF: STRING
                >,
                vBC: STRING,
                vIBS: STRING
              >
            >,
            ICMS: STRUCT<
              ICMS00: STRUCT<
                CST: STRING,
                modBC: STRING,
                orig: STRING,
                pICMS: STRING,
                vBC: STRING,
                vICMS: STRING
              >,
              ICMS20: STRUCT<
                CST: STRING,
                indDeduzDeson: STRING,
                modBC: STRING,
                motDesICMS: STRING,
                orig: STRING,
                pICMS: STRING,
                pRedBC: STRING,
                vBC: STRING,
                vICMS: STRING,
                vICMSDeson: STRING
              >,
              ICMS40: STRUCT<
                CST: STRING,
                orig: STRING
              >
            >,
            ICMSUFDest: STRUCT<
              pFCPUFDest: STRING,
              pICMSInter: STRING,
              pICMSInterPart: STRING,
              pICMSUFDest: STRING,
              vBCFCPUFDest: STRING,
              vBCUFDest: STRING,
              vFCPUFDest: STRING,
              vICMSUFDest: STRING,
              vICMSUFRemet: STRING
            >,
            IPI: STRUCT<
              IPINT: STRUCT<
                CST: STRING
              >,
              IPITrib: STRUCT<
                CST: STRING,
                pIPI: STRING,
                vBC: STRING,
                vIPI: STRING
              >,
              cEnq: STRING
            >,
            `IS`: STRUCT<
              CSTIS: STRING,
              cClassTribIS: STRING,
              pIS: STRING,
              vBCIS: STRING,
              vIS: STRING
            >,
            PIS: STRUCT<
              PISAliq: STRUCT<
                CST: STRING,
                pPIS: STRING,
                vBC: STRING,
                vPIS: STRING
              >,
              PISNT: STRUCT<
                CST: STRING
              >,
              PISOutr: STRUCT<
                CST: STRING,
                pPIS: STRING,
                vBC: STRING,
                vPIS: STRING
              >
            >
          >,
          impostoDevol: STRUCT<
            IPI: STRUCT<
              vIPIDevol: STRING
            >,
            pDevol: STRING
          >,
          infAdProd: STRING,
          prod: STRUCT<
            CFOP: STRING,
            NCM: STRING,
            cBenef: STRING,
            cEAN: STRING,
            cEANTrib: STRING,
            cProd: STRING,
            indTot: STRING,
            qCom: STRING,
            qTrib: STRING,
            uCom: STRING,
            uTrib: STRING,
            vProd: STRING,
            vUnCom: STRING,
            vUnTrib: STRING,
            xProd: STRING
          >,
          vItem: STRING
        >>,
        emit: STRUCT<
          CNPJ: STRING,
          CRT: STRING,
          IE: STRING,
          enderEmit: STRUCT<
            CEP: STRING,
            UF: STRING,
            cMun: STRING,
            cPais: STRING,
            nro: STRING,
            xBairro: STRING,
            xLgr: STRING,
            xMun: STRING,
            xPais: STRING
          >,
          xFant: STRING,
          xNome: STRING
        >,
        ide: STRUCT<
          NFref: STRUCT<
            refNFe: STRING
          >,
          cDV: STRING,
          cMunFG: STRING,
          cNF: STRING,
          cUF: STRING,
          dhEmi: STRING,
          dhSaiEnt: STRING,
          finNFe: STRING,
          idDest: STRING,
          indFinal: STRING,
          indIntermed: STRING,
          indPres: STRING,
          mod: STRING,
          nNF: STRING,
          natOp: STRING,
          procEmi: STRING,
          serie: STRING,
          tpAmb: STRING,
          tpEmis: STRING,
          tpImp: STRING,
          tpNF: STRING,
          verProc: STRING
        >,
        infAdic: STRUCT<
          infCpl: STRING
        >,
        pag: STRUCT<
          detPag: ARRAY<STRUCT<
            indPag: STRING,
            tPag: STRING,
            vPag: STRING
          >>
        >,
        total: STRUCT<
          IBSCBSTot: STRUCT<
            gCBS: STRUCT<
              vCBS: STRING,
              vCredPres: STRING,
              vCredPresCondSus: STRING,
              vDevTrib: STRING,
              vDif: STRING
            >,
            gIBS: STRUCT<
              gIBSMun: STRUCT<
                vDevTrib: STRING,
                vDif: STRING,
                vIBSMun: STRING
              >,
              gIBSUF: STRUCT<
                vDevTrib: STRING,
                vDif: STRING,
                vIBSUF: STRING
              >,
              vCredPres: STRING,
              vCredPresCondSus: STRING,
              vIBS: STRING
            >,
            vBCIBSCBS: STRING
          >,
          ICMSTot: STRUCT<
            vBC: STRING,
            vBCST: STRING,
            vCOFINS: STRING,
            vDesc: STRING,
            vFCP: STRING,
            vFCPST: STRING,
            vFCPSTRet: STRING,
            vFrete: STRING,
            vICMS: STRING,
            vICMSDeson: STRING,
            vII: STRING,
            vIPI: STRING,
            vIPIDevol: STRING,
            vNF: STRING,
            vOutro: STRING,
            vPIS: STRING,
            vProd: STRING,
            vST: STRING,
            vSeg: STRING
          >,
          ISTot: STRUCT<
            vIS: STRING
          >,
          vNFTot: STRING
        >,
        transp: STRUCT<
          modFrete: STRING,
          transporta: STRUCT<
            CNPJ: STRING,
            IE: STRING,
            UF: STRING,
            xEnder: STRING,
            xMun: STRING,
            xNome: STRING
          >,
          vol: ARRAY<STRUCT<
            esp: STRING,
            pesoB: STRING,
            pesoL: STRING,
            qVol: STRING
          >>
        >
      >
    >,
    protNFe: STRUCT<
      infProt: STRUCT<
        cStat: STRING,
        chNFe: STRING,
        dhRecbto: STRING,
        digVal: STRING,
        nProt: STRING,
        tpAmb: STRING,
        verAplic: STRING,
        xMotivo: STRING
      >
    >
  >,
  processado_em TIMESTAMP COMMENT 'Entrada na silver'
)
COMMENT 'NF-e parseada: 1 linha por arquivo válido (inclui reenvios). Fonte das views vw_nfe_*.';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.nfe_silver.nfe_quarentena (
  lote          STRING,
  arquivo       STRING,
  caminho       STRING,
  modificado_em TIMESTAMP,
  chave_acesso  STRING,
  motivo        STRING    COMMENT 'xml_corrompido | chave_invalida | chave_divergente_protocolo | nao_autorizada | cnpj_emitente_invalido',
  conteudo      STRING    COMMENT 'XML bruto, para investigar e reprocessar',
  detectado_em  TIMESTAMP
)
COMMENT 'Arquivos barrados na entrada da silver, com o conteúdo original.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Views de mapeamento (SQL): XML → colunas tipadas
-- MAGIC Atalhos: `n` = `doc.NFe.infNFe`, `p` = `doc.protNFe.infProt` (mesma notação do `contrato_dados.md`).
-- MAGIC `CAST` com ANSI ligado (padrão no serverless): número inválido **falha** a carga em vez de virar NULL.
-- MAGIC É uma escolha: `try_cast` aceitaria e esconderia o problema.

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.nfe_silver.vw_nfe_nota
COMMENT 'Cabeçalho da NF-e tipado (1 linha por arquivo válido; dedup no MERGE)'
AS
WITH base AS (
  SELECT *, doc.NFe.infNFe AS n, doc.protNFe.infProt AS p FROM workspace.nfe_silver.nfe_doc
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

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.nfe_silver.vw_nfe_item
COMMENT 'Itens (det) com todos os tributos: 1 linha por arquivo × nItem'
AS
WITH base AS (
  SELECT chave_acesso, lote, arquivo, caminho, modificado_em, carregado_em,
         explode(doc.NFe.infNFe.det) AS det
  FROM workspace.nfe_silver.nfe_doc
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

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.nfe_silver.vw_nfe_pagamento
COMMENT 'Formas de pagamento (pag/detPag): 1 linha por arquivo × sequência'
AS
SELECT chave_acesso,
       pos + 1                              AS seq,
       pg.indPag                            AS ind_pag,
       pg.tPag                              AS t_pag,
       CAST(pg.vPag AS DECIMAL(15,2))       AS v_pag,
       lote, arquivo, caminho, modificado_em, carregado_em,
       current_timestamp()                  AS processado_em
FROM workspace.nfe_silver.nfe_doc
LATERAL VIEW posexplode(doc.NFe.infNFe.pag.detPag) AS pos, pg;

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.nfe_silver.vw_nfe_duplicata
COMMENT 'Fatura e duplicatas (cobr): 1 linha por arquivo × parcela'
AS
SELECT chave_acesso,
       dp.nDup                                            AS n_dup,
       CAST(dp.dVenc AS DATE)                             AS d_venc,
       CAST(dp.vDup AS DECIMAL(15,2))                     AS v_dup,
       doc.NFe.infNFe.cobr.fat.nFat                       AS n_fat,
       CAST(doc.NFe.infNFe.cobr.fat.vOrig AS DECIMAL(15,2)) AS v_orig,
       CAST(doc.NFe.infNFe.cobr.fat.vLiq  AS DECIMAL(15,2)) AS v_liq,
       lote, arquivo, caminho, modificado_em, carregado_em,
       current_timestamp()                                AS processado_em
FROM workspace.nfe_silver.nfe_doc
LATERAL VIEW explode(doc.NFe.infNFe.cobr.dup) AS dp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Tabelas finais da silver
-- MAGIC - **CTAS vazio** (`AS SELECT * FROM vw ... WHERE false`): o schema nasce da view, sem repetir 70 colunas
-- MAGIC - **`CLUSTER BY`** = *Liquid Clustering* (seção 6 da prova): organiza os arquivos pelas colunas mais filtradas,
-- MAGIC   substitui `PARTITION BY` + `ZORDER`. Nota por data de emissão; item e afins pela chave (lado do MERGE)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.nfe_silver.nfe_nota CLUSTER BY (dt_emissao)
COMMENT 'NF-e: 1 linha por chave de acesso (reenvio: vale o mais recente)'
AS SELECT * FROM workspace.nfe_silver.vw_nfe_nota WHERE false;

CREATE TABLE IF NOT EXISTS workspace.nfe_silver.nfe_item CLUSTER BY (chave_acesso)
COMMENT 'Itens da NF-e: 1 linha por chave + nItem'
AS SELECT * FROM workspace.nfe_silver.vw_nfe_item WHERE false;

CREATE TABLE IF NOT EXISTS workspace.nfe_silver.nfe_pagamento CLUSTER BY (chave_acesso)
COMMENT 'Pagamentos: 1 linha por chave + seq'
AS SELECT * FROM workspace.nfe_silver.vw_nfe_pagamento WHERE false;

CREATE TABLE IF NOT EXISTS workspace.nfe_silver.nfe_duplicata CLUSTER BY (chave_acesso)
COMMENT 'Duplicatas: 1 linha por chave + nDup'
AS SELECT * FROM workspace.nfe_silver.vw_nfe_duplicata WHERE false;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Constraints Delta (rede de segurança)
-- MAGIC A quarentena (03b) barra o documento inválido **antes**. A constraint garante que nada inválido entre
-- MAGIC por outro caminho (um INSERT manual, um bug). Violação = **a escrita inteira falha** (diferente da quarentena).
-- MAGIC `DROP ... IF EXISTS` + `ADD`: mantém o notebook idempotente.

-- COMMAND ----------

ALTER TABLE workspace.nfe_silver.nfe_nota ALTER COLUMN chave_acesso SET NOT NULL;
ALTER TABLE workspace.nfe_silver.nfe_nota DROP CONSTRAINT IF EXISTS ck_nota_chave;
ALTER TABLE workspace.nfe_silver.nfe_nota ADD CONSTRAINT ck_nota_chave CHECK (length(chave_acesso) = 44);
ALTER TABLE workspace.nfe_silver.nfe_nota DROP CONSTRAINT IF EXISTS ck_nota_emit_cnpj;
ALTER TABLE workspace.nfe_silver.nfe_nota ADD CONSTRAINT ck_nota_emit_cnpj CHECK (emit_cnpj RLIKE '^[0-9A-Z]{12}[0-9]{2}$');
ALTER TABLE workspace.nfe_silver.nfe_nota DROP CONSTRAINT IF EXISTS ck_nota_modelo;
ALTER TABLE workspace.nfe_silver.nfe_nota ADD CONSTRAINT ck_nota_modelo CHECK (modelo = '55');

ALTER TABLE workspace.nfe_silver.nfe_item ALTER COLUMN chave_acesso SET NOT NULL;
ALTER TABLE workspace.nfe_silver.nfe_item ALTER COLUMN n_item SET NOT NULL;
ALTER TABLE workspace.nfe_silver.nfe_item DROP CONSTRAINT IF EXISTS ck_item_ncm;
ALTER TABLE workspace.nfe_silver.nfe_item ADD CONSTRAINT ck_item_ncm CHECK (ncm RLIKE '^[0-9]{8}$');
ALTER TABLE workspace.nfe_silver.nfe_item DROP CONSTRAINT IF EXISTS ck_item_n;
ALTER TABLE workspace.nfe_silver.nfe_item ADD CONSTRAINT ck_item_n CHECK (n_item BETWEEN 1 AND 990);

-- COMMAND ----------

-- Conferência: constraints aparecem nas propriedades da tabela
SHOW TBLPROPERTIES workspace.nfe_silver.nfe_nota;

-- COMMAND ----------

SHOW TABLES IN workspace.nfe_silver;

-- COMMAND ----------

SHOW VIEWS IN workspace.nfe_silver;
