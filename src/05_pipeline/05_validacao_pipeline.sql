-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Pipeline declarativo: criação, execução e comparação com os notebooks
-- MAGIC
-- MAGIC ```
-- MAGIC  landing/xml ─► nfe_ldp_bronze.nfe_xml_raw ─► nfe_ldp_silver.nfe_doc ─┬─► nota · item · pag · dup (AUTO CDC SCD1)
-- MAGIC                                               └─► nfe_quarentena    ├─► dm_nfe_participante_hist (AUTO CDC SCD2)
-- MAGIC                                                                     └─► gold: dm_participante · ft_nfe_item · ag (MV)
-- MAGIC  Um "Start" executa o grafo inteiro, na ordem das dependências.
-- MAGIC ```
-- MAGIC
-- MAGIC ## Criar o pipeline (uma vez, pela UI)
-- MAGIC 1. **Jobs & Pipelines → Create → ETL pipeline**
-- MAGIC 2. Nome: `nfe_ldp`
-- MAGIC 3. **Add existing assets** (ou *Source code*): pasta `src/05_pipeline/transformations` do Git folder
-- MAGIC    (os 5 arquivos `01_` a `05_`; este notebook fica **fora**: não é código do pipeline)
-- MAGIC 4. Default catalog `workspace`, default schema `nfe_ldp_silver` (os arquivos usam nomes completos)
-- MAGIC 5. Compute: **Serverless** · Pipeline mode: **Triggered**
-- MAGIC 6. **Dry run** (valida o grafo sem processar) → depois **Start** (ou *Run pipeline*)
-- MAGIC
-- MAGIC | Conceito de prova | O que é |
-- MAGIC |---|---|
-- MAGIC | **Triggered** × **Continuous** | processa o pendente e para × fica ligado esperando dado novo |
-- MAGIC | **Dry run / Validate** | analisa o código e monta o grafo, sem ler dados |
-- MAGIC | **Start (update)** × **Full refresh** | incremental do ponto onde parou × apaga o estado e reprocessa tudo |
-- MAGIC | **Development** × **Production** | reusa recursos e não faz retry × retry automático e recursos novos a cada update |
-- MAGIC
-- MAGIC ⚠️ **Full refresh** numa streaming table reprocessa da fonte. Se a fonte já não tiver os arquivos, o dado se perde.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Notebooks × pipeline: mesmos números?
-- MAGIC Esperado: `diferenca = 0` em tudo (as duas implementações leem os mesmos arquivos do Volume).

-- COMMAND ----------

SELECT camada, notebook, pipeline, notebook - pipeline AS diferenca
FROM (
  SELECT 'bronze arquivos' AS camada,
         (SELECT count(*) FROM workspace.nfe_bronze.nfe_xml_raw)         AS notebook,
         (SELECT count(*) FROM workspace.nfe_ldp_bronze.nfe_xml_raw)     AS pipeline
  UNION ALL SELECT 'quarentena',
         (SELECT count(*) FROM workspace.nfe_silver.nfe_quarentena),
         (SELECT count(*) FROM workspace.nfe_ldp_silver.nfe_quarentena)
  UNION ALL SELECT 'nfe_doc',
         (SELECT count(*) FROM workspace.nfe_silver.nfe_doc),
         (SELECT count(*) FROM workspace.nfe_ldp_silver.nfe_doc)
  UNION ALL SELECT 'nfe_nota',
         (SELECT count(*) FROM workspace.nfe_silver.nfe_nota),
         (SELECT count(*) FROM workspace.nfe_ldp_silver.nfe_nota)
  UNION ALL SELECT 'nfe_item',
         (SELECT count(*) FROM workspace.nfe_silver.nfe_item),
         (SELECT count(*) FROM workspace.nfe_ldp_silver.nfe_item)
  UNION ALL SELECT 'nfe_pagamento',
         (SELECT count(*) FROM workspace.nfe_silver.nfe_pagamento),
         (SELECT count(*) FROM workspace.nfe_ldp_silver.nfe_pagamento)
  UNION ALL SELECT 'nfe_duplicata',
         (SELECT count(*) FROM workspace.nfe_silver.nfe_duplicata),
         (SELECT count(*) FROM workspace.nfe_ldp_silver.nfe_duplicata)
  UNION ALL SELECT 'ft_nfe_item',
         (SELECT count(*) FROM workspace.nfe_gold.ft_nfe_item),
         (SELECT count(*) FROM workspace.nfe_ldp_gold.ft_nfe_item)
  UNION ALL SELECT 'v_cbs agregado',
         (SELECT sum(v_cbs) FROM workspace.nfe_gold.ag_nfe_ibscbs_mensal),
         (SELECT sum(v_cbs) FROM workspace.nfe_ldp_gold.ag_nfe_ibscbs_mensal)
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Se a gold dos notebooks estiver atrás (lote novo sem `REFRESH` no 04c), a diferença aparece só nela.
-- MAGIC Esse é o ponto: no pipeline, a gold anda junto com a silver no mesmo `Start`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. SCD tipo 2: o histórico do participante

-- COMMAND ----------

SELECT count(DISTINCT doc_participante)                 AS participantes,
       count(*)                                         AS versoes,
       count(DISTINCT doc_participante) FILTER (WHERE NOT fl_vigente) AS com_mudanca_cadastral
FROM workspace.nfe_ldp_gold.dm_nfe_participante;

-- COMMAND ----------

-- Linha do tempo de quem mudou: o que mudou e a partir de quando
SELECT doc_participante, nome, municipio, logradouro, bairro, cep, vigencia_inicio, vigencia_fim, fl_vigente
FROM workspace.nfe_ldp_gold.dm_nfe_participante
WHERE doc_participante IN (SELECT doc_participante FROM workspace.nfe_ldp_gold.dm_nfe_participante
                           WHERE NOT fl_vigente)
ORDER BY doc_participante, vigencia_inicio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Point-in-time na fato
-- MAGIC A mesma empresa, notas antes e depois da mudança: cada uma aponta para a versão vigente **na emissão**.
-- MAGIC É o que permite responder "qual era a razão social/endereço quando a nota foi emitida?".

-- COMMAND ----------

SELECT f.emit_cnpj, p.nome, p.municipio, p.vigencia_inicio, p.vigencia_fim,
       count(DISTINCT f.chave_acesso) AS notas, min(f.dh_emissao) AS primeira, max(f.dh_emissao) AS ultima
FROM workspace.nfe_ldp_gold.ft_nfe_item f
JOIN workspace.nfe_ldp_gold.dm_nfe_participante p ON p.sk_participante = f.sk_emitente
WHERE f.emit_cnpj IN (SELECT doc_participante FROM workspace.nfe_ldp_gold.dm_nfe_participante WHERE NOT fl_vigente)
GROUP BY ALL
ORDER BY f.emit_cnpj, p.vigencia_inicio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. AUTO CDC × janela (as duas formas de SCD2)
-- MAGIC Como a `nfe_doc` guarda todos os documentos, o SCD2 também pode ser **derivado** com `lag()`:
-- MAGIC abre versão nova quando o cadastro difere do anterior (ordem por data de emissão).
-- MAGIC Validado localmente numa massa de 3.000 notas: 1.744 participantes, 19 com mudança, 0 "vai-e-volta".
-- MAGIC Esperado aqui: `versoes_janela = versoes_auto_cdc`.

-- COMMAND ----------

WITH eventos AS (
  SELECT doc.NFe.infNFe.emit.CNPJ AS doc_participante, CAST(doc.NFe.infNFe.ide.dhEmi AS TIMESTAMP) AS dh,
         xxhash64(doc.NFe.infNFe.emit.xNome, doc.NFe.infNFe.emit.IE, doc.NFe.infNFe.emit.enderEmit.UF,
                  doc.NFe.infNFe.emit.enderEmit.cMun, doc.NFe.infNFe.emit.enderEmit.xMun,
                  doc.NFe.infNFe.emit.enderEmit.xLgr, doc.NFe.infNFe.emit.enderEmit.nro,
                  doc.NFe.infNFe.emit.enderEmit.xBairro, doc.NFe.infNFe.emit.enderEmit.CEP) AS h
  FROM workspace.nfe_ldp_silver.nfe_doc
  UNION ALL
  SELECT coalesce(doc.NFe.infNFe.dest.CNPJ, doc.NFe.infNFe.dest.CPF), CAST(doc.NFe.infNFe.ide.dhEmi AS TIMESTAMP),
         xxhash64(doc.NFe.infNFe.dest.xNome, doc.NFe.infNFe.dest.IE, doc.NFe.infNFe.dest.enderDest.UF,
                  doc.NFe.infNFe.dest.enderDest.cMun, doc.NFe.infNFe.dest.enderDest.xMun,
                  doc.NFe.infNFe.dest.enderDest.xLgr, doc.NFe.infNFe.dest.enderDest.nro,
                  doc.NFe.infNFe.dest.enderDest.xBairro, doc.NFe.infNFe.dest.enderDest.CEP)
  FROM workspace.nfe_ldp_silver.nfe_doc
),
marcado AS (
  SELECT *, lag(h) OVER (PARTITION BY doc_participante ORDER BY dh) AS h_anterior FROM eventos
)
SELECT (SELECT count_if(h_anterior IS NULL OR h <> h_anterior) FROM marcado)       AS versoes_janela,
       (SELECT count(*) FROM workspace.nfe_ldp_gold.dm_nfe_participante_hist)       AS versoes_auto_cdc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | | Janela (`lag`) sobre a fonte | `AUTO CDC ... SCD TYPE 2` |
-- MAGIC |---|---|---|
-- MAGIC | Precisa da fonte com todo o histórico | **sim** | não (guarda o estado) |
-- MAGIC | Custo a cada carga | relê tudo | só o que chegou |
-- MAGIC | Dado fora de ordem | resolvido pela ordenação | resolvido pelo `SEQUENCE BY` |
-- MAGIC | Fonte que sobrescreve (cadastro atual) | **não funciona** | funciona |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Expectations: o que o pipeline mediu
-- MAGIC O `event_log` registra, por flow e por execução, quantas linhas passaram e quantas violaram cada regra.
-- MAGIC Esperado: `nfe_aprovado` com descartes = truncados; `tributacao_conhecida` com falhas = itens de cClassTrib incoerente.

-- COMMAND ----------

SELECT timestamp,
       origin.flow_name                                                  AS flow,
       details:flow_progress.metrics.num_output_rows::BIGINT             AS linhas_saida,
       details:flow_progress.data_quality.dropped_records::BIGINT        AS descartadas,
       details:flow_progress.data_quality.expectations                   AS expectations
FROM event_log(TABLE(workspace.nfe_ldp_silver.nfe_doc))
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Incremental: lote novo
-- MAGIC ```
-- MAGIC  1. python gerador\gen_nfe.py --inbox data\inbox\lote_005 --n 500 --sujo --manifesto data\manifesto_lote_005.csv
-- MAGIC  2. Volume: landing/xml/lote_005/ (os ARQUIVOS)
-- MAGIC  3. Pipeline nfe_ldp → Start          (uma ação: bronze → silver → SCD2 → gold)
-- MAGIC  4. Aqui: seção 1 → a coluna "pipeline" anda; a "notebook" só anda rodando 02b + 03b + REFRESH
-- MAGIC ```
-- MAGIC No grafo do pipeline, cada nó mostra linhas lidas/escritas **desta** execução: só o lote_005.
