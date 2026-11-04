# Databricks notebook source
# MAGIC %md
# MAGIC # j06 · Job: qualidade do dado → ctrl_dq (D018)
# MAGIC Leva para dentro do Job o que antes só rodava à mão (03c alertas, 04c órfãos) e grava um **retrato por run**.
# MAGIC
# MAGIC ```
# MAGIC  severidade    camada  regras                                     fonte
# MAGIC  erro          silver  5 motivos de quarentena                    nfe_quarentena (detectado_em)
# MAGIC  aviso         silver  6 regras fiscais / de referência           vw_dq_alertas × nfe_nota (carregado_em)
# MAGIC  integridade   gold    6 chaves órfãs da fato                     ft_nfe_item × dimensões
# MAGIC ```
# MAGIC Duas medidas por regra:
# MAGIC - `qtd_carga`: o que **entrou nesta carga** (registro com data ≥ início da run) → eixo temporal do painel
# MAGIC - `qtd_total`: o **estado atual** da tabela inteira → número de hoje, independente de quando entrou
# MAGIC
# MAGIC Esta task **não falha** por achar problema no dado: problema de origem vai para o painel, não derruba a carga.
# MAGIC Ela só falha se o próprio código quebrar (aí é problema do processo).

# COMMAND ----------

dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("inicio_ms", "")
run_id = dbutils.widgets.get("run_id")
inicio_ms = dbutils.widgets.get("inicio_ms").strip()
INICIO = f"timestamp_millis({int(inicio_ms)})" if inicio_ms else "current_date()"   # à mão: o que entrou hoje
print("run_id:", run_id, "| início da carga:", spark.sql(f"SELECT {INICIO}").first()[0])

S, G = "workspace.nfe_silver", "workspace.nfe_gold"

# COMMAND ----------

# Cada regra: (severidade, camada, regra, SQL que devolve as linhas afetadas com uma coluna de data `quando`)
MOTIVOS = ["xml_corrompido", "chave_invalida", "chave_divergente_protocolo", "nao_autorizada", "cnpj_emitente_invalido"]
ALERTAS = ["tot_cbs_divergente", "vbc_ibscbs_divergente", "cclasstrib_incoerente",
           "ncm_fora_referencia", "cfop_fora_referencia", "municipio_emit_fora_referencia"]

FATO = f"""(SELECT f.*, n.carregado_em AS quando
            FROM {G}.ft_nfe_item f JOIN {S}.nfe_nota n USING (chave_acesso))"""
ORFAOS = {
    "sk_data":          f"LEFT ANTI JOIN {G}.dm_nfe_tempo d USING (sk_data)",
    "sk_operacao":      f"LEFT ANTI JOIN {G}.dm_nfe_operacao d USING (sk_operacao)",
    "sk_tributacao":    f"LEFT ANTI JOIN {G}.dm_nfe_tributacao d USING (sk_tributacao)",
    "ncm":              f"LEFT ANTI JOIN {G}.dm_nfe_ncm d USING (ncm)",
    "cfop":             f"LEFT ANTI JOIN {G}.dm_nfe_cfop d USING (cfop)",
    "cod_mun_emitente": f"LEFT ANTI JOIN {G}.dm_nfe_municipio d ON d.cod_municipio = f.cod_mun_emitente",
}

REGRAS = (
    [("erro", "silver", m,
      f"SELECT arquivo AS unidade, detectado_em AS quando FROM {S}.nfe_quarentena WHERE motivo = '{m}'")
     for m in MOTIVOS]
    + [("aviso", "silver", a,
        f"""SELECT DISTINCT a.chave_acesso AS unidade, n.carregado_em AS quando
            FROM {S}.vw_dq_alertas a JOIN {S}.nfe_nota n USING (chave_acesso) WHERE a.regra = '{a}'""")
       for a in ALERTAS]
    + [("integridade", "gold", f"orfao_{k}", f"SELECT 1 AS unidade, f.quando FROM {FATO} f {join}")
       for k, join in ORFAOS.items()]
)

linhas = []
for severidade, camada, regra, sql in REGRAS:
    r = spark.sql(f"""
        SELECT count(*) AS qtd_total, count_if(quando >= {INICIO}) AS qtd_carga FROM ({sql})
    """).first()
    linhas.append((severidade, camada, regra, int(r.qtd_carga), int(r.qtd_total)))

display(spark.createDataFrame(linhas, "severidade string, camada string, regra string, qtd_carga long, qtd_total long"))

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.nfe_bronze.ctrl_dq (
  run_id        STRING    COMMENT 'job run id: junta com a ctrl_lote',
  executado_em  TIMESTAMP,
  inicio_run    TIMESTAMP COMMENT 'início da run: corte entre "desta carga" e "anterior"',
  severidade    STRING    COMMENT 'erro (quarentena) | aviso (alerta fiscal) | integridade (órfão na gold)',
  camada        STRING,
  regra         STRING,
  qtd_carga     BIGINT    COMMENT 'registros afetados que entraram nesta carga',
  qtd_total     BIGINT    COMMENT 'registros afetados na tabela inteira (estado atual)'
)
COMMENT 'Retrato de qualidade por execução do Job (D018). Unidade: arquivo (erro), nota (aviso), item (integridade)'
""")

(spark.createDataFrame(linhas, "severidade string, camada string, regra string, qtd_carga long, qtd_total long")
 .selectExpr(f"'{run_id}' AS run_id", "current_timestamp() AS executado_em", f"CAST({INICIO} AS TIMESTAMP) AS inicio_run",
             "severidade", "camada", "regra", "qtd_carga", "qtd_total")
 .write.mode("append").saveAsTable("workspace.nfe_bronze.ctrl_dq"))

dbutils.jobs.taskValues.set(key="problemas_carga", value=sum(l[3] for l in linhas))
