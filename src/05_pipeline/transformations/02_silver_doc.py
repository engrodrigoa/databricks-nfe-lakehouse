# Lakeflow Declarative Pipeline · SILVER (documento)
#
#   nfe_xml_raw ─► nfe_classificado (view) ─┬─► nfe_aprovado (view, EXPECT ... DROP) ─► nfe_doc
#                  from_xml + motivo         └─► nfe_quarentena (motivo IS NOT NULL)
#
# Padrão "quarentena" em pipelines: uma expectation DROP no caminho bom (conta as descartadas no
# event log) + uma tabela com o filtro inverso (guarda o que foi descartado, com o XML).
# Equivale ao foreachBatch do 03b, sem txnAppId, sem checkpoint e sem 2 writes manuais.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

S = "workspace.nfe_ldp_silver"

# Contrato do parse (D011): o schema vem da tabela nfe_doc dos notebooks. Uma fonte da verdade
# para as duas implementações.
DOC_SCHEMA = spark.table("workspace.nfe_silver.nfe_doc").schema["doc"].dataType
PARSE_SCHEMA = StructType(DOC_SCHEMA.fields + [StructField("_corrupt_record", StringType())])
OPCOES_XML = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
DV = "workspace.nfe_silver.fn_dv_mod11"  # SQL UDF do Unity Catalog (03a)


def motivo_quarentena():
    chave, cnpj = F.col("chave_acesso"), F.col("p.NFe.infNFe.emit.CNPJ")
    dv = lambda c: F.expr(f"{DV}({c})")  # noqa: E731
    return (
        F.when(F.col("p._corrupt_record").isNotNull() | F.col("p.NFe").isNull(), "xml_corrompido")
        .when((F.length(chave) != 44) | (F.substring(chave, 44, 1) != dv("substr(chave_acesso, 1, 43)")),
              "chave_invalida")
        .when(chave != F.col("p.protNFe.infProt.chNFe"), "chave_divergente_protocolo")
        .when(F.coalesce(F.col("p.protNFe.infProt.cStat"), F.lit("")) != "100", "nao_autorizada")
        .when(~cnpj.rlike("^[0-9A-Z]{12}[0-9]{2}$")
              | (F.substring(cnpj, 13, 1) != dv("substr(p.NFe.infNFe.emit.CNPJ, 1, 12)"))
              | (F.substring(cnpj, 14, 1) != dv("substr(p.NFe.infNFe.emit.CNPJ, 1, 13)")),
              "cnpj_emitente_invalido")
    )


@dp.temporary_view(name="nfe_classificado")
def nfe_classificado():
    return (
        spark.readStream.table("workspace.nfe_ldp_bronze.nfe_xml_raw")
        .withColumn("p", F.from_xml("xml", PARSE_SCHEMA, OPCOES_XML))
        .withColumn("chave_acesso", F.substring("p.NFe.infNFe._Id", 4, 44))
        .withColumn("motivo", motivo_quarentena())
    )


@dp.temporary_view(name="nfe_aprovado")
@dp.expect_or_drop("documento_aprovado", "motivo IS NULL")   # métrica de descarte no event log
def nfe_aprovado():
    return spark.readStream.table("nfe_classificado")


@dp.table(name=f"{S}.nfe_doc", comment="NF-e parseada (pipeline): 1 linha por arquivo válido")
@dp.expect("tem_itens", "size(doc.NFe.infNFe.det) > 0")       # WARN: só registra
def nfe_doc():
    return spark.readStream.table("nfe_aprovado").select(
        "lote", "arquivo", "caminho", "tamanho_bytes", "modificado_em", "carregado_em", "chave_acesso",
        F.struct(*[F.col(f"p.{f.name}") for f in DOC_SCHEMA.fields]).alias("doc"),
        F.current_timestamp().alias("processado_em"),
    )


@dp.table(name=f"{S}.nfe_quarentena", comment="Arquivos barrados (pipeline), com o XML original")
def nfe_quarentena():
    return (
        spark.readStream.table("nfe_classificado")
        .filter("motivo IS NOT NULL")
        .select("lote", "arquivo", "caminho", "modificado_em", "chave_acesso", "motivo",
                F.col("xml").alias("conteudo"), F.current_timestamp().alias("detectado_em"))
    )
