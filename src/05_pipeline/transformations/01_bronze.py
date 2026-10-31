# Lakeflow Declarative Pipeline · BRONZE
# Mesmo desenho do 02b (B2, D010): Auto Loader lendo o arquivo inteiro como texto.
# Diferença: sem checkpointLocation/schemaLocation e sem trigger. O pipeline gerencia o estado
# e decide quando rodar (Triggered ou Continuous, na configuração do pipeline).

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ORIGEM = "/Volumes/workspace/nfe_bronze/landing/xml/"


@dp.table(
    name="workspace.nfe_ldp_bronze.nfe_xml_raw",
    comment="Bronze (pipeline): 1 linha por arquivo XML, conteúdo bruto",
)
def nfe_xml_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholeText", "true")
        .option("pathGlobFilter", "*.xml")
        .load(ORIGEM)
        .select(
            F.col("value").alias("xml"),
            F.col("_metadata.file_name").alias("arquivo"),
            F.col("_metadata.file_path").alias("caminho"),
            F.col("_metadata.file_size").alias("tamanho_bytes"),
            F.col("_metadata.file_modification_time").alias("modificado_em"),
            F.regexp_extract("_metadata.file_path", r"/(lote_[^/]+)/", 1).alias("lote"),
            F.current_timestamp().alias("carregado_em"),
        )
    )
