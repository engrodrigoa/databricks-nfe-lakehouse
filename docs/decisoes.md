# Decisões de arquitetura

Formato curto: contexto → decisão → consequência. Nova decisão = nova entrada; nunca reescrever as antigas.

## D001 — Ferramentas nativas do Databricks primeiro

**Contexto:** objetivo é a certificação Data Engineer Associate; a prova cobra Spark SQL/PySpark,
Auto Loader, Delta, Lakeflow Declarative Pipelines, Jobs e Unity Catalog. dbt e Polars não caem.
**Decisão:** implementar tudo com recursos nativos. dbt-databricks fica como comparativo futuro.
**Consequência:** a lógica do datalabs é reaproveitada como especificação, não como código.

## D002 — Gerador fora do Databricks

**Decisão:** o gerador roda local e simula o sistema de origem; o projeto começa no Volume.
**Consequência:** o Databricks só "recebe arquivos", como num cenário real.

## D003 — Bronze sem inferência de tipos (`inferSchema = false`)

**Contexto:** testado localmente (Spark 4.0): com inferência, `CST 000` vira `0`, CEP/CPF/IE perdem o
zero à esquerda, NCM/CFOP viram número e valores monetários viram `double`.
**Decisão:** bronze guarda tudo como string; a tipagem (decimal, date, timestamp) é explícita na silver.
**Consequência:** bronze nunca perde informação; o contrato de tipos fica num lugar só.

## D004 — `rowTag = nfeProc`

**Decisão:** uma linha por arquivo, com `NFe` e `protNFe` (protocolo de autorização) juntos.
**Consequência:** `det`, `pag.detPag` e `cobr.dup` chegam como arrays/structs e são explodidos na silver.
Risco: se uma amostra só tiver notas com 1 item, `det` pode ser inferido como struct e não array.
Mitigar com schema explícito/`schemaHints` antes da carga contínua.

## D005 — Massa por lotes no Volume

**Decisão:** `landing/xml/lote_NNN/`. Cada lote é gerado local, enviado inteiro e nunca alterado.
**Consequência:** o lote vira a unidade de carga e de log (contagem por lote, rejeitados, reenvios),
equivalente ao `ctrl.nfe_lote` do datalabs.

## D006 — Unity Catalog: catálogo `workspace`, um schema por camada

**Decisão:** `workspace.nfe_bronze`, `workspace.nfe_silver`, `workspace.nfe_gold`
(Free Edition: o catálogo padrão é `workspace`).
**Consequência:** se o ambiente permitir `CREATE CATALOG`, migrar para um catálogo `nfe` é só trocar o prefixo.
