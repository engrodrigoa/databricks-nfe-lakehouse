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

## D007 — Bronze guarda o XML bruto como STRING

**Contexto:** no marco 1, `read_files(..., inferColumnTypes => false)` devolveu `NFe` e `protNFe` como
STRING contendo o XML interno do elemento, e não struct (diferente de `spark.read` com `inferSchema=false`).
O Auto Loader se comporta como `read_files`.
**Decisão:** a bronze guarda `NFe`/`protNFe` como texto XML bruto. O parse tipado acontece na silver,
com `from_xml` + schema explícito do contrato (ou `xpath_*`).
**Consequência:** a bronze nunca quebra por mudança de layout (grupo novo da reforma = zero impacto na
ingestão) e mantém o documento íntegro para reprocessar, como o `landing_nfe.nfe_xml` do datalabs.
Custo: ~6–25 KB por linha (a assinatura digital é a maior parte). Substitui a parte "struct" de D003/D004.

## D008 — Log de carga em tabela de controle própria (opção B)

**Contexto:** o perfil por lote (contagem derivada da bronze) mostra o estado, mas não o histórico de execução.
**Decisão:** tabela `nfe_bronze.ctrl_lote` com início, fim, status e contagens por execução e por lote,
equivalente ao `ctrl.nfe_lote` do datalabs. Implementada no marco 6, junto com os Lakeflow Jobs.
Até lá, o log é a consulta derivada da bronze + inventário (`02c`).
**Consequência:** histórico de execuções para o painel de observabilidade e checagem de completude
(arquivos no Volume × linhas na bronze).

## D009 — Documentos e códigos sempre como STRING; CNPJ alfanumérico

**Contexto:** a IN RFB 2.229/2024 cria o CNPJ alfanumérico (12 posições alfanuméricas + 2 DVs numéricos)
para novas inscrições; o gerador já emite parte dos CNPJs nesse formato. CPF, IE, CEP, NCM, CFOP, CST e
cClassTrib têm zero à esquerda.
**Decisão:** todos como `STRING` em todas as camadas, nunca numérico. Tamanho e formato validados por
regra de qualidade (CHECK/expectation), não pelo tipo:
CNPJ `^[0-9A-Z]{12}[0-9]{2}$` + DV módulo 11 (valor do caractere = código ASCII − 48); CPF `^[0-9]{11}$`.
**Por que não `VARCHAR(14)`:** no Delta, `VARCHAR(n)` é STRING com checagem de tamanho na escrita.
Funciona, mas `dest_doc` guarda CNPJ (14) ou CPF (11), e o formato importa mais que o tamanho.
Uma regra explícita dá mensagem clara e vai para a quarentena, em vez de falhar a carga inteira.

## D010 — Bronze oficial: Auto Loader `text` + `wholeText` (`nfe_xml_raw`)

**Contexto (marco 2, 858 arquivos em 3 lotes):** os três métodos carregaram as mesmas linhas e nenhum
perdeu arquivo (inventário × tabela). A diferença foi o tratamento dos 5 truncados (lote_002 = 2, lote_003 = 3):
- `COPY INTO` (xml) e Auto Loader (xml): o truncado entra como **linha com tudo NULL**, sem marca e sem conteúdo
  (`num_skipped_corrupt_files = 0`, `_rescued_data` vazio). Só dá para perceber contando `NFe IS NULL`.
- Auto Loader (text): o arquivo entra **byte a byte**; `from_xml` PERMISSIVE separa o truncado em
  `_corrupt_record` **com o conteúdo**, pronto para quarentena e investigação.
**Decisão:** `workspace.nfe_bronze.nfe_xml_raw` é a bronze oficial. O parse vai para a silver (D007).
`nfe_xml_copyinto` e `nfe_xml_autoloader` são tabelas de estudo e podem ser removidas após o marco 3.
**Consequência:** "linha existe" ≠ "documento existe": a checagem de completude (inventário) e a de
integridade (`_corrupt_record`) são controles diferentes e os dois entram na `ctrl_lote` (D008).

## D011 — Silver em duas etapas: documento parseado (`nfe_doc`) → entidades por MERGE

**Contexto:** o parse do XML é a operação cara; nota, item, pagamento e duplicata saem do mesmo documento.
**Decisão:**
- `nfe_doc`: 1 linha por arquivo válido, com o `from_xml` feito **uma vez**. Todas as folhas STRING.
  O schema da coluna `doc` é o **contrato**: o 03b lê o schema da própria tabela para fazer o parse
- Mapeamento XML → colunas tipadas em **views SQL** (`vw_nfe_*`); processamento em **PySpark**
  (stream da bronze, `foreachBatch`, quarentena, dedup, `MERGE`)
- Tabelas finais criadas por CTAS vazio a partir das views (schema sem repetição) + `CLUSTER BY`
- Erro (documento inutilizável) → `nfe_quarentena` com o XML; aviso (inconsistência fiscal) → `vw_dq_alertas`
- Escritas do `foreachBatch` idempotentes com `txnAppId` + `txnVersion`
**Consequência:** reprocessar uma entidade não exige reparsear XML; mudança de layout = mudar o DDL de
`nfe_doc` + a view, sem tocar no código PySpark.

## D012 — Reenvio: vale o arquivo mais recente, por chave

**Decisão:** dentro do micro-batch, 1 vencedor por chave (`modificado_em`, `carregado_em`, `arquivo`);
entre micro-batches, o `MERGE` só atualiza se `s.modificado_em > t.modificado_em`. Itens, pagamentos e
duplicatas vêm só do arquivo vencedor.
**Limite conhecido:** se um reenvio tiver **menos** itens que a versão gravada, os itens excedentes ficam.
No gerador o reenvio é idêntico, então não ocorre. Correção futura: `WHEN NOT MATCHED BY SOURCE` restrito
às chaves do lote, ou delete+insert por chave numa transação.
