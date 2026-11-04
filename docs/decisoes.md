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

## D013 — Gold: TABLE para dimensões, MATERIALIZED VIEW para fatos e agregado, VIEW para consumo

**Contexto:** a seção 3 da prova cobra a escolha do objeto de gold (table, view, materialized view, streaming table).
**Decisão:**
- Dimensões (`dm_*`): `CREATE OR REPLACE TABLE ... AS` = SCD tipo 1, pequenas, mudam com a referência
- Fatos (`ft_nfe_item`, `ft_nfe_documento`) e agregado (`ag_nfe_ibscbs_mensal`): **materialized view** com
  `CLUSTER BY (sk_data)`; o refresh decide o que recalcular (substitui o `delete+insert` por mês afetado do datalabs)
- Consumo (`vw_ibscbs_uf_mes`): view simples sobre o agregado + dimensões
- `dm_nfe_participante` (SCD2) adiada para o marco 5 (`AUTO CDC ... STORED AS SCD TYPE 2`); até lá a fato leva
  CNPJ/CPF como chave natural
**Consequência:** a gold não tem código de carga: só definições. A ordem de refresh (fato antes do agregado)
passa a ser responsabilidade da orquestração (marco 6).

## D014 — Pipeline declarativo em schemas paralelos (`nfe_ldp_*`)

**Contexto:** comparar notebooks (imperativo) × Lakeflow Declarative Pipelines (declarativo) com os mesmos arquivos.
**Decisão:**
- Um pipeline `nfe_ldp` publica em `nfe_ldp_bronze`, `nfe_ldp_silver`, `nfe_ldp_gold` (nomes totalmente qualificados)
- Bronze em Python (Auto Loader text, igual ao D010); silver-documento em Python (parse + quarentena por
  expectation DROP + tabela inversa); entidades em SQL com `AUTO CDC ... SCD TYPE 1` (substitui o MERGE do 03b);
  participante com `AUTO CDC ... SCD TYPE 2` (`SEQUENCE BY dh_emissao`); gold em MVs com expectations
- Contrato de parse e UDF de DV reaproveitados da silver dos notebooks (`nfe_doc`, `fn_dv_mod11`)
- O SQL de mapeamento das entidades é **gerado** a partir das views do 03a (mesmo texto, fonte `STREAM(...)`)
- Dimensões estáticas reaproveitadas de `workspace.nfe_gold`
**Consequência:** duas implementações vivas; o mapeamento precisa ficar sincronizado (custo aceito para estudo).
No marco 6 o Job orquestra uma delas, e a outra vira referência.

## D015 — Orquestração: um Job com os dois ramos, condição, reconciliação e log

**Contexto:** marco 6. Duas implementações vivas (D014); a prova cobra tipos de task, dependências,
condições, retries, gatilhos e bundles.
**Decisão:**
- Job `nfe_job` com gatilho *file arrival* no Volume, `max_concurrent_runs = 1`
- Ramo notebooks: `bronze` → condição `arquivos_novos > 0` (task value) → `silver` → `gold`
- Ramo pipeline: task `pipeline` (nfe_ldp) em paralelo, a partir da raiz
- `reconcilia` junta os ramos e **falha de propósito** se divergirem (Job verde = dado consistente)
- `ctrl_lote` com `run_if: ALL_DONE` grava o log por lote e por execução (D008), inclusive de runs com falha
- Notebooks de Job enxutos em `src/06_jobs` (bronze reescrita com o mesmo checkpoint; silver via `%run` do 03b)
- Definição como código em `databricks.yml` + `resources/nfe_job.yml`; host fora do repo (vem da CLI);
  pipeline referenciado por `lookup`, não recriado
**Consequência:** o projeto roda sozinho ao chegar um lote, e cada execução deixa rastro auditável.
Próximo passo natural: o pipeline também como recurso do bundle (exige recriar, porque hoje pertence à UI).

## D016 — CI/CD: bundle com targets dev/prod e GitHub Actions

**Contexto:** marco 6b; seção 5 da prova (Git, bundles, variáveis por ambiente, CLI em CI/CD).
**Decisão:**
- `dev` (padrão): cópia por desenvolvedor, prefixada, gatilho pausado
- `prod`: nome final, gatilho ligado; sem service principal (Free Edition), `run_as` e `permissions`
  explícitos; implantação na pasta padrão do usuário (`/Workspace/Shared` testado: aviso de pasta gravável
  por todos, ou seja, qualquer usuário alteraria o código executado com as permissões do `run_as`)
- GitHub Actions: `validate` em todo push/PR; `deploy -t prod` só por execução manual (decisão humana)
- Host e token em secrets do GitHub; nenhum identificador do workspace no repositório
- Fonte única: o Job criado na UI é apagado depois do primeiro deploy em prod
**Consequência:** toda mudança no Job passa pelo Git; o workspace roda a versão implantada, não a editada à mão.

## D017 — Reconciliação com corte comum entre os ramos

**Contexto:** mesmo com *wait after last change* (120 s), o upload pela CLI (arquivo a arquivo) fez o gatilho
disparar com o lote incompleto. Os dois ramos leram o Volume em momentos diferentes (bronze 5.101 × pipeline
5.161) e a `reconcilia` acusou divergência que não era erro de processamento.
**Decisão:** a `reconcilia` recebe `{{job.start_time.timestamp_ms}}` e compara só o que os dois ramos com certeza
leram: camadas por arquivo com `modificado_em <= corte`; camadas por nota só com chaves cujo último arquivo
(reenvio incluído) chegou antes do corte. O agregado mensal, sem chave, é conferido pela soma de CBS na fato.
**Consequência:** divergência passa a significar processamento diferente do mesmo conjunto de arquivos.
Arquivos atrasados entram na run seguinte sem alarme falso. O gatilho continua sem garantir lote completo;
quem garante que nada se perde é o checkpoint, não o gatilho.

## D018 — Três linhas de planejamento e painel sobre tabelas de controle

**Contexto:** marco 7. O mapa de ponta a ponta ([fluxo.md](fluxo.md)) mostrou a qualidade fora do Job e o
status da run escondendo falhas (`SuccessWithFailures`: a folha `ctrl_lote`, com ALL_DONE, termina bem).
**Decisão:**
- Todo pipeline é planejado em três linhas, etapa por etapa: carga, observabilidade, qualidade
- O painel lê `ctrl_lote` e `ctrl_dq`, nunca o histórico de runs do orquestrador
- Nova task `dq` grava `ctrl_dq` (run × lote × regra) para dar tendência à qualidade
- O status da `ctrl_lote` abre o painel (leitor = operador; sem alerta por e-mail), nunca o status do Job
- Eixo temporal do painel = carga (data/hora), não lote: em produção só existem arquivos chegando
**Consequência:** observabilidade e qualidade sobrevivem à troca ou recriação do Job e ficam consultáveis em SQL.
