# databricks-nfe-lakehouse

Pipeline **ELT de NF-e (modelo 55) com os grupos da reforma tributária (IBS/CBS/IS)** no Databricks:
do XML bruto num Volume do Unity Catalog até um modelo dimensional para BI. A carga roda sozinha quando um lote
chega, deixa log auditável de cada execução e separa erro de dado de erro de processo. O Job inteiro é código,
implantado por Asset Bundle e validado em CI. Dados **100% sintéticos** (gerador validado contra o XSD oficial
`leiauteNFe_v4.00`, com ~2% de anomalias injetadas e gabarito).

Projeto de portfólio e preparação para a **Databricks Certified Data Engineer Associate**.
Porte para o Databricks do pipeline NF-e do [datalabs](https://github.com/engrodrigoa/datalabs)
(Postgres + dbt + Airflow).

## Como o projeto foi pensado: três linhas

Cada etapa do medallion foi planejada respondendo três perguntas ao mesmo tempo:

| Linha | Pergunta | Registro |
|---|---|---|
| **Carga** | o que entra, o que sai, onde grava? | bronze → silver → gold |
| **Observabilidade** | a carga rodou, chegou ao fim e não perdeu nada? | `ctrl_lote` (1 linha por lote × execução) |
| **Qualidade** | o conteúdo é válido e coerente? qual a verdade de referência? | `ctrl_dq` (1 linha por regra × execução) · quarentena · gabarito |

Mapa completo, com cada controle posicionado na etapa que vigia: **[docs/fluxo.md](docs/fluxo.md)**.

## Arquitetura

```
┌──────────────────────┐ lotes (CLI fs cp)  ┌──────────────────────────────────────┐
│ gerador/gen_nfe.py   │ ─────────────────► │ Volume  workspace.nfe_bronze.landing │
│ (sistema de origem)  │                    │   xml/lote_NNN/*.xml  manifesto/ ref/│
└──────────────────────┘                    └──────────────────┬───────────────────┘
                                                               │ file arrival → nfe_job
                                                               ▼
                         BRONZE  nfe_xml_raw   Auto Loader (text, wholeText): XML bruto + _metadata, 1 linha/arquivo
                                                               │ from_xml com schema-contrato · regras de entrada
                                                               ▼
                         SILVER  nfe_doc ──► nfe_nota · nfe_item · nfe_pagamento · nfe_duplicata  (MERGE, dedup de reenvio)
                                 └──► nfe_quarentena (erro: XML corrompido, chave/DV, protocolo, cStat, CNPJ)
                                                               │
                                                               ▼
                         GOLD    dm_* (TABLE) · ft_nfe_item · ft_nfe_documento · ag_nfe_ibscbs_mensal (MATERIALIZED VIEW)
                                                               ▼
                                 vw_ibscbs_uf_mes ──► AI/BI Dashboard (marco 7)

 Em paralelo: o mesmo fluxo como Lakeflow Declarative Pipeline (expectations, AUTO CDC SCD1/SCD2),
 reconciliado camada a camada com os notebooks a cada execução.
```

## Orquestração como código

O Job `nfe_job` não foi montado à mão em produção. Ele foi prototipado na UI (marco 6a) e depois descrito em
YAML como **Databricks Asset Bundle** (marco 6b). Um comando recria tudo: tasks, dependências, condição,
gatilho, retries, parâmetros e permissões.

```
               ┌► bronze ─► arquivos_novos > 0 ? ──true──► silver ─► gold ─┬─► dq ──────────┐
 file arrival ─┤                                                            ├─► reconcilia ──┴─► ctrl_lote
               └► pipeline nfe_ldp ─────────────────────────────────────────┘                   (ALL_DONE)
```

```powershell
databricks bundle validate            # YAML, caminhos e variáveis
databricks bundle deploy              # dev: "[dev <usuário>] nfe_job", gatilho pausado
databricks bundle deploy -t prod      # prod: "nfe_job", gatilho ligado, run_as explícito
```

| | Efeito |
|---|---|
| **Targets dev/prod** | a mesma definição vira uma cópia isolada por desenvolvedor ou o Job oficial |
| **Variáveis** | o que muda entre ambientes (estado do gatilho, id do pipeline por `lookup`) fica fora do código |
| **CI no GitHub Actions** | `validate` em todo push/PR; `deploy -t prod` só por execução manual |
| **Correção de drift** | o Job de prod foi apagado pela UI e o `deploy` seguinte o recriou idêntico |
| **Estado no workspace** | deploy pelo PC e pelo CI encontram o mesmo estado (`0 created, 2 unchanged`) |

Começar um projeto novo com o Job já estruturado em YAML é o que dá velocidade: o planejamento das três linhas
vira tasks, e as tasks viram código revisável em PR. Detalhes: [docs/job_nfe.md](docs/job_nfe.md).

## Observabilidade e qualidade

| Controle | Linha | Onde roda |
|---|---|---|
| Completude origem (`controle_lote.json`) × Volume × bronze; balanço bronze = quarentena + válidos; `nfe_doc` → `nfe_nota` | observabilidade | `ctrl_lote`, toda execução |
| Reconciliação notebooks × pipeline com corte comum, falha de propósito | observabilidade | `j04_reconcilia`, toda execução |
| Quarentena por motivo + constraints Delta | qualidade (erro, bloqueia) | silver, toda execução |
| Alertas fiscais: totais × soma dos itens, CST × cClassTrib, códigos fora da referência | qualidade (aviso) | `j06_dq` → `ctrl_dq`, toda execução |
| Reenvio entre cargas (mesma chave em outra entrega) | qualidade (aviso) | `j06_dq` → `ctrl_dq`, toda execução |
| Gabarito: anomalias injetadas pelo gerador × detectadas (31/31) | teste dos testes | `03c` |
| Chaves órfãs na gold (o Delta não força FK) | integridade | `j06_dq` → `ctrl_dq`, toda execução |

Lição registrada: **Job verde não é carga saudável**. O status da run vem das tasks folha; com a `ctrl_lote`
(ALL_DONE) como folha, uma reconciliação falha aparece como *Succeeded*. Por isso painel e alerta leem a
`ctrl_lote`, não o orquestrador (D018).

## Datalabs → Databricks

| datalabs | aqui |
|---|---|
| MinIO / pasta inbox | Unity Catalog **Volume** |
| `ctrl.fn_nfe_ingerir_lote` (SQL) | **Auto Loader** |
| dbt incremental `merge` | Delta `MERGE` / **Lakeflow Declarative Pipelines** |
| testes dbt + `store_failures` | quarentena + **expectations** + alertas |
| Airflow `dag_nfe` | **Lakeflow Jobs** |
| GitLab CI | **Asset Bundles** + GitHub Actions |
| Grafana | **AI/BI Dashboard** |

## Estrutura

```
gerador/            gen_nfe.py: gera os XMLs localmente (só biblioteca padrão do Python)
referencias/        tabelas de referência (CFOP, NCM, cClassTrib, município, UF)
docs/               fluxo (três linhas) · contrato de dados · decisões · Job · dashboard · guia de estudos
src/00_setup/       schemas e volumes no Unity Catalog
src/01_exploracao/  leitura exploratória do XML
src/02_bronze/      três formas de ingestão + comparação
src/03_silver/      setup (refs, UDF, views) · carga PySpark (stream + MERGE) · qualidade
src/04_gold/        dimensões · fatos e agregado (MV) · validação
src/05_pipeline/    transformations/ do pipeline nfe_ldp · notebook de validação
src/06_jobs/        notebooks enxutos das tasks do Job
resources/          Job em YAML (Asset Bundle); databricks.yml na raiz
.github/workflows/  CI do bundle
data/               massa local gerada (fora do Git)
```

## Gerar e enviar massa (Windows, PowerShell)

```powershell
cd D:\lambda4\nfe
python gerador\gen_nfe.py --inbox data\inbox\lote_NNN --n 300 --sujo --manifesto data\manifesto_lote_NNN.csv
databricks fs cp -r data\inbox\lote_NNN dbfs:/Volumes/workspace/nfe_bronze/landing/xml/lote_NNN
databricks fs cp data\manifesto_lote_NNN.csv dbfs:/Volumes/workspace/nfe_bronze/landing/manifesto/manifesto_lote_NNN.csv
```

O upload dispara o `nfe_job` (*file arrival*). `--sujo` injeta ≈2% de inconsistência fiscal, 1% de reenvio e
0,5% de XML truncado; o manifesto registra cada anomalia (gabarito). Cada lote é uma entrega única: o gerador
recusa pasta ou manifesto já usados e grava `controle_lote.json` com a quantidade de arquivos, que a `ctrl_lote` confere
com o que chegou ao Volume.

## Roadmap

| Marco | Entrega | Status |
|---|---|---|
| 1 | Unity Catalog (schemas/volumes), massa inicial, exploração do XML, contrato de dados | ✔ |
| 2 | Bronze: `COPY INTO` × Auto Loader (xml) × Auto Loader (text) → oficial: Auto Loader text (D010) | ✔ |
| 3 | Silver: nfe_doc → nota, item, pagamento, duplicata (MERGE); quarentena; alertas; gabarito 31/31 | ✔ |
| 4 | Gold: dimensões (TABLE), fatos e agregado (MATERIALIZED VIEW), consumo (VIEW) | ✔ |
| 5 | Lakeflow Declarative Pipeline em schemas paralelos: expectations, AUTO CDC SCD1/SCD2, MVs (= notebooks, diferença 0) | ✔ |
| 6 | Lakeflow Job (file arrival, notebooks ∥ pipeline, condição, reconciliação, ctrl_lote) como Asset Bundle dev/prod + CI no GitHub Actions | ✔ |
| 7 | Mapa das três linhas ✔ · reconciliação com corte comum ✔ · task de qualidade `ctrl_dq` ✔ · fronteiras vigiadas (arquivo de controle, `nfe_doc → nfe_nota`, reenvio entre cargas) ✔ · painel "Saúde da Carga" ✔ · páginas Qualidade e Negócio · painel como recurso do bundle · carga até 50k notas | em andamento |

Detalhes: [fluxo](docs/fluxo.md) · [Job](docs/job_nfe.md) · [dashboard](docs/dashboard.md) ·
[contrato de dados](docs/contrato_dados.md) · [decisões](docs/decisoes.md) · [guia de estudos](docs/guia_estudos.md)
