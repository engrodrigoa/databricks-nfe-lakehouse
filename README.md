# databricks-nfe-lakehouse

Pipeline **ELT de NF-e (modelo 55) com os grupos da reforma tributária (IBS/CBS/IS)** no Databricks:
do XML bruto num Volume do Unity Catalog até um modelo dimensional para BI, com qualidade de dados e
CI/CD. Dados **100% sintéticos** (gerador validado contra o XSD oficial `leiauteNFe_v4.00`).

Projeto de portfólio e preparação para a **Databricks Certified Data Engineer Associate**.
Porte para o Databricks do pipeline NF-e do [datalabs](https://github.com/engrodrigoa/datalabs)
(Postgres + dbt + Airflow).

## Arquitetura

```
┌──────────────────────┐  lotes (UI / CLI)   ┌──────────────────────────────────────┐
│ gerador/gen_nfe.py   │ ──────────────────► │ Volume  workspace.nfe_bronze.landing │
│ (sistema de origem,  │                     │   xml/lote_NNN/*.xml   ref/*.csv     │
│  roda local)         │                     └──────────────────┬───────────────────┘
└──────────────────────┘                                        │ Auto Loader (cloudFiles, xml)
                                                                ▼
                                    BRONZE   nfe_xml  (struct como string + _metadata + _corrupt_record)
                                                                │ cast tipado + dedup de reenvio
                                                                ▼
                                    SILVER   nfe_nota · nfe_item · nfe_pagamento · nfe_duplicata
                                                                │ + referências (CFOP, NCM, cClassTrib...)
                                                                ▼
                                    GOLD     dm_* · ft_nfe_item · ft_nfe_documento · ag_nfe_ibscbs_mensal
                                                                ▼
                                             AI/BI Dashboard
   Orquestração: Lakeflow Jobs │ Qualidade: expectations + quarentena │ CI/CD: Git folder + Asset Bundles
```

## Datalabs → Databricks

| datalabs | aqui |
|---|---|
| MinIO / pasta inbox | Unity Catalog **Volume** |
| `ctrl.fn_nfe_ingerir_lote` (SQL) | **Auto Loader** |
| dbt incremental `merge` | Delta `MERGE` / **Lakeflow Declarative Pipelines** |
| testes dbt + `store_failures` | **expectations** + tabela de quarentena |
| Airflow `dag_nfe` | **Lakeflow Jobs** |
| Grafana | **AI/BI Dashboard** |
| GitLab CI | **Git folder** + **Databricks Asset Bundles** |

## Estrutura

```
gerador/          gen_nfe.py: gera os XMLs localmente (só biblioteca padrão do Python)
referencias/      tabelas de referência (CFOP, NCM, cClassTrib, município, UF)
docs/             contrato de dados e decisões de arquitetura
src/00_setup/     schemas e volumes no Unity Catalog
src/01_exploracao/ leitura exploratória do XML
data/             massa local gerada (fora do Git)
```

## Gerar massa de dados (Windows, PowerShell)

```powershell
cd D:\lambda4\nfe
python gerador\gen_nfe.py --inbox data\inbox\lote_001 --n 50 --data-ini 2026-01-01 --data-fim 2026-01-31 --seed 1
# com anomalias (≈2% inconsistência fiscal, 1% reenvio, 0,5% truncado) e gabarito:
python gerador\gen_nfe.py --inbox data\inbox\lote_002 --n 500 --sujo --manifesto data\manifesto_lote_002.csv
```

## Roadmap

| Marco | Entrega | Status |
|---|---|---|
| 1 | Unity Catalog (schemas/volumes), massa inicial, exploração do XML, contrato de dados | em andamento |
| 2 | Bronze: `COPY INTO` / `read_files` × Auto Loader | |
| 3 | Silver: nota, item, pagamento, duplicata; dedup de reenvio; qualidade | |
| 4 | Gold: modelo dimensional + agregado IBS/CBS mensal | |
| 5 | Refatoração em Lakeflow Declarative Pipeline + expectations | |
| 6 | Lakeflow Jobs, Asset Bundles, grants e lineage no Unity Catalog | |
| 7 | Dashboard AI/BI e carga contínua até 50k notas | |

Detalhes: [contrato de dados](docs/contrato_dados.md) · [decisões](docs/decisoes.md)
