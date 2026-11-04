# Planejamento do dashboard AI/BI (marco 7)

## Objetivo

Responder em menos de um minuto, nesta ordem:

1. **A carga está saudável?** (observabilidade)
2. **O dado que entrou é bom?** (qualidade)
3. **O que a reforma está mostrando?** (negócio: IBS, CBS e IS)

A ordem é proposital: número de negócio só tem valor depois de provado que a carga fechou e o dado é confiável.

## Fontes por página

```
 ctrl_lote ──────────────────────────► Página 1 · Saúde da carga
 nfe_quarentena ─┐
 ctrl_dq (novo) ─┴───────────────────► Página 2 · Qualidade
 vw_ibscbs_uf_mes · ag_nfe_ibscbs_mensal · ft_nfe_item ──► Página 3 · Negócio
```

O painel **não** lê o histórico de runs do Job: ele some quando o Job é recriado (visto no marco 6) e o status
da run esconde falhas de tasks intermediárias. As tabelas `ctrl_*` são a fonte (D008, D018).

## Pré-requisitos no Job (7a)

| Mudança | Por quê |
|---|---|
| Task `dq` (`j06_dq`) depois da `gold` | leva alertas, quarentena por motivo e órfãos para dentro do Job |
| Tabela `nfe_bronze.ctrl_dq` | retrato por run: sem ela a página 2 mostra só o estado atual, nunca a tendência |
| `ctrl_lote` recebe `inicio_run` (`{{job.start_time.iso_datetime}}`) | duração da run sem depender do orquestrador |
| `ctrl_lote` passa a depender também de `dq` | continua sendo o log de tudo (ALL_DONE) |

`ctrl_dq` (uma linha por run × lote × regra):

| Coluna | Exemplo |
|---|---|
| `run_id`, `executado_em` | igual à `ctrl_lote`: as duas se juntam por `run_id` |
| `lote` | `lote_007` |
| `camada` | `silver` · `gold` |
| `severidade` | `erro` (quarentena) · `aviso` (alerta fiscal) · `integridade` (órfão) |
| `regra` | `chave_invalida` · `tot_cbs_divergente` · `orfao_sk_tributacao` |
| `qtd` | nº de arquivos, notas ou itens afetados |

## Página 1 · Saúde da carga

| Widget | Tipo | Dataset |
|---|---|---|
| Status da última run | counter (OK / DIVERGENTE / INCOMPLETO...) | `ds_ultima_run` |
| Horas desde o último arquivo novo (frescor) | counter | `ds_frescor` |
| Notas vigentes | counter | `ds_ultima_run` |
| Arquivos no Volume × na bronze, por lote | barras agrupadas | `ds_lotes` |
| Latência upload → carga, por lote | barras | `ds_lotes` |
| Duração por run | linha | `ds_runs` (após 7a) |
| Últimas runs | tabela com status destacado | `ds_runs` |

```sql
-- ds_runs: uma linha por execução (o pior status entre os lotes vence)
SELECT run_id,
       min(executado_em) AS executado_em,
       max(arquivos_novos_execucao) AS arquivos_novos,
       max(diferencas_nb_x_ldp) AS diferencas,
       CASE WHEN count_if(status = 'FALHA_OU_PULADO') > 0 THEN 'FALHA_OU_PULADO'
            WHEN count_if(status = 'DIVERGENTE') > 0      THEN 'DIVERGENTE'
            WHEN count_if(status = 'INCOMPLETO') > 0      THEN 'INCOMPLETO'
            WHEN count_if(status = 'SEM_ARQUIVO_NOVO') > 0 THEN 'SEM_ARQUIVO_NOVO'
            ELSE 'OK' END AS status
FROM workspace.nfe_bronze.ctrl_lote
GROUP BY run_id;

-- ds_ultima_run: estado de cada lote na execução mais recente
SELECT *
FROM workspace.nfe_bronze.ctrl_lote
WHERE run_id = (SELECT max_by(run_id, executado_em) FROM workspace.nfe_bronze.ctrl_lote);

-- ds_lotes: volume e latência por lote (latência = último upload do lote → 1ª run que o deixou OK)
WITH arq AS (
  SELECT lote, count(*) AS arquivos, min(modificado_em) AS primeiro_upload, max(modificado_em) AS ultimo_upload
  FROM workspace.nfe_bronze.nfe_xml_raw GROUP BY lote),
ok AS (
  SELECT lote, min(executado_em) AS primeira_run_ok
  FROM workspace.nfe_bronze.ctrl_lote WHERE status = 'OK' GROUP BY lote)
SELECT a.lote, a.arquivos, a.primeiro_upload, a.ultimo_upload, o.primeira_run_ok,
       timestampdiff(MINUTE, a.ultimo_upload, o.primeira_run_ok) AS latencia_min
FROM arq a LEFT JOIN ok o USING (lote);

-- ds_frescor
SELECT max(carregado_em) AS ultima_carga,
       timestampdiff(HOUR, max(carregado_em), current_timestamp()) AS horas_sem_dado_novo
FROM workspace.nfe_bronze.nfe_xml_raw;
```

> `latencia_min` de lotes antigos sai inflada: a `ctrl_lote` nasceu depois deles. Vale a partir do lote_005.

## Página 2 · Qualidade

| Widget | Tipo | Dataset |
|---|---|---|
| Taxa de erro por lote (quarentena ÷ arquivos) | barras | `ds_taxa_erro` |
| Quarentena por motivo × lote | barras empilhadas | `ds_quarentena` |
| Alertas por regra ao longo das runs | linha | `ctrl_dq` (após 7a) |
| Chaves órfãs na gold | counter (meta: 0, exceto `sk_tributacao`) | `ctrl_dq` |
| Detalhe dos alertas | tabela filtrável por regra | `vw_dq_alertas` |

```sql
-- ds_quarentena
SELECT lote, motivo, count(*) AS arquivos
FROM workspace.nfe_silver.nfe_quarentena
GROUP BY lote, motivo;

-- ds_taxa_erro
SELECT b.lote, b.arquivos, coalesce(q.qtd, 0) AS quarentena,
       round(100 * coalesce(q.qtd, 0) / b.arquivos, 2) AS pct_erro
FROM (SELECT lote, count(*) AS arquivos FROM workspace.nfe_bronze.nfe_xml_raw GROUP BY lote) b
LEFT JOIN (SELECT lote, count(*) AS qtd FROM workspace.nfe_silver.nfe_quarentena GROUP BY lote) q USING (lote);
```

## Página 3 · Negócio

| Widget | Tipo | Dataset |
|---|---|---|
| IBS, CBS e líquido por mês | linha | `vw_ibscbs_uf_mes` |
| Carga efetiva (%) por UF | barras | `vw_ibscbs_uf_mes` |
| Venda × devolução | barras | `ag_nfe_ibscbs_mensal` (`natureza`) |
| Top 10 NCM por base IBS/CBS | barras horizontais | `ft_nfe_item` + `dm_nfe_ncm` |
| Imposto Seletivo por mês | linha | `ag_nfe_ibscbs_mensal` (`v_is`) |

Filtros globais da página: período (`ano_mes`) e UF do emitente.
Medidas não aditivas (`pct_carga_efetiva`, `qtd_notas` distintas) são recalculadas no dataset, nunca somadas
no widget.

## Alerta

SQL Alert agendado (a cada hora), disparando quando > 0:

```sql
SELECT count(*) AS lotes_com_problema
FROM workspace.nfe_bronze.ctrl_lote
WHERE run_id = (SELECT max_by(run_id, executado_em) FROM workspace.nfe_bronze.ctrl_lote)
  AND status NOT IN ('OK', 'SEM_ARQUIVO_NOVO');
```

## Ordem de construção

```
7a  patch: j06_dq + ctrl_dq + inicio_run ──► bundle deploy -t prod ──► 1 lote de teste
7b  UI: datasets + páginas 1 e 2 (aprender o editor AI/BI)
7c  UI: página 3 + filtros + SQL Alert
7d  bundle: dashboard vira recurso (databricks bundle generate dashboard) → versionado com o Job
7e  carga contínua até 50k notas: o painel mostra o comportamento sob volume
```

No 7d, o painel criado na UI é exportado para `resources/` + um `.lvdash.json`. A partir daí, valem as mesmas
regras do Job: mudança passa pelo Git e o `deploy` corrige alterações feitas à mão.
