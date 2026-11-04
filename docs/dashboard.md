# Dashboard AI/BI `nfe_operacao` (marco 7)

## Decisões de desenho

| Pergunta | Resposta | Consequência |
|---|---|---|
| Quem lê? | **o operador do pipeline** | abre pela saúde da carga; tabelas de diagnóstico valem mais que gráfico bonito |
| Alerta por e-mail? | **não** | o status da última carga é o primeiro widget do painel, em destaque |
| Eixo temporal | **data/hora da carga**, não lote | como em produção, onde só existem arquivos chegando; `lote` aparece só como coluna de detalhe |

## Objetivo

Responder em menos de um minuto, nesta ordem:

1. **A última carga está saudável?** (observabilidade)
2. **O que entrou nela tem problema?** (qualidade)
3. **O que a reforma está mostrando?** (negócio: IBS, CBS e IS)

Número de negócio só tem valor depois de provado que a carga fechou e o dado é confiável.

## Fontes por página

```
 ctrl_lote · nfe_xml_raw ────────────► Página 1 · Saúde da carga
 ctrl_dq · nfe_quarentena ───────────► Página 2 · Qualidade
 vw_ibscbs_uf_mes · ag_nfe_ibscbs_mensal · ft_nfe_item ──► Página 3 · Negócio
```

O painel **não** lê o histórico de runs do Job: ele some quando o Job é recriado (marco 6) e o status da run
esconde falhas de tasks intermediárias (`SuccessWithFailures`). As tabelas `ctrl_*` são a fonte (D008, D018).

## Pré-requisitos no Job (7b, entregues)

| Mudança | Para quê |
|---|---|
| Task `dq` (`j06_dq`) depois da `gold` | alertas, quarentena por motivo e órfãos rodam a cada carga |
| Tabela `nfe_bronze.ctrl_dq` | retrato por run: `qtd_carga` (o que entrou nesta carga) e `qtd_total` (estado atual) |
| `ctrl_lote.inicio_run` | duração = `executado_em − inicio_run`, sem depender do orquestrador |
| `ctrl_lote.arquivos_esperados` e `chaves_sem_nota` (7b+) | fronteiras origem → Volume e nfe_doc → nfe_nota vigiadas (D019) |

`ctrl_dq`, uma linha por run × regra (18 regras):

| severidade | camada | regras | unidade |
|---|---|---|---|
| `erro` | silver | 5 motivos de quarentena | arquivo |
| `aviso` | silver | `tot_cbs_divergente`, `vbc_ibscbs_divergente`, `cclasstrib_incoerente`, NCM/CFOP/município fora da referência | nota |
| `aviso` | silver | `reenvio_entre_cargas`: chave que chega de novo em outra entrega (D019) | nota |
| `integridade` | gold | `orfao_sk_data`, `orfao_sk_operacao`, `orfao_sk_tributacao`, `orfao_ncm`, `orfao_cfop`, `orfao_cod_mun_emitente` | item |

`orfao_sk_tributacao` > 0 é esperado: é o alerta `cclasstrib_incoerente` da silver reaparecendo como FK órfã.

## Página 1 · Saúde da carga

```
┌───────────────┬───────────────┬───────────────┬───────────────┐
│ status última │ horas sem     │ duração da    │ notas         │   ← counters
│ carga         │ dado novo     │ última carga  │ vigentes      │
├───────────────┴───────────────┴───────────────┴───────────────┤
│ cargas recentes: início, duração, arquivos novos, diferenças, status   │   ← tabela (diagnóstico)
├────────────────────────────────┬───────────────────────────────┤
│ arquivos por carga (barras)    │ latência upload → bronze      │
│                                │ p50 / p95 por carga (linhas)  │
├────────────────────────────────┴───────────────────────────────┤
│ lotes fora do OK na última carga (tabela; vazia = saudável)     │
└─────────────────────────────────────────────────────────────────┘
```

```sql
-- ds_cargas: uma linha por execução do Job (o pior status entre os lotes vence)
SELECT run_id,
       coalesce(max(inicio_run), min(executado_em))          AS inicio_carga,
       min(executado_em)                                     AS fim_carga,
       timestampdiff(SECOND, max(inicio_run), min(executado_em)) AS duracao_s,
       max(arquivos_novos_execucao)                          AS arquivos_novos,
       max(diferencas_nb_x_ldp)                              AS diferencas,
       sum(notas_vigentes)                                   AS notas_vigentes,
       CASE WHEN count_if(status = 'FALHA_OU_PULADO') > 0  THEN 'FALHA_OU_PULADO'
            WHEN count_if(status = 'DIVERGENTE') > 0       THEN 'DIVERGENTE'
            WHEN count_if(status = 'INCOMPLETO') > 0       THEN 'INCOMPLETO'
            WHEN count_if(status = 'SEM_ARQUIVO_NOVO') > 0 THEN 'SEM_ARQUIVO_NOVO'
            ELSE 'OK' END                                    AS status
FROM workspace.nfe_bronze.ctrl_lote
GROUP BY run_id;

-- ds_lotes_ultima_carga: o que não está OK na execução mais recente
SELECT lote, arquivos_esperados, arquivos_volume, arquivos_bronze, faltando_na_bronze, quarentena, docs_validos,
       chaves_sem_nota, status
FROM workspace.nfe_bronze.ctrl_lote
WHERE run_id = (SELECT max_by(run_id, executado_em) FROM workspace.nfe_bronze.ctrl_lote)
  AND status NOT IN ('OK', 'SEM_ARQUIVO_NOVO');

-- ds_chegada: arquivos e latência por hora de carga (upload = modificado_em, entrada = carregado_em)
SELECT date_trunc('HOUR', carregado_em)                                        AS hora_carga,
       count(*)                                                                AS arquivos,
       percentile_approx(timestampdiff(SECOND, modificado_em, carregado_em), 0.5)  AS latencia_p50_s,
       percentile_approx(timestampdiff(SECOND, modificado_em, carregado_em), 0.95) AS latencia_p95_s
FROM workspace.nfe_bronze.nfe_xml_raw
GROUP BY 1;

-- ds_frescor
SELECT max(carregado_em)                                             AS ultima_entrada,
       round(timestampdiff(MINUTE, max(carregado_em), current_timestamp()) / 60, 1) AS horas_sem_dado_novo
FROM workspace.nfe_bronze.nfe_xml_raw;
```

> Cargas anteriores ao 7b têm `inicio_run` nulo: duração vazia e `inicio_carga` = fim. Os lotes 001–004
> foram carregados à mão, antes do Job: a latência deles mede o tempo até o notebook rodar, não o pipeline.

## Página 2 · Qualidade

```
┌───────────────┬───────────────┬───────────────┐
│ erros na      │ avisos na     │ órfãos na     │   ← counters (qtd_carga da última run com dq)
│ última carga  │ última carga  │ gold (total)  │
├───────────────┴───────────────┴───────────────┤
│ problemas por carga, empilhado por severidade (barras)          │
├────────────────────────────────┬───────────────────────────────┤
│ taxa de erro por carga (%)     │ regras: carga × total (tabela)│
├────────────────────────────────┴───────────────────────────────┤
│ detalhe dos alertas (vw_dq_alertas), filtro por regra           │
└─────────────────────────────────────────────────────────────────┘
```

```sql
-- ds_dq: base de todos os widgets de tendência
SELECT d.run_id, d.inicio_run, d.executado_em, d.severidade, d.camada, d.regra, d.qtd_carga, d.qtd_total
FROM workspace.nfe_bronze.ctrl_dq d;

-- ds_dq_ultima: estado na execução mais recente da task dq
SELECT severidade, regra, qtd_carga, qtd_total
FROM workspace.nfe_bronze.ctrl_dq
WHERE run_id = (SELECT max_by(run_id, executado_em) FROM workspace.nfe_bronze.ctrl_dq);

-- ds_taxa_erro: quarentena ÷ arquivos, por hora de carga
SELECT b.hora_carga, b.arquivos, coalesce(q.qtd, 0) AS quarentena,
       round(100 * coalesce(q.qtd, 0) / b.arquivos, 2) AS pct_erro
FROM (SELECT date_trunc('HOUR', carregado_em) AS hora_carga, count(*) AS arquivos
      FROM workspace.nfe_bronze.nfe_xml_raw GROUP BY 1) b
LEFT JOIN (SELECT date_trunc('HOUR', detectado_em) AS hora_carga, count(*) AS qtd
           FROM workspace.nfe_silver.nfe_quarentena GROUP BY 1) q USING (hora_carga);
```

> `ds_taxa_erro` aproxima a carga pela hora: bronze (`carregado_em`) e silver (`detectado_em`) da mesma run caem
> na mesma hora salvo run que cruza a virada. A `ctrl_dq` é a medida exata, a partir do 7b.

## Página 3 · Negócio

| Widget | Tipo | Dataset |
|---|---|---|
| IBS, CBS e líquido por mês de emissão | linha | `vw_ibscbs_uf_mes` |
| Carga efetiva (%) por UF | barras | `vw_ibscbs_uf_mes` |
| Venda × devolução | barras | `ag_nfe_ibscbs_mensal` (`natureza`) |
| Top 10 NCM por base IBS/CBS | barras horizontais | `ft_nfe_item` + `dm_nfe_ncm` |
| Imposto Seletivo por mês | linha | `ag_nfe_ibscbs_mensal` (`v_is`) |

Aqui o eixo é o **mês de emissão** da nota (fato de negócio), não a carga. Filtros: período e UF do emitente.
Medidas não aditivas (`pct_carga_efetiva`, notas distintas) são recalculadas no dataset, nunca somadas no widget.

## Ordem de construção

```
7b  ✔ patch: j06_dq + ctrl_dq + inicio_run ──► bundle deploy -t prod ──► 1 lote de teste
7c  UI: datasets + página 1 (aprender o editor AI/BI)
7d  UI: páginas 2 e 3, filtros, publicação
7e  bundle: dashboard vira recurso (databricks bundle generate dashboard) → versionado com o Job
7f  carga contínua até 50k notas: o painel mostra o comportamento sob volume
```

No 7e, o painel criado na UI vira `resources/*.yml` + um `.lvdash.json`. A partir daí valem as regras do Job:
mudança passa pelo Git e o `deploy` corrige alterações feitas à mão.
