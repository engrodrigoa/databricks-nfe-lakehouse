# Contrato de dados — NF-e modelo 55 (layout 4.00 + IBS/CBS/IS)

Fonte da verdade do mapeamento **XML → bronze → silver**. Caminhos no formato do leitor XML do Spark
(`rowTag = nfeProc`), abreviando `NFe.infNFe` como **`n`** e `protNFe.infProt` como **`p`**.
Atributos XML viram colunas com prefixo `_` (ex.: `n._Id`, `det._nItem`).

## Bronze — `nfe_bronze.nfe_xml`

1 linha por **arquivo** recebido (reenvios incluídos). Sem tipagem: tudo string, como chegou.

| Coluna | Origem | Observação |
|---|---|---|
| `NFe`, `protNFe`, `_versao` | XML (`rowTag nfeProc`) | struct aninhado, folhas como string |
| `_corrupt_record` | leitor XML | preenchido quando o arquivo está truncado/malformado |
| `arquivo`, `caminho`, `tamanho_bytes`, `modificado_em` | `_metadata` | rastreabilidade |
| `lote` | pasta `lote_NNN` | unidade de carga |
| `carregado_em` | `current_timestamp()` | |

## Silver

Chave natural: `chave_acesso = substring(n._Id, 4)` (44 dígitos). Reenvio = mesma chave em outro
arquivo → **vale o último recebido** (`modificado_em`, depois `arquivo`).

### `nfe_nota` — 1 linha por `chave_acesso`

| Grupo | Colunas (silver) | Origem (`n.`) | Tipo |
|---|---|---|---|
| Identificação | `cod_uf, modelo, serie, nnf` | `ide.cUF, ide.mod, ide.serie, ide.nNF` | string, string, int, bigint |
| | `dh_emissao`, `dt_emissao` | `ide.dhEmi` | timestamp, date (America/Sao_Paulo) |
| | `nat_op, tp_nf, fin_nfe, id_dest, ind_final, ind_pres, tp_amb` | `ide.*` | string |
| | `chave_referenciada` | `ide.NFref.refNFe` | string (devoluções) |
| Emitente | `emit_cnpj, emit_nome, emit_ie, emit_crt` | `emit.CNPJ, xNome, IE, CRT` | string (CNPJ pode ser alfanumérico) |
| | `emit_uf, emit_cod_municipio, emit_municipio, emit_logradouro, emit_bairro, emit_cep` | `emit.enderEmit.*` | string |
| Destinatário | `dest_tipo_doc, dest_doc` | `dest.CNPJ` / `dest.CPF` | string (`CNPJ`/`CPF`) |
| | `dest_nome, dest_ie, dest_ind_ie` + endereço | `dest.*`, `dest.enderDest.*` | string |
| Totais ICMS | `v_prod, v_bc_icms, v_icms, v_icms_deson, v_ipi, v_pis, v_cofins, v_nf` | `total.ICMSTot.*` | decimal(15,2) |
| Totais IBS/CBS | `v_bc_ibscbs` | `total.IBSCBSTot.vBCIBSCBS` | decimal(15,2) |
| | `v_ibs_uf, v_dif_ibs_uf, v_devtrib_ibs_uf` | `total.IBSCBSTot.gIBS.gIBSUF.*` | decimal(15,2) |
| | `v_ibs_mun, v_dif_ibs_mun, v_devtrib_ibs_mun` | `total.IBSCBSTot.gIBS.gIBSMun.*` | decimal(15,2) |
| | `v_ibs, v_credpres_ibs` | `total.IBSCBSTot.gIBS.vIBS, vCredPres` | decimal(15,2) |
| | `v_cbs, v_dif_cbs, v_devtrib_cbs, v_credpres_cbs` | `total.IBSCBSTot.gCBS.*` | decimal(15,2) |
| IS / total | `v_is`, `v_nf_tot` | `total.ISTot.vIS`, `total.vNFTot` | decimal(15,2), opcionais |
| Transporte | `mod_frete, transp_cnpj, transp_nome, transp_uf, qtd_volumes, peso_bruto` | `transp.*` | string / int / decimal(15,3) |
| Protocolo | `n_protocolo, dh_autorizacao, c_stat` | `p.nProt, p.dhRecbto, p.cStat` | string, timestamp, string |
| Derivadas | `qtd_itens` | `size(n.det)` | int |
| Controle | `lote, arquivo, carregado_em` | bronze | |

### `nfe_item` — 1 linha por `chave_acesso + n_item`

| Grupo | Colunas | Origem (`det.`) | Tipo |
|---|---|---|---|
| Produto | `n_item` | `_nItem` | int |
| | `cprod, xprod, ncm, cfop, c_benef, ucom` | `prod.*` | string (NCM/CFOP **nunca** numérico) |
| | `qcom, vuncom, vprod` | `prod.*` | decimal(15,4), decimal(21,10), decimal(15,2) |
| ICMS | `cst_icms, p_red_bc_icms, vbc_icms, p_icms, v_icms, v_icms_deson` | `imposto.ICMS.{ICMS00\|ICMS20\|ICMS40}.*` | `coalesce` entre os grupos |
| IPI/PIS/COFINS | `cst_ipi, v_ipi, cst_pis, v_pis, cst_cofins, v_cofins` | `imposto.IPI.{IPITrib\|IPINT}`, `PIS.{PISAliq\|PISNT\|PISOutr}`, idem COFINS | `coalesce` |
| IBS/CBS | `cst_ibscbs, cclasstrib` | `imposto.IBSCBS.CST, cClassTrib` | string (`000`, `000001`) |
| | `vbc_ibscbs, v_ibs` | `imposto.IBSCBS.gIBSCBS.vBC, vIBS` | decimal(15,2) |
| | `p_ibs_uf, p_red_aliq, p_aliqefet_ibs_uf, p_dif, v_dif_ibs_uf, v_devtrib_ibs_uf, v_ibs_uf` | `...gIBSCBS.gIBSUF.*` (`gRed`, `gDif`, `gDevTrib`) | decimal(7,4) / (15,2) |
| | `p_ibs_mun, v_dif_ibs_mun, v_devtrib_ibs_mun, v_ibs_mun` | `...gIBSCBS.gIBSMun.*` | idem |
| | `p_cbs, p_aliqefet_cbs, v_dif_cbs, v_devtrib_cbs, v_cbs` | `...gIBSCBS.gCBS.*` | idem |
| IS | `cst_is, cclasstrib_is, v_bc_is, p_is, v_is` | `imposto.IS.*` | opcional (`--com-is`) |
| Total | `v_item` | `vItem` | decimal(15,2) |

### `nfe_pagamento` — 1 linha por `chave_acesso + seq`

`ind_pag, t_pag, v_pag` ← `n.pag.detPag` (`t_pag = 90` em devolução, "sem pagamento").

### `nfe_duplicata` — 1 linha por `chave_acesso + n_dup`

`n_fat, v_orig, v_liq` ← `n.cobr.fat`; `n_dup, d_venc, v_dup` ← `n.cobr.dup` (só saídas, ~70%).

## Referências (`referencias/*.csv` → `nfe_silver.ref_*`)

`ref_cfop`, `ref_ncm`, `ref_classtrib` (CST × cClassTrib), `ref_municipio`, `ref_uf`.
Ler sempre com schema explícito em string (códigos com zero à esquerda).

## Regras de qualidade (herdadas do datalabs)

| Regra | Severidade | Ação |
|---|---|---|
| XML truncado/malformado (`_corrupt_record`) | erro | quarentena, fora da silver |
| `chave_acesso` com 44 posições e DV módulo 11 válido | erro | quarentena |
| `p.cStat = 100` (autorizada) | erro | quarentena |
| Reenvio da mesma chave | info | dedup, vale o mais recente |
| Total CBS ≠ Σ `v_cbs` dos itens | aviso | mantém + registra |
| `v_bc_ibscbs` total ≠ Σ itens | aviso | mantém + registra |
| `cclasstrib` incompatível com `cst_ibscbs` (`ref_classtrib`) | aviso | mantém + registra |
| NCM / CFOP / município fora da referência | aviso | mantém + registra |
