import logging

import pandas as pd

from utils.date_utils import TIMEZONE_SP

logger = logging.getLogger(__name__)

STATUS_TRADUZIDO = {
    "Lost": "Encerrado",
    "Ongoing": "Ativo",
    "Won": "Fechado",
}


# A API devolve "" (ou só espaços) para campo em branco — no banco isso deve virar NULL
def vazio_para_none(valor):
    if isinstance(valor, str) and not valor.strip():
        return None
    return valor


# Recebe a lista crua de deals + dicts de lookup, e retorna um DataFrame
# achatado com todas as colunas mapeadas para português
def transform_deals(deals: list[dict], lookups: dict) -> pd.DataFrame:
    lookup_vendedores = lookups.get("users", {})
    lookup_origens = lookups.get("sources", {})
    lookup_organizacoes = lookups.get("organizations", {})
    lookup_motivos_perda = lookups.get("lost_reasons", {})
    lookup_contatos = lookups.get("contacts", {})
    lookup_produtos = lookups.get("products", {})
    produtos_por_deal = lookups.get("products_by_deal", {})

    rows = []
    for deal in deals:
        owner_id = deal.get("owner_id") or ""
        source_id = deal.get("source_id") or ""
        org_id = deal.get("organization_id") or ""
        lost_reason_id = deal.get("lost_reason_id") or ""
        contact_ids = deal.get("contact_ids") or []

        # Fallback: se não tem organização, usa o primeiro contato como cliente
        if org_id:
            cliente_id = org_id
            cliente_descricao = lookup_organizacoes.get(org_id, "")
        elif contact_ids:
            cliente_id = contact_ids[0]
            cliente_descricao = lookup_contatos.get(contact_ids[0], "")
        else:
            cliente_id = ""
            cliente_descricao = ""

        custom_fields = deal.get("custom_fields") or {}

        # Produtos vinculados ao deal
        deal_id = deal.get("id")
        product_ids = produtos_por_deal.get(deal_id, [])
        if product_ids:
            produto_id = ", ".join(product_ids)
            produto_descricao = ", ".join(
                lookup_produtos.get(pid, "") for pid in product_ids
            )
        else:
            produto_id = None
            produto_descricao = None

        rows.append({
            "id_negociacao": deal.get("id"),
            "nome_negociacao": deal.get("name"),
            "data_criacao": deal.get("created_at"),
            "data_atualizacao": deal.get("updated_at"),
            "data_fechamento": deal.get("closed_at"),
            "id_cliente": cliente_id,
            "descricao_cliente": cliente_descricao,
            "id_origem": source_id,
            "descricao_origem": lookup_origens.get(source_id, ""),
            "id_vendedor": owner_id,
            "descricao_vendedor": lookup_vendedores.get(owner_id, ""),
            "status_negociacao": STATUS_TRADUZIDO.get(deal.get("status"), deal.get("status")),
            "id_motivo_perda": lost_reason_id,
            "descricao_motivo_perda": lookup_motivos_perda.get(lost_reason_id, ""),
            "descricao_municipio": custom_fields.get("cidade-teste"),
            "descricao_estado": custom_fields.get("estado"),
            "descricao_filial": custom_fields.get("filial"),
            "descricao_unidade_negocio": custom_fields.get("setor"),
            "id_produto": produto_id,
            "descricao_produto": produto_descricao,
            "valor_total": deal.get("total_price"),
            "descricao_prazo": custom_fields.get("prazo-de-locacao"),
        })

    df = pd.DataFrame(rows)

    # Normaliza todo valor vazio (""/espaços) para None -> NULL no banco
    df = df.map(vazio_para_none)

    # Converte as datas de string ISO 8601 para o horário local de São Paulo, sem tz.
    # A API devolve com offset (-03:00) e o to_sql do pandas grava qualquer coluna
    # tz-aware convertida para UTC — o DW ficaria 3h adiantado. utc=True primeiro
    # normaliza tudo (inclusive offsets diferentes por horário de verão antigo),
    # depois convertemos para SP e removemos o tz.
    for col in ["data_criacao", "data_atualizacao", "data_fechamento"]:
        if col in df.columns:
            serie = pd.to_datetime(df[col], errors="coerce", utc=True)
            df[col] = serie.dt.tz_convert(TIMEZONE_SP).dt.tz_localize(None)

    logger.info("Transform concluído: %d linhas, %d colunas", len(df), len(df.columns))
    return df
