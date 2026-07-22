import logging
from datetime import datetime

import requests

from drivers.api_client import RDStationClient
from utils.date_utils import build_updated_at_filter

logger = logging.getLogger(__name__)


# Extrai todos os deals da API V2.
# Usa a varredura convergente: uma passada só perde ~5-15% dos registros.
def extract_deals(client: RDStationClient) -> list[dict]:
    deals = client.get_all_deals_convergente()
    logger.info("%d deals extraídos (varredura convergente)", len(deals))
    return deals


# Extrai os ids de todos os deals — usado para levantar CANDIDATOS a exclusão.
# Mesmo com a varredura convergente, um id ausente aqui ainda precisa ser
# confirmado um a um: convergência é heurística, 404 é prova.
def extract_ids_deals(client: RDStationClient) -> list[str]:
    deals = client.get_all_deals_convergente()
    ids = [deal["id"] for deal in deals if deal.get("id")]
    logger.info("%d ids conhecidos na API", len(ids))
    return ids


# Confirma quais candidatos foram realmente excluídos, buscando um a um pelo ID.
# Só 404 conta como exclusão — qualquer outro erro é tratado como "não sei", e o
# registro fica sem marcar (conservador: nunca marca uma negociação viva).
def confirmar_deals_excluidos(client: RDStationClient, ids_candidatos: list[str]) -> list[str]:
    ids_excluidos = []
    for contador, deal_id in enumerate(ids_candidatos, 1):
        try:
            client.get_deal(deal_id)
        except requests.HTTPError as erro:
            if erro.response is not None and erro.response.status_code == 404:
                ids_excluidos.append(deal_id)
            else:
                logger.warning("Erro ao confirmar deal %s: %s", deal_id, erro)
        except Exception as erro:
            logger.warning("Erro ao confirmar deal %s: %s", deal_id, erro)
        if contador % 50 == 0:
            logger.info("Candidatos confirmados: %d/%d", contador, len(ids_candidatos))
    logger.info(
        "%d candidatos verificados -> %d confirmados como excluídos na origem",
        len(ids_candidatos),
        len(ids_excluidos),
    )
    return ids_excluidos


# Extrai somente os deals atualizados desde o cutoff (incremental)
def extract_deals_atualizados(client: RDStationClient, cutoff: datetime) -> list[dict]:
    filtro = build_updated_at_filter(cutoff)
    deals = client.get_all_deals_convergente(filter_str=filtro)
    logger.info("%d deals atualizados desde %s", len(deals), cutoff)
    return deals


# Monta lookup {id: name} dos vendedores (users)
def extract_lookup_vendedores(client: RDStationClient) -> dict[str, str]:
    lista_vendedores = client.get_all_users()
    lookup_vendedores = {vendedor["id"]: vendedor["name"] for vendedor in lista_vendedores}
    logger.info("Lookup de vendedores: %d registros", len(lookup_vendedores))
    return lookup_vendedores


# Monta lookup {id: name} das origens (sources)
def extract_lookup_origens(client: RDStationClient) -> dict[str, str]:
    lista_origens = client.get_all_sources()
    lookup_origens = {origem["id"]: origem["name"] for origem in lista_origens}
    logger.info("Lookup de origens: %d registros", len(lookup_origens))
    return lookup_origens


# Monta lookup {id: name} dos motivos de perda
# Busca um por um pelo ID, pois a V2 não tem listagem de lost_reasons
def extract_lookup_motivos_perda(client: RDStationClient, ids_motivos_perda: set[str]) -> dict[str, str]:
    lookup_motivos_perda = {}
    for contador, lost_reason_id in enumerate(ids_motivos_perda, 1):
        try:
            dados_motivo = client.get_lost_reason(lost_reason_id)
            lookup_motivos_perda[lost_reason_id] = dados_motivo.get("name", "")
        except Exception as erro:
            logger.warning("Erro ao buscar motivo de perda %s: %s", lost_reason_id, erro)
            lookup_motivos_perda[lost_reason_id] = ""
    logger.info("Lookup de motivos de perda: %d registros", len(lookup_motivos_perda))
    return lookup_motivos_perda


# Monta lookup {id: name} das organizações (clientes)
# Busca uma por uma, pois a API não lista todas de forma paginada
def extract_lookup_organizacoes(client: RDStationClient, ids_organizacoes: set[str]) -> dict[str, str]:
    lookup_organizacoes = {}
    for contador, organization_id in enumerate(ids_organizacoes, 1):
        try:
            dados_organizacao = client.get_organization(organization_id)
            lookup_organizacoes[organization_id] = dados_organizacao.get("name", "")
        except Exception as erro:
            logger.warning("Erro ao buscar organização %s: %s", organization_id, erro)
            lookup_organizacoes[organization_id] = ""
        if contador % 50 == 0:
            logger.info("Organizações processadas: %d/%d", contador, len(ids_organizacoes))
    logger.info("Lookup de organizações: %d registros", len(lookup_organizacoes))
    return lookup_organizacoes


# Monta lookup {id: name} dos contatos
# Usado como fallback quando o deal não tem organization_id
def extract_lookup_contatos(client: RDStationClient, ids_contatos: set[str]) -> dict[str, str]:
    lookup_contatos = {}
    for contador, contact_id in enumerate(ids_contatos, 1):
        try:
            dados_contato = client.get_contact(contact_id)
            lookup_contatos[contact_id] = dados_contato.get("name", "")
        except Exception as erro:
            logger.warning("Erro ao buscar contato %s: %s", contact_id, erro)
            lookup_contatos[contact_id] = ""
        if contador % 50 == 0:
            logger.info("Contatos processados: %d/%d", contador, len(ids_contatos))
    logger.info("Lookup de contatos: %d registros", len(lookup_contatos))
    return lookup_contatos


# Para cada deal, busca os produtos vinculados e retorna {deal_id: [product_id, ...]}
def extract_produtos_por_deal(client: RDStationClient, deal_ids: list[str]) -> dict[str, list[str]]:
    produtos_por_deal = {}
    for contador, deal_id in enumerate(deal_ids, 1):
        try:
            deal_products = client.get_deal_products(deal_id)
            product_ids = [dp.get("product_id") for dp in deal_products if dp.get("product_id")]
            produtos_por_deal[deal_id] = product_ids
        except Exception as erro:
            logger.warning("Erro ao buscar produtos do deal %s: %s", deal_id, erro)
            produtos_por_deal[deal_id] = []
        if contador % 50 == 0:
            logger.info("Produtos por deal processados: %d/%d", contador, len(deal_ids))
    logger.info("Deals com produtos mapeados: %d", len(produtos_por_deal))
    return produtos_por_deal


# Monta lookup {id: name} dos produtos
# Busca um por um pelo ID para pegar o nome
def extract_lookup_produtos(client: RDStationClient, ids_produtos: set[str]) -> dict[str, str]:
    lookup_produtos = {}
    for contador, product_id in enumerate(ids_produtos, 1):
        try:
            dados_produto = client.get_product(product_id)
            lookup_produtos[product_id] = dados_produto.get("name", "")
        except Exception as erro:
            logger.warning("Erro ao buscar produto %s: %s", product_id, erro)
            lookup_produtos[product_id] = ""
        if contador % 50 == 0:
            logger.info("Produtos processados: %d/%d", contador, len(ids_produtos))
    logger.info("Lookup de produtos: %d registros", len(lookup_produtos))
    return lookup_produtos
