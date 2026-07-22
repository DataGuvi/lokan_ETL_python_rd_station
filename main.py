import logging

from drivers.api_client import RDStationClient
from src.extract import (
    extract_deals,
    extract_lookup_vendedores,
    extract_lookup_origens,
    extract_lookup_motivos_perda,
    extract_lookup_organizacoes,
    extract_lookup_contatos,
    extract_produtos_por_deal,
    extract_lookup_produtos,
)
from src.transform import transform_deals
from src.load import load_to_staging
from utils.logger import setup_logger

logger = logging.getLogger(__name__)


def run_etl():
    setup_logger()
    logger.info("Iniciando pipeline ETL")

    client = RDStationClient()

    logger.info("1. Extraindo todos os deals...")
    deals = extract_deals(client)
    logger.info("%d deals extraídos", len(deals))

    logger.info("2. Extraindo lookups...")
    lookup_vendedores = extract_lookup_vendedores(client)
    lookup_origens = extract_lookup_origens(client)

    ids_motivos_perda = {deal["lost_reason_id"] for deal in deals if deal.get("lost_reason_id")}
    lookup_motivos_perda = extract_lookup_motivos_perda(client, ids_motivos_perda)

    ids_organizacoes = {deal["organization_id"] for deal in deals if deal.get("organization_id")}
    lookup_organizacoes = extract_lookup_organizacoes(client, ids_organizacoes)

    ids_contatos = set()
    for deal in deals:
        if not deal.get("organization_id") and deal.get("contact_ids"):
            ids_contatos.add(deal["contact_ids"][0])
    lookup_contatos = extract_lookup_contatos(client, ids_contatos)

    deal_ids = [deal["id"] for deal in deals]
    produtos_por_deal = extract_produtos_por_deal(client, deal_ids)

    todos_product_ids = set()
    for product_ids in produtos_por_deal.values():
        todos_product_ids.update(product_ids)
    lookup_produtos = extract_lookup_produtos(client, todos_product_ids)

    lookups = {
        "users": lookup_vendedores,
        "sources": lookup_origens,
        "organizations": lookup_organizacoes,
        "lost_reasons": lookup_motivos_perda,
        "contacts": lookup_contatos,
        "products": lookup_produtos,
        "products_by_deal": produtos_por_deal,
    }

    logger.info("3. Transformando dados...")
    df = transform_deals(deals, lookups)

    logger.info("4. Gravando no banco...")
    load_to_staging(df, "fato_rd_station_negociacoes")
    
    logger.info("Pipeline ETL finalizado com sucesso")


if __name__ == "__main__":
    run_etl()

