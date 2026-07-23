import datetime
import logging
import traceback

from config.settings import PROJECT_NAME
from drivers.api_client import RDStationClient
from src.extract import (
    extract_deals_atualizados,
    extract_ids_deals,
    confirmar_deals_excluidos,
    extract_lookup_vendedores,
    extract_lookup_origens,
    extract_lookup_motivos_perda,
    extract_lookup_organizacoes,
    extract_lookup_contatos,
    extract_produtos_por_deal,
    extract_lookup_produtos,
)
from src.transform import transform_deals
from src.load import (
    criar_staging,
    dropar_staging,
    listar_candidatos_deletados,
    marcar_registros_deletados,
    reconstruir_prata,
    upsert_staging,
)
from src.watermark import get_cutoff_atualizacao
from utils.date_utils import now_sao_paulo_naive
from utils.db_logger import DBLogger
from utils.log_mail_message import send_email
from utils.logger import setup_logger

logger = logging.getLogger(__name__)

TABELA = "bronze_rd_station_negociacoes"
TABELA_STAGE = "stg_rd_station_negociacoes"
TABELA_PRATA = "prata_rd_station_negociacoes"
OPERACAO = "etl_incremental_negociacoes"


def run_etl_incremental():

    db_logger = DBLogger()
    start_time_dt = now_sao_paulo_naive()

    try:
        setup_logger()
        logger.info("Iniciando pipeline ETL incremental")

        cutoff = get_cutoff_atualizacao(TABELA)
        if cutoff is None:
            # Vira exceção para cair no tratamento único de erro (log + e-mail):
            # sem watermark o incremental não roda, e isso precisa de ação humana.
            raise RuntimeError(
                f"Tabela {TABELA} está vazia — rode main.py (full load) antes do incremental."
            )

        client = RDStationClient()

        logger.info("Recriando tabela de stage...")
        criar_staging(TABELA, TABELA_STAGE)
        try:
            logger.info("1. Extraindo deals atualizados desde %s...", cutoff)
            deals = extract_deals_atualizados(client, cutoff)

            # Roda antes do return: exclusão no RD Station não gera atualização,
            # então precisa ser verificada mesmo quando nada foi atualizado.
            logger.info("2. Verificando registros deletados no RD Station...")
            # A listagem paginada da API pula registros, então id ausente dela é só
            # candidato — a marcação só acontece após confirmar um a um (404).
            ids_listagem = extract_ids_deals(client)
            candidatos, _ = listar_candidatos_deletados(ids_listagem, TABELA, TABELA_STAGE)
            ids_excluidos = confirmar_deals_excluidos(client, candidatos)
            marcar_registros_deletados(ids_excluidos, TABELA)

            if not deals:
                logger.info("Nenhum deal atualizado desde o cutoff. Pipeline finalizado.")
                reconstruir_prata(TABELA, TABELA_PRATA)
                db_logger.log_operation(
                    operation=OPERACAO,
                    status="SUCCESS",
                    start_time=start_time_dt,
                    end_time=now_sao_paulo_naive(),
                )
                return

            logger.info("3. Extraindo lookups...")
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

            logger.info("4. Transformando dados...")
            df = transform_deals(deals, lookups)

            logger.info("5. Upsert no banco (via stage)...")
            upsert_staging(df, TABELA, TABELA_STAGE)
        finally:
            dropar_staging(TABELA_STAGE)

        logger.info("6. Reconstruindo prata a partir da bronze...")
        reconstruir_prata(TABELA, TABELA_PRATA)

        logger.info("Pipeline ETL incremental finalizado com sucesso")
        db_logger.log_operation(
            operation=OPERACAO,
            status="SUCCESS",
            start_time=start_time_dt,
            end_time=now_sao_paulo_naive(),
        )
    except Exception as e:
        logger.exception("Pipeline ETL incremental falhou")
        db_logger.log_operation(
            operation=OPERACAO,
            status="FAILED",
            start_time=start_time_dt,
            end_time=now_sao_paulo_naive(),
            error_reason=str(e),
        )

        assunto = "[FALHA ENGENHARIA] Lokan - Python ETL API RD STATION"
        corpo = (
            f"Falha no pipeline ETL incremental ({PROJECT_NAME}).\n\n"
            f"Operação: {OPERACAO}\n"
            f"Início: {start_time_dt:%d/%m/%Y %H:%M:%S}\n"
            f"Erro: {type(e).__name__}: {e}\n\n"
            f"{traceback.format_exc()}"
        )
        send_email(assunto, corpo)


if __name__ == "__main__":
    run_etl_incremental()
