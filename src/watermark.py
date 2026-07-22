import logging
from datetime import datetime, timedelta

from sqlalchemy import text

from config.settings import POSTGRES_SCHEMA
from drivers.database import get_engine
from utils.date_utils import TIMEZONE_SP

logger = logging.getLogger(__name__)

BUFFER_HORAS = 2


# Consulta o data_atualizacao mais recente já gravado e devolve o cutoff
# (com margem de segurança) a ser usado no filtro incremental da API.
# Retorna None se a tabela estiver vazia (sem watermark ainda).
def get_cutoff_atualizacao(table_name: str) -> datetime | None:
    engine = get_engine()

    with engine.connect() as conn:
        resultado = conn.execute(
            text(f"SELECT MAX(data_atualizacao) FROM {POSTGRES_SCHEMA}.{table_name}")
        )
        ultima_atualizacao = resultado.scalar()

    if ultima_atualizacao is None:
        logger.warning(
            "Nenhum registro em %s.%s — sem watermark para o incremental",
            POSTGRES_SCHEMA,
            table_name,
        )
        return None

    # A coluna é "timestamp without time zone" e o DW grava horário local de SP —
    # é esse o fuso que precisa ser atribuído aqui. O build_updated_at_filter
    # converte para UTC depois, que é como a API interpreta o filtro.
    if ultima_atualizacao.tzinfo is None:
        ultima_atualizacao = ultima_atualizacao.replace(tzinfo=TIMEZONE_SP)

    cutoff = ultima_atualizacao - timedelta(hours=BUFFER_HORAS)
    logger.info(
        "Última atualização: %s | Cutoff (margem de %dh): %s",
        ultima_atualizacao,
        BUFFER_HORAS,
        cutoff,
    )
    return cutoff
