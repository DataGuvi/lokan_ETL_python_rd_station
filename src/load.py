import logging

import pandas as pd
from sqlalchemy import MetaData, Table, text

from config.settings import POSTGRES_SCHEMA
from drivers.database import get_engine
from utils.date_utils import now_sao_paulo

logger = logging.getLogger(__name__)

# Colunas que nunca devem ser sobrescritas por um upsert (controladas por outro processo)
COLUNAS_PROTEGIDAS_NO_UPDATE = {"registro_deletado"}

# Fração máxima da fato que pode ser marcada como deletada numa única rodada.
# Protege contra paginação incompleta da API marcando a base inteira por engano.
LIMITE_MARCACAO_DELETADOS = 0.20


def load_to_staging(df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> None:
    df = df.copy()
    if "id_negociacao" in df.columns:
        df = df.drop_duplicates(subset=["id_negociacao"], keep="last")
    df["time_import"] = now_sao_paulo()

    engine = get_engine()

    # PostgreSQL aceita no máximo 32.767 parâmetros por query (int16).
    # Com method="multi", total de parâmetros = num_linhas * num_colunas.
    num_cols = max(1, len(df.columns))
    safe_chunksize = min(500, max(1, 30000 // num_cols))

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {POSTGRES_SCHEMA}.{table_name}"))

        df.to_sql(
            name=table_name,
            schema=POSTGRES_SCHEMA,
            con=conn,
            if_exists="append",
            index=False,
            chunksize=safe_chunksize,
            method="multi",
        )

    logger.info(
        "%s registros carregados para %s.%s (chunksize=%d)",
        len(df),
        POSTGRES_SCHEMA,
        table_name,
        safe_chunksize,
    )


# Insere/atualiza as linhas do df na tabela fato via uma tabela de stage:
# 1. Trunca a stage e carrega o lote nela (bulk, sem checar conflito)
# 2. INSERT dos ids que ainda não existem no fato
# 3. UPDATE dos ids que já existem, comparando pela chave_primaria
# Usado no fluxo incremental — nunca faz TRUNCATE na tabela fato.
def upsert_staging(df: pd.DataFrame, table_name: str, staging_table_name: str, chave_primaria: str = "id_negociacao") -> None:
    if df.empty:
        logger.info("Nenhum registro para upsert em %s.%s", POSTGRES_SCHEMA, table_name)
        return

    df = df.copy()
    df["time_import"] = now_sao_paulo()
    # A stage tem PK em chave_primaria — garante que o lote não tem ids repetidos
    df = df.drop_duplicates(subset=[chave_primaria], keep="last")

    engine = get_engine()
    tabela_fato = Table(table_name, MetaData(), schema=POSTGRES_SCHEMA, autoload_with=engine)

    # Só grava/atualiza colunas que o df realmente carrega — evita sobrescrever com
    # NULL qualquer coluna da tabela que o transform não preencher no futuro.
    colunas_df = set(df.columns)
    colunas_comuns = [col.name for col in tabela_fato.columns if col.name in colunas_df]
    colunas_sql = ", ".join(colunas_comuns)
    colunas_update = [
        c for c in colunas_comuns
        if c != chave_primaria and c not in COLUNAS_PROTEGIDAS_NO_UPDATE
    ]
    set_clause = ", ".join(f"{c} = stg.{c}" for c in colunas_update)

    num_cols = max(1, len(colunas_comuns))
    safe_chunksize = min(500, max(1, 30000 // num_cols))

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {POSTGRES_SCHEMA}.{staging_table_name}"))

        df[colunas_comuns].to_sql(
            name=staging_table_name,
            schema=POSTGRES_SCHEMA,
            con=conn,
            if_exists="append",
            index=False,
            chunksize=safe_chunksize,
            method="multi",
        )

        resultado_insert = conn.execute(text(f"""
            INSERT INTO {POSTGRES_SCHEMA}.{table_name} ({colunas_sql})
            SELECT {colunas_sql}
            FROM {POSTGRES_SCHEMA}.{staging_table_name} stg
            WHERE NOT EXISTS (
                SELECT 1 FROM {POSTGRES_SCHEMA}.{table_name} fato
                WHERE fato.{chave_primaria} = stg.{chave_primaria}
            )
        """))

        resultado_update = conn.execute(text(f"""
            UPDATE {POSTGRES_SCHEMA}.{table_name} fato
            SET {set_clause}
            FROM {POSTGRES_SCHEMA}.{staging_table_name} stg
            WHERE fato.{chave_primaria} = stg.{chave_primaria}
        """))

    logger.info(
        "Upsert via stage: %d linhas no lote -> %d inseridas, %d atualizadas em %s.%s",
        len(df),
        resultado_insert.rowcount,
        resultado_update.rowcount,
        POSTGRES_SCHEMA,
        table_name,
    )


# Sobe os ids da listagem da API para a stage e compara com a fato.
# Retorna (candidatos, restaurados):
#   candidatos  - ids que estão na fato mas não vieram na listagem. NÃO são
#                 exclusões confirmadas: a paginação da API pula registros, então
#                 cada um precisa ser verificado por confirmar_deals_excluidos().
#   restaurados - ids que voltaram a aparecer na listagem e estavam marcados como
#                 deletados; presença na API é prova positiva, então desmarca na hora.
def listar_candidatos_deletados(
    ids_listagem: list[str],
    table_name: str,
    staging_table_name: str,
    chave_primaria: str = "id_negociacao",
) -> tuple[list[str], int]:
    if not ids_listagem:
        logger.error(
            "Listagem da API vazia — verificação de deletados abortada em %s.%s",
            POSTGRES_SCHEMA,
            table_name,
        )
        return ([], 0)

    # A stage tem PK em chave_primaria — ids repetidos quebrariam o insert
    df = pd.DataFrame({chave_primaria: sorted(set(ids_listagem))})
    df["time_import"] = now_sao_paulo()

    safe_chunksize = min(500, max(1, 30000 // len(df.columns)))

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {POSTGRES_SCHEMA}.{staging_table_name}"))

        df.to_sql(
            name=staging_table_name,
            schema=POSTGRES_SCHEMA,
            con=conn,
            if_exists="append",
            index=False,
            chunksize=safe_chunksize,
            method="multi",
        )

        candidatos = [
            linha[0]
            for linha in conn.execute(text(f"""
                SELECT fato.{chave_primaria}
                FROM {POSTGRES_SCHEMA}.{table_name} fato
                WHERE NOT EXISTS (
                    SELECT 1 FROM {POSTGRES_SCHEMA}.{staging_table_name} stg
                    WHERE stg.{chave_primaria} = fato.{chave_primaria}
                )
            """))
        ]

        resultado_restaurados = conn.execute(text(f"""
            UPDATE {POSTGRES_SCHEMA}.{table_name} fato
            SET registro_deletado = FALSE
            WHERE EXISTS (
                SELECT 1 FROM {POSTGRES_SCHEMA}.{staging_table_name} stg
                WHERE stg.{chave_primaria} = fato.{chave_primaria}
            )
            AND fato.registro_deletado IS TRUE
        """))

    logger.info(
        "Comparação com a stage: %d ids na listagem -> %d candidatos a deletado, %d restaurados",
        len(df),
        len(candidatos),
        resultado_restaurados.rowcount,
    )
    return (candidatos, resultado_restaurados.rowcount)


# Soft delete: marca registro_deletado = TRUE nos ids já confirmados como excluídos
# na origem. Não apaga nada — a linha continua na fato para histórico.
def marcar_registros_deletados(
    ids_excluidos: list[str],
    table_name: str,
    chave_primaria: str = "id_negociacao",
) -> int:
    if not ids_excluidos:
        logger.info("Nenhuma exclusão confirmada para marcar em %s.%s", POSTGRES_SCHEMA, table_name)
        return 0

    engine = get_engine()
    with engine.begin() as conn:
        total_fato = conn.execute(
            text(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.{table_name}")
        ).scalar()

        # Rede de segurança: mesmo confirmadas uma a uma, uma marcação em massa
        # indica algo errado (API respondendo 404 pra tudo, por exemplo).
        # O raise desfaz a transação inteira.
        if total_fato and len(ids_excluidos) / total_fato > LIMITE_MARCACAO_DELETADOS:
            raise RuntimeError(
                f"Marcação de deletados abortada: {len(ids_excluidos)} de {total_fato} registros "
                f"({len(ids_excluidos) / total_fato:.1%}) seriam marcados, acima do limite de "
                f"{LIMITE_MARCACAO_DELETADOS:.0%}."
            )

        # IS DISTINCT FROM TRUE (e não = FALSE) porque a coluna aceita NULL
        resultado = conn.execute(
            text(f"""
                UPDATE {POSTGRES_SCHEMA}.{table_name}
                SET registro_deletado = TRUE
                WHERE {chave_primaria} = ANY(:ids)
                AND registro_deletado IS DISTINCT FROM TRUE
            """),
            {"ids": list(ids_excluidos)},
        )

    logger.info(
        "%d registros marcados como deletados em %s.%s",
        resultado.rowcount,
        POSTGRES_SCHEMA,
        table_name,
    )
    return resultado.rowcount