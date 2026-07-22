from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Servidor roda em Linux — fixamos o fuso explicitamente em vez de depender do TZ do SO
TIMEZONE_SP = ZoneInfo("America/Sao_Paulo")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_sao_paulo() -> datetime:
    return datetime.now(TIMEZONE_SP)


# Horário de São Paulo SEM tzinfo. O DW grava horário local em colunas
# "timestamp without time zone" — e um datetime tz-aware seria convertido para UTC
# pelo to_sql do pandas na hora de gravar, deixando a coluna 3h adiantada.
def now_sao_paulo_naive() -> datetime:
    return now_sao_paulo().replace(tzinfo=None)


def format_iso(dt: datetime) -> str:
    return dt.isoformat()


# RDQL (filtro da API do RD Station) exige DateTime em ISO 8601 com "T" —
# separar data e hora com espaço devolve HTTP 500 (em qualquer codificação de URL).
# Verificado que "...T13:11:07" e "...T13:11:07Z" filtram o mesmo conjunto, ou seja,
# a API lê valor sem fuso como UTC. Convertemos e marcamos com Z para não depender disso.
def format_rdstation_datetime(dt: datetime) -> str:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_updated_at_filter(cutoff: datetime) -> str:
    return f"updated_at:>={format_rdstation_datetime(cutoff)}"
