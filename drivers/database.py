from sqlalchemy import create_engine

from config.settings import DATABASE_URL

# Engine singleton — reutiliza a mesma conexão em toda a aplicação,
# evitando criar um pool novo a cada chamada.
_engine = None


def get_engine():
    # Retorna o engine existente ou cria um novo na primeira chamada.
    # pool_pre_ping=True testa a conexão antes de usar (evita erros com conexões expiradas).
    # sslmode=require já está na DATABASE_URL (exigido pelo Azure PostgreSQL).
    global _engine
    if _engine is None:
        _engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return _engine
