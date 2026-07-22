import datetime
import logging

from sqlalchemy import text

from config.settings import POSTGRES_SCHEMA, PROJECT_NAME
from drivers.database import get_engine

logger = logging.getLogger(__name__)

TABELA_LOGS = "etl_execution_logs"


# Registra o resultado de cada execução do ETL na etl_execution_logs.
class DBLogger:
    def __init__(self, project_name: str = PROJECT_NAME):
        self.project_name = project_name

    # Nunca levanta exceção: se o log falhar, a causa original do erro (e o e-mail
    # de alerta) não pode ser perdida por causa do registro do log.
    def log_operation(
        self,
        operation: str,
        status: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        error_reason: str | None = None,
    ) -> None:
        try:
            with get_engine().begin() as conn:
                conn.execute(
                    text(f"""
                        INSERT INTO {POSTGRES_SCHEMA}.{TABELA_LOGS}
                            (project_name, operation, status, error_reason, start_time, end_time)
                        VALUES
                            (:project_name, :operation, :status, :error_reason, :start_time, :end_time)
                    """),
                    {
                        "project_name": self.project_name,
                        "operation": operation,
                        "status": status,
                        "error_reason": error_reason,
                        "start_time": start_time,
                        "end_time": end_time,
                    },
                )
            logger.info("Execução registrada em %s.%s: %s %s", POSTGRES_SCHEMA, TABELA_LOGS, operation, status)
        except Exception as erro:
            logger.error("Falha ao gravar log de execução em %s.%s: %s", POSTGRES_SCHEMA, TABELA_LOGS, erro)
