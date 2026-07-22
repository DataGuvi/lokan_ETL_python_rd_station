import logging
import os
import time

import requests

from config.settings import RD_API_BASE_URL, RD_OAUTH_URL, RD_CLIENT_ID, RD_CLIENT_SECRET, RD_REFRESH_TOKEN

logger = logging.getLogger(__name__)

# RD Station CRM: limite de 120 requisições/minuto (todos os planos). Margem de
# segurança abaixo do teórico (0.5s/120 por min) pra absorver jitter de relógio.
MIN_REQUEST_INTERVAL_SECONDS = 0.55

# A paginação de /deals é não-determinística: a API reporta sempre o mesmo total,
# mas cada varredura pula um subconjunto aleatório (~5-15%) e duplica outros entre
# páginas. Os parâmetros sort/order são ignorados, e não há cursor — o link "next"
# é só page+1. Como o pulo é aleatório, varrer repetidamente e unir os resultados
# converge para o conjunto completo (medido: 3 varreduras para fechar 1290 de 1290).
MAX_VARREDURAS = 10
VARREDURAS_ESTAVEIS_PARA_PARAR = 2


class RDStationClient:
    def __init__(self):
        self.base_url = RD_API_BASE_URL
        self.oauth_url = RD_OAUTH_URL
        self.client_id = RD_CLIENT_ID
        self.client_secret = RD_CLIENT_SECRET
        self.refresh_token = RD_REFRESH_TOKEN
        self.access_token = None
        self.token_expires_at = None
        self._last_request_at: float | None = None

    def _authenticate(self):
       payload = {
           "client_id": self.client_id,
           "client_secret": self.client_secret,
           "refresh_token": self.refresh_token,
           "grant_type": "refresh_token",
       }

       response = requests.post(self.oauth_url, json=payload)

       if not response.ok:
           logger.error("Erro na autenticação: %s - %s", response.status_code, response.text)
           response.raise_for_status()

       data = response.json()
       self.access_token = data["access_token"]
       self.refresh_token = data["refresh_token"]

       expires_in = data["expires_in"]
       self.token_expires_at = time.time() + expires_in

       # Salva os tokens no .env para não perder entre execuções
       self._save_tokens(data["access_token"], data["refresh_token"])

       logger.info("Autenticação realizada com sucesso. Token expira em %ds.", expires_in)
       return data

    def _save_tokens(self, new_access_token: str, new_refresh_token: str):
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
        try:
            with open(env_path, "r") as f:
                lines = f.readlines()
            with open(env_path, "w") as f:
                for line in lines:
                    if line.startswith("RD_REFRESH_TOKEN="):
                        f.write(f"RD_REFRESH_TOKEN={new_refresh_token}\n")
                    elif line.startswith("RD_ACCESS_TOKEN="):
                        f.write(f"RD_ACCESS_TOKEN={new_access_token}\n")
                    else:
                        f.write(line)
            logger.info("Access token e refresh token atualizados no .env")
        except Exception as erro:
            logger.warning("Não foi possível salvar refresh_token no .env: %s", erro)

    def _get_headers(self) -> dict:
        if self.access_token is None or time.time() >= self.token_expires_at:
            self._authenticate()

        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    # Garante um espaçamento mínimo entre o início de cada requisição, dormindo
    # só o tempo que falta (em vez de somar um sleep fixo depois de toda chamada).
    def _throttle(self):
        if self._last_request_at is not None:
            elapsed = time.time() - self._last_request_at
            remaining = MIN_REQUEST_INTERVAL_SECONDS - elapsed
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_at = time.time()

    def _request(self, endpoint: str, params: dict | None = None) -> dict:
        self._throttle()

        url = f"{self.base_url}/{endpoint}"
        headers = self._get_headers()

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning("Rate limit atingido. Aguardando %ds...", retry_after)
            time.sleep(retry_after)
            return self._request(endpoint, params)

        response.raise_for_status()
        return response.json()

    # Paginação genérica — funciona para qualquer endpoint que retorne {"data": [...], "links": {...}}
    def _get_all_paginated(self, endpoint: str, extra_params: dict | None = None) -> list[dict]:
        all_records = []
        page = 1

        while True:
            logger.info("Extraindo %s - página %d", endpoint, page)
            params = {"page[number]": page, "page[size]": 200}
            if extra_params:
                params.update(extra_params)
            response = self._request(endpoint, params=params)

            records = response.get("data", [])
            all_records.extend(records)

            next_link = response.get("links", {}).get("next")
            if not next_link:
                break
            page += 1

        logger.info("Total de %s extraídos: %d", endpoint, len(all_records))
        return all_records

    # filter_str: expressão RDQL (ex: "updated_at:>=2026-07-20 10:00:00") para incremental
    def get_all_deals(self, filter_str: str | None = None) -> list[dict]:
        extra_params = {"filter": filter_str} if filter_str else None
        return self._get_all_paginated("deals", extra_params=extra_params)

    # Varre /deals repetidamente e une os resultados até a união parar de crescer,
    # contornando os registros que a paginação pula. Deduplica por id mantendo a
    # última versão vista (a mais fresca). Use esta em vez de get_all_deals()
    # sempre que faltar registro for um problema.
    def get_all_deals_convergente(self, filter_str: str | None = None) -> list[dict]:
        deals_por_id: dict[str, dict] = {}
        varreduras_estaveis = 0

        for varredura in range(1, MAX_VARREDURAS + 1):
            antes = len(deals_por_id)
            for deal in self.get_all_deals(filter_str=filter_str):
                if deal.get("id"):
                    deals_por_id[deal["id"]] = deal
            novos = len(deals_por_id) - antes
            logger.info(
                "Varredura %d: %d deals conhecidos (+%d novos)",
                varredura,
                len(deals_por_id),
                novos,
            )

            if novos == 0:
                varreduras_estaveis += 1
                if varreduras_estaveis >= VARREDURAS_ESTAVEIS_PARA_PARAR:
                    logger.info("Convergiu em %d varreduras: %d deals", varredura, len(deals_por_id))
                    break
            else:
                varreduras_estaveis = 0
        else:
            logger.warning(
                "Limite de %d varreduras atingido sem convergir — podem faltar deals",
                MAX_VARREDURAS,
            )

        return list(deals_por_id.values())

    # Busca uma negociação pelo ID — levanta HTTPError 404 se ela não existe mais
    def get_deal(self, deal_id: str) -> dict:
        response = self._request(f"deals/{deal_id}")
        return response.get("data", {})

    def get_all_users(self) -> list[dict]:
        return self._get_all_paginated("users")

    def get_all_sources(self) -> list[dict]:
        return self._get_all_paginated("sources")

    # Busca um motivo de perda pelo ID
    def get_lost_reason(self, lost_reason_id: str) -> dict:
        response = self._request(f"lost_reasons/{lost_reason_id}")
        return response.get("data", {})

    # Organizations não tem listagem eficiente — busca uma por uma pelo ID
    def get_organization(self, org_id: str) -> dict:
        response = self._request(f"organizations/{org_id}")
        return response.get("data", {})

    # Busca um contato pelo ID
    def get_contact(self, contact_id: str) -> dict:
        response = self._request(f"contacts/{contact_id}")
        return response.get("data", {})

    # Lista os produtos vinculados a um deal
    def get_deal_products(self, deal_id: str) -> list[dict]:
        response = self._request(f"deals/{deal_id}/products")
        return response.get("data", [])

    # Busca um produto pelo ID (para pegar o name)
    def get_product(self, product_id: str) -> dict:
        response = self._request(f"products/{product_id}")
        return response.get("data", {})

