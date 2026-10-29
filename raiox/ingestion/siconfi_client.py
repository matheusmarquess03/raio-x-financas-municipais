"""Cliente da API de dados abertos do SICONFI (Tesouro Nacional).

Documentação: https://apidatalake.tesouro.gov.br/docs/siconfi/
"""

import logging
import time
from collections.abc import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"

log = logging.getLogger(__name__)


class SiconfiClient:
    """Faz as requisições com retry automático, pausa entre chamadas e paginação."""

    def __init__(self, base_url: str = BASE_URL, pausa: float = 1.0, timeout: int = 60):
        self.base_url = base_url
        self.pausa = pausa
        self.timeout = timeout
        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def get(self, endpoint: str, **params) -> Iterator[dict]:
        """Retorna todos os registros do endpoint, percorrendo todas as páginas."""
        offset = 0
        while True:
            resposta = self.session.get(
                f"{self.base_url}/{endpoint}",
                params={**params, "offset": offset},
                timeout=self.timeout,
            )
            resposta.raise_for_status()
            pagina = resposta.json()
            itens = pagina.get("items", [])
            log.debug("%s offset=%d: %d itens", endpoint, offset, len(itens))
            yield from itens
            time.sleep(self.pausa)
            if not pagina.get("hasMore") or not itens:
                return
            offset += len(itens)
