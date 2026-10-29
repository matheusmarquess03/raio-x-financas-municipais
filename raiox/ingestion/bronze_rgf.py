"""Bronze layer: downloads RGF Annex 01 (personnel expenses) for a state's municipalities.

Data is written exactly as returned by the API (JSON Lines), plus the
ingestion timestamp. No business rules are applied here.
"""

import argparse
import json
import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any

import requests

from raiox.ingestion.siconfi_client import SiconfiClient

logger = logging.getLogger(__name__)

Record = dict[str, Any]

BRONZE_DIR = Path("data/bronze/siconfi")
# On-disk names are kept in Portuguese to preserve the existing bronze layout.
INGESTION_COLUMN = "_ingerido_em"
FISCAL_YEAR_PARTITION = "exercicio"

# Municipalities with fewer than 50k inhabitants may publish the semiannual
# (simplified) RGF. We try the quadrimestral report first and, if it comes
# back empty, fall back to the semiannual one.
PERIODS: tuple[Mapping[str, str | int], ...] = (
    MappingProxyType({"in_periodicidade": "Q", "nr_periodo": 3, "co_tipo_demonstrativo": "RGF"}),
    MappingProxyType(
        {"in_periodicidade": "S", "nr_periodo": 2, "co_tipo_demonstrativo": "RGF Simplificado"}
    ),
)


def get_state_municipalities(client: SiconfiClient, state: str) -> list[Record]:
    """Return the municipal entities (sphere "M") of the given state."""
    return [e for e in client.get("entes") if e.get("esfera") == "M" and e.get("uf") == state]


def download_personnel_rgf(client: SiconfiClient, ibge_code: int, fiscal_year: int) -> list[Record]:
    """Download RGF Annex 01 for one municipality, or an empty list if unpublished."""
    for period in PERIODS:
        rows = list(
            client.get(
                "rgf",
                an_exercicio=fiscal_year,
                id_ente=ibge_code,
                co_poder="E",
                no_anexo="RGF-Anexo 01",
                **period,
            )
        )
        if rows:
            return rows
    return []


def with_ingestion_timestamp(records: Sequence[Record], ingested_at: str) -> list[Record]:
    """Return copies of the records with the ingestion timestamp added."""
    return [{**record, INGESTION_COLUMN: ingested_at} for record in records]


def save_jsonl(records: Sequence[Record], destination: Path) -> None:
    """Write the records as JSON Lines, creating the needed directories."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def ingest_entities(client: SiconfiClient, state: str, ingested_at: str) -> list[Record]:
    """Write the state's municipalities to bronze and return them."""
    municipalities = get_state_municipalities(client, state)
    logger.info("%d municipalities found in %s", len(municipalities), state)
    save_jsonl(
        with_ingestion_timestamp(municipalities, ingested_at),
        BRONZE_DIR / "entes" / f"uf={state}" / "data.jsonl",
    )
    return municipalities


def ingest_rgf(
    client: SiconfiClient,
    municipalities: Sequence[Record],
    state: str,
    fiscal_year: int,
    ingested_at: str,
) -> None:
    """Download each municipality's RGF and write the fiscal-year/state partition to bronze."""
    records: list[Record] = []
    without_data: list[str] = []
    total = len(municipalities)

    for i, municipality in enumerate(municipalities, start=1):
        name = municipality["ente"]
        try:
            rows = download_personnel_rgf(client, municipality["cod_ibge"], fiscal_year)
        except requests.RequestException:
            logger.exception("[%d/%d] %s: API failure", i, total, name)
            without_data.append(name)
            continue
        if not rows:
            without_data.append(name)
        records.extend(with_ingestion_timestamp(rows, ingested_at))
        logger.info("[%d/%d] %s: %d rows", i, total, name, len(rows))

    destination = (
        BRONZE_DIR
        / "rgf_anexo01"
        / f"{FISCAL_YEAR_PARTITION}={fiscal_year}"
        / f"uf={state}"
        / "data.jsonl"
    )
    save_jsonl(records, destination)
    logger.info("%d rows written to %s", len(records), destination)
    if without_data:
        logger.warning(
            "%d municipalities without a published RGF: %s",
            len(without_data),
            ", ".join(without_data),
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default="RJ", help="state abbreviation (default: %(default)s)")
    parser.add_argument(
        "--fiscal-year", type=int, default=2025, help="fiscal year (default: %(default)s)"
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client = SiconfiClient()
    ingested_at = datetime.now(UTC).isoformat()

    municipalities = ingest_entities(client, args.state, ingested_at)
    ingest_rgf(client, municipalities, args.state, args.fiscal_year, ingested_at)


if __name__ == "__main__":
    main()
