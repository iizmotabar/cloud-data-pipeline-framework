"""
Generic extractor pattern for a paginated REST API. Meant to be subclassed or
parameterized per source rather than rewritten each time, so every extractor in
the pipeline handles pagination, retries, and rate limiting the same way.
"""

import logging
import time
from dataclasses import dataclass

import requests
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("extract_api_source")


@dataclass
class APISourceConfig:
    base_url: str
    auth_header: dict
    page_param: str = "page"
    page_size_param: str = "per_page"
    page_size: int = 100
    results_key: str = "data"
    destination_table: str = ""


def fetch_all_pages(config: APISourceConfig, max_retries: int = 3) -> list[dict]:
    """Pulls every page from a paginated API endpoint, retrying transient failures."""
    all_records = []
    page = 1

    while True:
        params = {config.page_param: page, config.page_size_param: config.page_size}

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    config.base_url, headers=config.auth_header, params=params, timeout=30
                )
                response.raise_for_status()
                break
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for page {page}: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)

        payload = response.json()
        records = payload.get(config.results_key, [])

        if not records:
            break

        all_records.extend(records)
        logger.info(f"Fetched page {page}, {len(records)} records ({len(all_records)} total)")

        page += 1
        time.sleep(0.2)  # basic rate limit courtesy

    return all_records


def load_to_warehouse(records: list[dict], config: APISourceConfig) -> None:
    """Loads raw records into the destination table's landing zone, untransformed."""
    bq_client = bigquery.Client()
    errors = bq_client.insert_rows_json(config.destination_table, records)
    if errors:
        raise RuntimeError(f"Failed to load records into {config.destination_table}: {errors}")
    logger.info(f"Loaded {len(records)} records into {config.destination_table}")


def run(config: APISourceConfig) -> None:
    records = fetch_all_pages(config)
    load_to_warehouse(records, config)


if __name__ == "__main__":
    example_config = APISourceConfig(
        base_url="https://api.example-crm.com/v1/contacts",
        auth_header={"Authorization": "Bearer REPLACE_WITH_TOKEN"},
        destination_table="project.raw.crm_contacts",
    )
    run(example_config)
