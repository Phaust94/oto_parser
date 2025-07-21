import sys
import time
import random

import tqdm

from helpers.connection import query_url_as_human, CITY
from helpers.models_base import (
    ListingAdditionalInfo,
    ListingAIInfo,
    ListingGone,
)
from helpers.services import Service

__all__ = [
    "process_missing_metadata",
    "process_missing_ai_metadata",
    "check_alive",
]


def get_html(html_file_path: str):
    try:
        with open(html_file_path, "r", encoding="utf-8") as f:
            html_content = f.read()
    except FileNotFoundError:
        print(f"Error: File not found at {html_file_path}")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None
    return html_content


def extract_info(
    listing_id: str, html_content: str | None, service: Service
) -> ListingAdditionalInfo | ListingGone:
    if html_content is None:
        res = ListingGone(listing_id=listing_id, service=service.value)
        return res
    metadata = service.listing_metadata_model_class.from_text(
        text=html_content,
        listing_id=listing_id,
        city=CITY,
    )
    return metadata


def extract_ai_info(
    listing_id: str,
    html_content: str,
    client,
    service: Service,
) -> ListingAIInfo:

    message = f"""
    Based on the following description, find the following information. 
    If any of the pieces are missing  - return null for that piece.
    {service.listing_ai_metadata_model_class.prompt}
    Description:
    {html_content}
    """

    ai_data = dict(
        model="gemini-2.5-pro",
        # model="gemini-2.0-flash",
        contents=message,
        config={
            "response_mime_type": "application/json",
            "response_schema": service.listing_ai_metadata_schema_class,
        },
    )

    try:
        response = client.models.generate_content(**ai_data)
    except Exception as e:
        if getattr(e, "status") == "RESOURCE_EXHAUSTED":
            delay = 60
        else:
            delay = 1
            ai_data["model"] = "gemini-2.0-flash-lite"
        time.sleep(delay)
        response = client.models.generate_content(**ai_data)
    inst = response.parsed
    ai_info = service.listing_ai_metadata_model_class.from_ai_metadata(
        inst, listing_id=listing_id, city=CITY
    )
    return ai_info


def get_slugs(cursor, service: Service) -> list[tuple[str, str]]:
    query = f"""
    select listing_id, min(url) as url 
    from listing_info_full
    where 1=1
    and not scraped
    and not irrelevant
    and service = '{service.value}'
    group by listing_id 
    order by listing_id 
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return results


def get_slugs_no_ai(cursor, service: Service) -> list[tuple[str, str, str]]:
    query = f"""
    with urls as (
        select 
            {service.info_for_ai.select_columns}
        from listing_info_full
        where 1=1
        and scraped
        and not irrelevant
        and parsed_on is null
        and service = '{service.value}'
        group by listing_id 
        order by listing_id 
    )
    select 
        urls.* {service.info_for_ai.select_top_level_additional}
    from urls
    {service.info_for_ai.joins_additional}
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return results


def get_slugs_alive(cursor, service: Service) -> list[tuple[str, str]]:
    query = f"""
    select listing_id, min(url) as url 
    from listing_info_full
    where 1=1
    and not irrelevant
    and service = '{service.value}'
    group by listing_id 
    order by listing_id 
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return results


def get_html_url(url: str) -> str | None:
    data = query_url_as_human(url)
    if data:
        data = data.text
    return data


def process_missing_metadata(
    cursor, conn, ai_client, service: Service
) -> list[tuple[str, str]]:
    urls = get_slugs(cursor, service)
    for listing_id, url in tqdm.tqdm(urls, file=sys.stdout):
        body = get_html_url(url)
        metadata = extract_info(listing_id, body, service)
        metadata.to_db(cursor)
        conn.commit()
        if not isinstance(metadata, ListingGone):
            text_for_ai = getattr(metadata, service.info_for_ai.select_columns)
            ai_info = extract_ai_info(listing_id, text_for_ai, ai_client, service)
            ai_info.to_db(conn.cursor())
            conn.commit()
        time.sleep(random.randint(0, 1000) / 1000)

    return urls


def process_missing_ai_metadata(
    cursor, conn, ai_client, service: Service
) -> list[tuple[str, str]]:
    urls = get_slugs_no_ai(cursor, service)
    for listing_id, url, raw_info in tqdm.tqdm(urls, file=sys.stdout):
        ai_info = extract_ai_info(listing_id, raw_info, ai_client, service)
        ai_info.to_db(conn.cursor())
        conn.commit()
        time.sleep(random.randint(0, 1000) / 1000)
    urls_fixed = [x[:2] for x in urls]
    # noinspection PyTypeChecker
    return urls_fixed


def check_alive(cursor, conn, service: Service) -> tuple[list[str], list[str]]:
    urls = get_slugs_alive(cursor, service)
    alive, dead = [], []
    for listing_id, url in tqdm.tqdm(urls, file=sys.stdout):
        body = get_html_url(url)
        if body is None:
            metadata = ListingGone(listing_id=listing_id, service=service.value)
            metadata.to_db(cursor)
            conn.commit()
            dead.append(listing_id)
        else:
            alive.append(listing_id)
        time.sleep(random.randint(0, 3000) / 1000)
    return alive, dead
