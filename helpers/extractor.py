import json
import time
import random
import types

import tqdm
from bs4 import BeautifulSoup
from google import genai
import google.genai.errors

from connection import (
    get_db_connection, get_db_credentials,
    query_url_as_human, get_ai_client
)
from helpers.models import ListingAdditionalInfo, ListingAIMetadata, ListingAIInfo, ListingGone

__all__ = [
    "process_missing_metadata"
]

def get_html(html_file_path: str):
    try:
      with open(html_file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    except FileNotFoundError:
      print(f"Error: File not found at {html_file_path}")
      return None
    except Exception as e:
      print(f"Error reading file: {e}")
      return None
    return html_content


def extract_info(listing_id: int, html_content: str | None) -> ListingAdditionalInfo | ListingGone:
    if html_content is None:
        res = ListingGone(listing_id=listing_id)
        return res
    soup = BeautifulSoup(html_content, 'html.parser')
    script = soup.find_all('script')[-1].text
    info = json.loads(script)
    if info.get("page") == '/pl/wyniki/[[...searchingCriteria]]':
        res = ListingGone(listing_id=listing_id)
        return res
    ad_info = info['props']['pageProps']['ad']

    tg = ad_info['target']

    floor_info = tg.get('Floor_no')
    if not floor_info:
        floor = None
    else:
        floor_info = floor_info[0]
        if floor_info == 'ground_floor':
            floor = 0
        else:
            floor = int(floor_info.split('_')[-1])
    floor_total = tg.get('Building_floors_num')

    extras = tg.get('Extras_types') or []

    window_info = (tg.get("Windows_type") or [])
    windows = None if not window_info else window_info[0]

    soup_inner = BeautifulSoup(ad_info['description'], "html.parser")
    description_long = soup_inner.get_text(separator=" ", strip=True)

    metadata = ListingAdditionalInfo(
        listing_id=ad_info['id'],
        description_long=description_long,
        deposit=tg.get("Deposit"),
        floor=floor,
        floors_total=floor_total,
        has_ac='air_conditioning' in extras,
        has_lift='lift' in extras,
        windows=windows,
        latitude=str(ad_info['location']['coordinates']['latitude']),
        longitude=str(ad_info['location']['coordinates']['longitude']),
        raw_info=json.dumps(ad_info),
    )

    return metadata


def extract_ai_info(listing_id: int, html_content: str, client) -> ListingAIInfo:

    message = f"""
    Based on the following description, find the following information. 
    If any of the pieces are missing  - return null for that piece.
    allowed_with_pets: if it is explicitly allowed to live with pets in the apartment
    availability_date: since when the apartment is available to be leased
    bedroom_number: the number of bedrooms or rooms that could be used as a bedroom (excluding kitchen)
    kitchen_combined_with_living_room: whether the kitchen is combined with a living room (true), or is it a separate room
    occasional_lease: whether the occasional lease agreement (najem okazjonalny) is required
    Description:
    {html_content}
    """

    ai_data = dict(
        # model='gemini-2.5-flash-preview-04-17',
        model='gemini-2.0-flash',
        contents=message,
            config={
                "response_mime_type": "application/json",
                "response_schema": ListingAIMetadata,
            })

    try:
        response = client.models.generate_content(**ai_data)
    except genai.errors.ClientError as e:
        if e.status == 'RESOURCE_EXHAUSTED':
            time.sleep(60)
        else:
            raise
        response = client.models.generate_content(**ai_data)
    inst = response.parsed
    ai_info = ListingAIInfo.from_ai_metadata(inst, listing_id=listing_id)
    return ai_info



def get_slugs(cursor) -> list[tuple[int, str]]:
    query = """
    select listing_id, min(url) as url 
    from otodom.listing_info_full
    where 1=1
    and not scraped
    and not irrelevant
    group by listing_id 
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return results


def get_html_url(url: str) -> str | None:
    data = query_url_as_human(url)
    if data:
        data = data.text
    return data


def process_missing_metadata(cursor, conn, ai_client):
    urls = get_slugs(cursor)
    for listing_id, url in tqdm.tqdm(urls):
        body = get_html_url(url)
        metadata = extract_info(listing_id,  body)
        metadata.to_db(cursor)
        conn.commit()
        if not isinstance(metadata, ListingGone):
            ai_info = extract_ai_info(listing_id, body, ai_client)
            ai_info.to_db(conn.cursor())
            conn.commit()
        time.sleep(random.randint(0, 1000)/1000)

    conn.close()
    return None
