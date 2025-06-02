import json
import time
import random
import math

import tqdm
from bs4 import BeautifulSoup
from google import genai
import google.genai.errors

from helpers.connection import query_url_as_human, CITY
from helpers.models import (
    ListingAdditionalInfo,
    ListingAIMetadata,
    ListingAIInfo,
    ListingGone,
)

__all__ = [
    "process_missing_metadata",
    "process_missing_ai_metadata",
    "check_alive",
    "haversine",
    "ROOT",
]

# Warsaw center
ROOT_DICT = {
    "Warsaw": (52.23182630705096, 21.00591455254282),
    "Krakow": (50.06196857618123, 19.938187263875268),
}
ROOT = ROOT_DICT[CITY]

# Radius of the Earth in kilometers
R = 6371.0


def haversine(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Distance in kilometers
    distance = R * c
    return distance


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
    listing_id: int, html_content: str | None
) -> ListingAdditionalInfo | ListingGone:
    if html_content is None:
        res = ListingGone(listing_id=listing_id)
        return res
    soup = BeautifulSoup(html_content, "html.parser")
    script = soup.find_all("script")[-1].text
    info = json.loads(script)
    if info.get("page") == "/pl/wyniki/[[...searchingCriteria]]":
        res = ListingGone(listing_id=listing_id)
        return res
    ad_info = info["props"]["pageProps"]["ad"]

    tg = ad_info["target"]

    floor_info = tg.get("Floor_no")
    if not floor_info:
        floor = None
    else:
        floor_info = floor_info[0]
        if floor_info == "ground_floor":
            floor = 0
        elif floor_info == "cellar":
            floor = -1
        else:
            floor = int(floor_info.split("_")[-1])
    floor_total = tg.get("Building_floors_num")

    extras = tg.get("Extras_types") or []

    window_info = tg.get("Windows_type") or []
    windows = None if not window_info else window_info[0]

    soup_inner = BeautifulSoup(ad_info["description"], "html.parser")
    description_long = soup_inner.get_text(separator=" ", strip=True)

    top_info = ad_info.get("topInformation", [])
    available_from_li = [x for x in top_info if x.get("label") == "free_from"]
    if available_from_li:
        available_from_info = available_from_li[0].get("values", [None])
        if available_from_info:
            available_from = available_from_info[0]
        else:
            available_from = None
    else:
        available_from = None

    lat = ad_info["location"]["coordinates"]["latitude"]
    lon = ad_info["location"]["coordinates"]["longitude"]

    dist = haversine(*ROOT, lat, lon)

    metadata = ListingAdditionalInfo(
        listing_id=ad_info["id"],
        description_long=description_long,
        deposit=tg.get("Deposit"),
        floor=floor,
        floors_total=floor_total,
        has_ac="air_conditioning" in extras,
        has_lift="lift" in extras,
        windows=windows,
        latitude=str(lat),
        longitude=str(lon),
        available_from=available_from,
        raw_info=json.dumps(ad_info),
        distance_from_center_km=dist,
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
        model="gemini-2.0-flash",
        contents=message,
        config={
            "response_mime_type": "application/json",
            "response_schema": ListingAIMetadata,
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
    ai_info = ListingAIInfo.from_ai_metadata(inst, listing_id=listing_id)
    return ai_info


def get_slugs(cursor) -> list[tuple[int, str]]:
    query = """
    select listing_id, min(url) as url 
    from listing_info_full
    where 1=1
    and not scraped
    and not irrelevant
    group by listing_id 
    order by listing_id 
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return results


def get_slugs_no_ai(cursor) -> list[tuple[int, str, str]]:
    query = """
    with urls as (
        select listing_id, min(url) as url
        from listing_info_full
        where 1=1
        and scraped
        and not irrelevant
        and parsed_on is null
        group by listing_id 
        order by listing_id 
    )
    select 
        urls.*,
        listing_metadata.raw_info
    from urls
    inner join listing_metadata
    on (urls.listing_id = listing_metadata.listing_id)
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return results


def get_slugs_alive(cursor) -> list[tuple[int, str]]:
    query = """
    select listing_id, min(url) as url 
    from listing_info_full
    where 1=1
    and not irrelevant
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


def process_missing_metadata(cursor, conn, ai_client) -> list[tuple[int, str]]:
    urls = get_slugs(cursor)
    for listing_id, url in tqdm.tqdm(urls):
        body = get_html_url(url)
        metadata = extract_info(listing_id, body)
        metadata.to_db(cursor)
        conn.commit()
        if not isinstance(metadata, ListingGone):
            ai_info = extract_ai_info(listing_id, metadata.raw_info, ai_client)
            ai_info.to_db(conn.cursor())
            conn.commit()
        time.sleep(random.randint(0, 1000) / 1000)

    return urls


def process_missing_ai_metadata(cursor, conn, ai_client) -> list[tuple[int, str]]:
    urls = get_slugs_no_ai(cursor)
    for listing_id, url, raw_info in tqdm.tqdm(urls):
        ai_info = extract_ai_info(listing_id, raw_info, ai_client)
        ai_info.to_db(conn.cursor())
        conn.commit()
        time.sleep(random.randint(0, 1000) / 1000)
    urls_fixed = [x[:2] for x in urls]
    return urls_fixed


def check_alive(cursor, conn) -> tuple[list[int], list[int]]:
    urls = get_slugs_alive(cursor)
    alive, dead = [], []
    for listing_id, url in tqdm.tqdm(urls):
        body = get_html_url(url)
        if body is None:
            metadata = ListingGone(listing_id=listing_id)
            metadata.to_db(cursor)
            conn.commit()
            dead.append(listing_id)
        else:
            alive.append(listing_id)
        time.sleep(random.randint(0, 3000) / 1000)
    return alive, dead
