import json

import tqdm
import typing

from helpers.connection import get_db_connection, get_db_credentials
from helpers.models_otodom import Saveable
from helpers.extractor import haversine, ROOT


class ListingLocation(Saveable):
    listing_id: int
    distance_from_center_km: float

    TABLE_NAME: typing.ClassVar[str] = "listing_metadata"


def extract_info(listing_id: int, html_content: str | None) -> ListingLocation:
    ad_info = json.loads(html_content)
    lat = ad_info["location"]["coordinates"]["latitude"]
    lon = ad_info["location"]["coordinates"]["longitude"]

    dist = haversine(*ROOT, lat, lon)
    loc = ListingLocation(
        listing_id=listing_id,
        distance_from_center_km=dist,
    )
    return loc


def get_infos(limit: int, cursor) -> list[tuple[int, str]]:
    query = f"""
    select listing_id, raw_info
    from listing_metadata
        where 1=1
        and distance_from_center_km = 0
    limit {limit}
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return results


def main():

    creds = get_db_credentials()
    conn = get_db_connection(*creds)
    cursor = conn.cursor()

    while True:
        urls = get_infos(limit=100, cursor=cursor)
        if not urls:
            break
        for listing_id, body in tqdm.tqdm(urls):
            metadata = extract_info(listing_id, body)
            metadata.to_db_patch(cursor)
        conn.commit()

    conn.close()
    return None


if __name__ == "__main__":
    main()
