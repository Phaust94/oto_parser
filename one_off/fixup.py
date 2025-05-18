import json
import time
import random
import types

import tqdm
import typing
from bs4 import BeautifulSoup

from listing import (
    ListingAdditionalInfo, ListingAIInfo, get_db_connection, get_db_credentials,
    ListingAIMetadata, query_url_as_human, ListingGone, Saveable
)

class ListingLocation(Saveable):
    listing_id: int
    latitude: str
    longitude: str

    TABLE_NAME: typing.ClassVar[str] = 'listing_metadata'


def extract_info(listing_id: int, html_content: str | None) -> ListingLocation:
    # soup = BeautifulSoup(html_content, 'html.parser')
    # script = soup.find_all('script')[-1].text
    ad_info = json.loads(html_content)
    loc = ListingLocation(
        listing_id=listing_id,
        latitude=str(ad_info['location']['coordinates']['latitude']),
        longitude=str(ad_info['location']['coordinates']['longitude']),
    )
    return loc

def get_infos(cursor) -> list[tuple[int, str]]:
    query = """
    select listing_id, raw_info
    from otodom.listing_metadata
        where 1=1
        and latitude = ''
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return results


def main():

    creds = get_db_credentials()
    conn = get_db_connection(*creds)
    cursor = conn.cursor()

    urls = get_infos(cursor)
    for listing_id, body in tqdm.tqdm(
        urls,
    ):
        metadata = extract_info(listing_id,  body)
        metadata.to_db_patch(cursor)
    conn.commit()

    conn.close()
    return None

if __name__ == "__main__":
    main()
