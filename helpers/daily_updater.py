import time
import random
import sys

import tqdm

from helpers.connection import CITY
from helpers.models_base import ListingItem
from helpers.services import Service

__all__ = ["update_listings"]


PAGES = 36


def scrape_page(page_num: int, service: Service) -> list[ListingItem]:
    text = service.get_page_function(page_num)
    listing_items = service.listing_item_model_class.from_text(text)
    return listing_items


def save_to_db(cursor, data: list[ListingItem], conn) -> bool:
    present = {}
    for item in data:
        is_present = item.is_present_in_db(cursor)
        present[item.listing_id] = is_present
        if not is_present:
            item.to_db(cursor=cursor)
    conn.commit()
    return all(present.values())


def update_listings(cursor, conn, service: Service) -> bool:
    all_present = False
    for i in tqdm.tqdm(range(1, PAGES), file=sys.stdout):
        li_chunk = scrape_page(service.search_url_dict[CITY], i, service)
        all_present = save_to_db(cursor, li_chunk, conn)
        if all_present:
            break
        time.sleep(10 + random.randint(1, 1000) / 1000)
    return all_present
