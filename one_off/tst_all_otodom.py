import time
import tqdm
import sys

from helpers.services import Service
from helpers.daily_updater import scrape_page
import typing

from helpers.connection import (
    get_db_connection,
    get_db_credentials,
    get_ai_client,
    get_tg_info,
    CURRENT_DATASOURCES,
)
from helpers.extractor import process_missing_metadata, process_missing_ai_metadata
from helpers.daily_updater import update_listings
from helpers.notifier import send_updates, send_status_update
from helpers.services import Service
from helpers.models_olx import ListingItemOLX


PAGES = 20


def update_listings(cursor, conn, service: Service) -> bool:
    for i in tqdm.tqdm(range(1), file=sys.stdout):
        ListingItemOLX.TABLE_NAME = "listing_items"
        li_chunk = scrape_page(i, service)
        for elem in li_chunk:
            if not elem.is_present_in_db_slug_external(cursor):
                print(f"NOPE: https://otodom.pl/pl/oferta/{elem.slug_external}.html")
            else:
                print(f"HURRAY!: https://otodom.pl/pl/oferta/{elem.slug_external}.html")


def main():
    db_creds = get_db_credentials()
    conn = get_db_connection(*db_creds)
    cursor = conn.cursor()
    update_listings(cursor, conn, service=Service.OLX)


if __name__ == "__main__":
    main()
