from helpers.connection import (
    get_db_connection,
    get_db_credentials,
    get_ai_client,
    get_tg_info,
)
from helpers.extractor import process_missing_metadata, process_missing_ai_metadata
from helpers.daily_updater import update_listings
from helpers.notifier import send_updates, send_status_update


def get_slugs(cursor) -> list[tuple[int, str, str]]:
    query = """
    with urls as (
        select listing_id, url
        from listing_info_full
        where parsed_on is null
        and not irrelevant
    )
    select 
        urls.*
    from urls
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return results


def process_missing(cursor, conn, ai_client) -> list[tuple[int, str]]:
    urls = get_slugs(cursor)
    return urls


def main():
    from runners.updater import main as updater_main

    updater_main(update_listings_switch=False, metadata_update_only_ai_switch=True)

    return None


if __name__ == "__main__":
    main()
