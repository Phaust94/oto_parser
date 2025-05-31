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
        select listing_id, min(url) as url
    from listing_info_full
    where 1=1
    and scraped
    and not irrelevant
    and parsed_on >= '2025-05-25'
    group by listing_id 
    order by listing_id 
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
    db_creds = get_db_credentials()
    conn = get_db_connection(*db_creds)
    cursor = conn.cursor()
    ai_client = get_ai_client()
    tg_info = get_tg_info()

    new_listing_info = process_missing(cursor, conn, ai_client)
    send_updates(new_listing_info, cursor, tg_info)
    send_status_update(new_listing_info, tg_info)

    conn.close()

    return None


if __name__ == "__main__":
    main()
