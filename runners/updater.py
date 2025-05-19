from helpers.connection import (get_db_connection, get_db_credentials, get_ai_client, get_tg_info)
from helpers.extractor import process_missing_metadata
from helpers.daily_updater import update_listings
from helpers.notifier import send_updates


def main():
    db_creds = get_db_credentials()
    conn = get_db_connection(*db_creds)
    cursor = conn.cursor()
    ai_client = get_ai_client()
    tg_info = get_tg_info()

    # update_listings(cursor, conn)
    new_listing_info = process_missing_metadata(cursor, conn, ai_client)
    send_updates(new_listing_info, cursor, tg_info)

    conn.close()

    return None


if __name__ == '__main__':
    main()
