from listing import (get_db_connection, get_db_credentials, get_ai_client)
from helpers.extractor import process_missing_metadata
from helpers.daily_updater import update_listings


def main():
    db_creds = get_db_credentials()
    conn = get_db_connection(*db_creds)
    cursor = conn.cursor()
    ai_client = get_ai_client()
    update_listings(cursor, conn)
    process_missing_metadata(cursor, conn, ai_client)

    conn.close()

    return None


if __name__ == '__main__':
    main()
