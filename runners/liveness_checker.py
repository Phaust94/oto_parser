from helpers.connection import (get_db_connection, get_db_credentials, get_tg_info)
from helpers.extractor import check_alive
from helpers.notifier import send_status_update_alive


def main():
    db_creds = get_db_credentials()
    conn = get_db_connection(*db_creds)
    cursor = conn.cursor()
    tg_info = get_tg_info()

    alive, dead = check_alive(cursor, conn)
    send_status_update_alive(alive, dead, tg_info)

    conn.close()

    return None


if __name__ == '__main__':
    main()
