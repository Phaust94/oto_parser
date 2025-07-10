from helpers.connection import get_db_connection, get_db_credentials, get_tg_info
from helpers.extractor import check_alive
from helpers.notifier import send_status_update_alive
from helpers.services import Service


def main():
    db_creds = get_db_credentials()
    conn = get_db_connection(*db_creds)
    cursor = conn.cursor()
    tg_info = get_tg_info()

    for service in Service:
        alive, dead = check_alive(cursor, conn, service)
        send_status_update_alive(alive, dead, tg_info, service=service)

    conn.close()

    return None


if __name__ == "__main__":
    main()
