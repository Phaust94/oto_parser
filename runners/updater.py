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


def main_single_service(
    update_listings_switch,
    metadata_update_only_ai_switch,
    conn,
    cursor,
    ai_client,
    tg_info,
    service: Service,
) -> None:
    if update_listings_switch:
        update_listings(cursor, conn, service)

    md_func = (
        process_missing_ai_metadata
        if metadata_update_only_ai_switch or not service.ad_parsing_needed
        else process_missing_metadata
    )
    new_listing_info = md_func(cursor, conn, ai_client, service)
    send_updates(new_listing_info, cursor, tg_info)
    send_status_update(new_listing_info, tg_info, service)
    return None


def main(
    update_listings_switch: bool = True,
    metadata_update_only_ai_switch: bool = False,
    services_to_update: typing.Iterable[Service] = None,
):
    db_creds = get_db_credentials()
    conn = get_db_connection(*db_creds)
    cursor = conn.cursor()
    ai_client = get_ai_client()
    tg_info = get_tg_info()
    services_to_update = services_to_update or [Service(x) for x in CURRENT_DATASOURCES]

    for service in services_to_update:
        main_single_service(
            update_listings_switch,
            metadata_update_only_ai_switch,
            conn,
            cursor,
            ai_client,
            tg_info,
            service,
        )

    conn.close()

    return None


if __name__ == "__main__":
    main()
