import textwrap

import requests

from helpers.connection import CITY

__all__ = [
    "send_updates",
    "send_status_update",
    "send_status_update_alive",
]

COLUMN_NAMES = [
    "listing_id",
    "title",
    "slug",
    "rent_price",
    "administrative_price",
    "total_rent_price",
    "area_m2",
    "n_rooms",
    "street",
    "street_number",
    "district",
    "district_specific",
    "created_on",
    "scraped",
    "floor",
    "floors_total",
    "deposit",
    "has_ac",
    "has_lift",
    "windows",
    "latitude",
    "longitude",
    "description_long",
    "allowed_with_pets",
    "availability_date",
    "bedroom_number",
    "occasional_lease",
    "kitchen_combined_with_living_room",
    "irrelevant",
    "our_decision",
    "url",
    "distance_from_center_km",
]

CONDITIONS_DI = {
    (
        "Warsaw",
        True,
    ): """AND (n_rooms > 3 or (kitchen_combined_with_living_room is null) or (not kitchen_combined_with_living_room))
        and total_rent_price < 7500
        and distance_from_center_km < 4.5
        and (allowed_with_pets is null or allowed_with_pets)""",
    (
        "Krakow",
        True,
    ): """AND total_rent_price <= 2100
        AND (has_lift OR floor <= 1)""",
    (
        "Warsaw",
        False,
    ): """AND (n_rooms > 3 or (kitchen_combined_with_living_room is null) or (not kitchen_combined_with_living_room))
        and total_rent_price < 7500
        and distance_from_center_km >= 4.5
        and (allowed_with_pets is null or allowed_with_pets)""",
}


def get_to_notify(
    new_listing_ids: list[int], cursor, include_distance: True
) -> list[dict]:
    placeholders = ",".join("%s" for _ in range(len(new_listing_ids)))
    column_names_list = ",\n".join(COLUMN_NAMES)

    condition = CONDITIONS_DI.get((CITY, include_distance))
    if not condition:
        return []
    sql = f"""
    select 
        {column_names_list}
    from listing_info_full
    where 1=1
    and listing_id in ({placeholders})
    and scraped
    and not irrelevant
    {condition}
    and our_decision is null
    """
    cursor.execute(sql, new_listing_ids)
    results = cursor.fetchall()
    new_rooms_dicts = [dict(zip(COLUMN_NAMES, x)) for x in results]
    return new_rooms_dicts


def send_telegram_message(bot_token, chat_id, message, thread: str = None, **_):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",  # Optional: allows HTML formatting
    }
    if thread:
        payload["message_thread_id"] = thread
    response = requests.post(url, data=payload)
    return response.json()


CENTER_DICT = {
    "Warsaw": "Pa%C5%82ac+Kultury+i+Nauki,+Pa%C5%82ac+Kultury+i+Nauki,+plac+Defilad,+Warszawa",
    "Krakow": "Rynek+Główny,+31-422+Kraków",
}
DASHBOARD_DICT = {
    "Warsaw": "2-warsaw",
    "Krakow": "4-krakow",
}


def format_msg(di: dict) -> str:
    res = """<a href="{url}">A new apt</a> just dropped, and it seems to be 🔥:
    Name: {title}
    Price: {total_rent_price}
    Area: {area_m2}
    Rooms: {n_rooms}
    District: {district_specific}
    Occasional lease: {occasional_lease}
    Availability date: {availability_date}
    Distance: {distance_from_center_km}
    Location: <a href="https://www.google.com/maps/dir/{center}/{latitude},{longitude}">Maps</a>
    Metabase link: <a href="https://metabase.home.arpa/dashboard/{dash}?bedrooms=&decision=&listing_id={listing_id}&not_okazjonalny=&not_pets=&distance_from_center_max=&price_max=&not_separate_kitchen=&rooms=&undecided%253F=">Metabase</a>
    """.format(
        center=CENTER_DICT[CITY], dash=DASHBOARD_DICT[CITY], **di
    )
    return res


def send_updates(info: list[tuple[int, str]], cursor, tg_info) -> None:
    if not info:
        return None

    ids = [x[0] for x in info]

    for include_distance in (True, False):
        thread = (
            tg_info["update_thread"]
            if include_distance
            else tg_info["update_no_distance_thread"]
        )
        to_notify_dicts = get_to_notify(ids, cursor, include_distance=include_distance)
        messages = [format_msg(di) for di in to_notify_dicts]
        for msg in messages:
            send_telegram_message(**tg_info, message=msg, thread=thread)

    return None


def format_status_msg(info: list[tuple[int, str]]) -> str:
    res = textwrap.dedent(
        """\
    Done updating from Otodom for {city}!
    Parsed ads: {n_ads}\
    """
    ).format(n_ads=len(info), city=CITY)
    return res


def send_status_update(info: list[tuple[int, str]], tg_info) -> None:
    msg = format_status_msg(info)
    send_telegram_message(
        **tg_info, message=msg, thread=tg_info["update_status_thread"]
    )
    return None


def format_status_msg_alive(alive: list[int], dead: list[int]) -> str:
    res = textwrap.dedent(
        """\
    Done live-checking from Otodom for {city}!
    Still alive ads: {n_alive}
    Dead ads: {n_dead}.\
    """.format(
            n_alive=len(alive), n_dead=len(dead), city=CITY
        )
    )
    return res


def send_status_update_alive(alive: list[int], dead: list[int], tg_info) -> None:
    msg = format_status_msg_alive(alive, dead)
    send_telegram_message(
        **tg_info, message=msg, thread=tg_info["update_status_thread"]
    )
    return None
