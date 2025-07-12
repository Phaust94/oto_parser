from __future__ import annotations

import typing
import json

import pydantic

from helpers.models_base import (
    Saveable,
    ListingItem,
    ListingAIMetadata,
    ListingAdditionalInfo,
    ListingAIInfo,
)
from helpers.helper_functions import dist_from_root
from helpers.connection import query_url_as_human, CITY
from helpers.olx_graphql import OLX_QUERY, SEARCH_PARAMS

__all__ = [
    "Saveable",
    "ListingItemOLX",
    "ListingAdditionalInfoOLX",
    "ListingAIMetadataOLX",
    "ListingAIInfoOLX",
    "get_page",
]


def get_by_key(params: list, key: str, value_key: str) -> typing.Any:
    par = [x for x in params if x["key"] == key]
    if not par:
        return None
    param_item = par[0]
    res = param_item["value"].get(value_key)
    return res


def int_from_text(text: str | None) -> int | None:
    if text is None:
        return None
    try:
        res = int(text.split(" ")[0])
    except ValueError:
        res = None
    return res


def floor_from_text(text: str | None) -> int | None:
    if text is None:
        return None
    if text.lower() == "parter":
        return 0
    return int_from_text(text)


class ListingItemOLX(ListingItem):
    allowed_with_pets: bool | None = pydantic.Field(default=None)
    n_rooms: int | None = pydantic.Field(default=None)
    administrative_price: float | None = pydantic.Field(default=None)
    area_m2: float | None = pydantic.Field(default=None)
    floor: int | None = pydantic.Field(default=None)
    has_lift: bool | None = pydantic.Field(default=None)
    latitude: str | None = pydantic.Field(default=None)
    longitude: str | None = pydantic.Field(default=None)

    distance_from_center_km: float

    description_long: str
    raw_info: str

    TABLE_NAME: typing.ClassVar[str] = "listing_items_olx"

    @classmethod
    def from_jo_single(cls, obj: dict) -> ListingItemOLX:
        params = obj["params"]
        lat, lon = obj.get("map", {}).get("lat"), obj.get("map", {}).get("lon")
        inst = ListingItemOLX(
            listing_id=str(obj["id"]),
            title=obj["title"],
            slug=obj["url"].split("/")[-1].split(".")[0],
            rent_price=get_by_key(params, "price", "value"),
            allowed_with_pets=str_to_bool(get_by_key(params, "pets", "key")),
            n_rooms=int_from_text(get_by_key(params, "rooms", "label")),
            administrative_price=int_from_text(get_by_key(params, "rent", "key")),
            area_m2=int_from_text(get_by_key(params, "m", "key")),
            floor=floor_from_text(get_by_key(params, "floor_select", "label")),
            description_long=obj["description"],
            raw_info=obj["description"],
            district=obj["location"].get("district", {}).get("name"),
            has_lift=str_to_bool(get_by_key(params, "winda", "key")),
            latitude=str(lat),
            longitude=str(lon),
            distance_from_center_km=dist_from_root(CITY, lat, lon),
        )
        return inst

    @classmethod
    def from_text(cls, text: str) -> list[ListingItemOLX]:
        listing_items = []
        jo = json.loads(text)
        res = jo["data"]["clientCompatibleListings"].get("data", [])
        for offer in res:
            if offer["external_url"] is not None or "olx.pl" not in offer["url"]:
                continue
            inst = cls.from_jo_single(offer)
            listing_items.append(inst)

        return listing_items


class RegexpInfo(pydantic.BaseModel):
    regexp: str
    convertor_func: typing.Callable[[str], typing.Any] = pydantic.Field(
        default=lambda x: x
    )


def str_to_bool(s: str | None) -> bool | None:
    if s is None:
        return None
    s = s.lower()
    if s == "tak":
        return True
    if s == "nie":
        return False
    return None


class ListingAdditionalInfoOLX(ListingAdditionalInfo):
    TABLE_NAME: typing.ClassVar[str] = "listing_metadata_olx"


class ListingAIMetadataOLX(ListingAIMetadata):
    allowed_with_pets: bool | None
    availability_date: str | None
    bedroom_number: int | None
    kitchen_combined_with_living_room: bool | None
    occasional_lease: bool | None
    deposit: int | None
    has_ac: bool | None
    street: str | None
    street_number: str | None

    prompt: typing.ClassVar[
        str
    ] = """
    allowed_with_pets: if it is explicitly allowed to live with pets in the apartment
    availability_date: since when the apartment is available to be leased
    bedroom_number: the number of bedrooms or rooms that could be used as a bedroom (excluding kitchen)
    kitchen_combined_with_living_room: whether the kitchen is combined with a living room (true), or is it a separate room
    occasional_lease: whether the occasional lease agreement (najem okazjonalny) is required
    deposit: what is the amount of deposit (kaucja) that is required
    has_ac: whether the apartment is air-conditioned (klimatyzowane)
    street: what street the apartment is located on (przy jakiej ulicy). Put just street name, not building number. Remove any prefixes like ul. or similar
    street_number: what is the number of the building on the street that the apartment is situated on. Put a single number, without address
    """


CITY_TO_LOCAL_NAME = {
    "Warsaw": "Warszawa",
    "Krakow": "Kraków",
}


class ListingAIInfoOLX(ListingAIInfo, ListingAIMetadataOLX, Saveable):
    TABLE_NAME: typing.ClassVar[str] = "listing_ai_metadata_olx"


BASE_URL = "https://www.olx.pl/apigateway/graphql"

LIMIT = 40


def get_page(page_num: int) -> str:
    offset = page_num * LIMIT
    search_params_current = SEARCH_PARAMS.copy()
    search_params_current.append({"key": "limit", "value": str(LIMIT)})
    search_params_current.append({"key": "offset", "value": str(offset)})
    params = {
        "query": OLX_QUERY,
        "variables": {
            "searchParameters": search_params_current,
        },
    }
    if offset > 1000:
        return "{}"

    res = query_url_as_human(url=BASE_URL, method="POST", body=params)
    text = res.text
    return text
