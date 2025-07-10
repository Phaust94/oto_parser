from __future__ import annotations

import datetime
import typing
import json

from bs4 import BeautifulSoup
import pydantic

from helpers.models_base import (
    Saveable,
    ListingItem,
    ListingAdditionalInfo,
    ListingAIInfo,
    ListingGone,
    ListingAIMetadata,
)
from helpers.helper_functions import dist_from_root

__all__ = [
    "ListingItemOtodom",
    "ListingAdditionalInfoOtodom",
    "ListingAIMetadataOtodom",
    "ListingAIInfoOtodom",
    "SEARCH_DICT",
]


class ListingItemOtodom(ListingItem):
    listing_id: int

    administrative_price: float | None = pydantic.Field(default=None)

    district_specific: str | None = pydantic.Field(default=None)

    area_m2: float | None = pydantic.Field(default=None)
    n_rooms: int | None = pydantic.Field(default=None)

    street: str | None = pydantic.Field(default=None)
    street_number: str | None = pydantic.Field(default=None)

    created_on: datetime.datetime | None = pydantic.Field(default=None)

    TABLE_NAME: typing.ClassVar[str] = "listing_items"

    @classmethod
    def from_otodom_data(cls, item: dict) -> ListingItem:
        street_info = ((item.get("location") or {}).get("address") or {}).get(
            "street"
        ) or {}
        district, district_specific = get_district_info(item)
        created_on = item.get("dateCreatedFirst")
        if created_on:
            created_on = datetime.datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S")

        data = dict(
            listing_id=item["id"],
            title=item["title"],
            slug=item["slug"],
            rent_price=(item.get("totalPrice") or {}).get("value"),
            administrative_price=(item.get("rentPrice") or {}).get("value"),
            area_m2=item.get("areaInSquareMeters"),
            n_rooms=get_rooms_number(item.get("roomsNumber")),
            street=street_info.get("name"),
            street_number=street_info.get("number"),
            district=district,
            district_specific=district_specific,
            created_on=created_on,
        )
        inst = cls(**data)
        return inst

    @classmethod
    def from_text(cls, text: str) -> list[ListingItemOtodom]:
        listing_items = []
        soup = BeautifulSoup(text, "html.parser")
        script = soup.find_all("script")[-1].text
        body = json.loads(script)
        listings = get_listings(body)
        for listing in listings[:-1]:
            inst = ListingItem.from_otodom_data(listing)
            listing_items.append(inst)
        return listing_items


class ListingAdditionalInfoOtodom(ListingAdditionalInfo):
    floors_total: int | None
    has_ac: bool
    deposit: int | None
    has_lift: bool
    windows: str | None
    available_from: str | None = pydantic.Field(default=None)

    latitude: str
    longitude: str

    distance_from_center_km: float

    TABLE_NAME: typing.ClassVar[str] = "listing_metadata"

    @classmethod
    def from_text(
        cls, text: str, listing_id: str, city: str
    ) -> ListingAdditionalInfoOtodom | ListingGone:
        from services import Service

        soup = BeautifulSoup(text, "html.parser")
        script = soup.find_all("script")[-1].text
        info = json.loads(script)
        if info.get("page") == "/pl/wyniki/[[...searchingCriteria]]":
            res = ListingGone(listing_id=listing_id, service=Service.Otodom.value)
            return res
        ad_info = info["props"]["pageProps"]["ad"]

        tg = ad_info["target"]

        floor_info = tg.get("Floor_no")
        if not floor_info:
            floor = None
        else:
            floor_info = floor_info[0]
            if floor_info == "ground_floor":
                floor = 0
            elif floor_info == "cellar":
                floor = -1
            else:
                floor = int(floor_info.split("_")[-1])
        floor_total = tg.get("Building_floors_num")

        extras = tg.get("Extras_types") or []

        window_info = tg.get("Windows_type") or []
        windows = None if not window_info else window_info[0]

        soup_inner = BeautifulSoup(ad_info["description"], "html.parser")
        description_long = soup_inner.get_text(separator=" ", strip=True)

        top_info = ad_info.get("topInformation", [])
        available_from_li = [x for x in top_info if x.get("label") == "free_from"]
        if available_from_li:
            available_from_info = available_from_li[0].get("values", [None])
            if available_from_info:
                available_from = available_from_info[0]
            else:
                available_from = None
        else:
            available_from = None

        lat = ad_info["location"]["coordinates"]["latitude"]
        lon = ad_info["location"]["coordinates"]["longitude"]

        dist = dist_from_root(city, lat, lon)

        inst = cls(
            listing_id=ad_info["id"],
            description_long=description_long,
            deposit=tg.get("Deposit"),
            floor=floor,
            floors_total=floor_total,
            has_ac="air_conditioning" in extras,
            has_lift="lift" in extras,
            windows=windows,
            latitude=str(lat),
            longitude=str(lon),
            available_from=available_from,
            raw_info=json.dumps(ad_info),
            distance_from_center_km=dist,
        )
        return inst


class ListingAIMetadataOtodom(ListingAIMetadata):
    prompt: typing.ClassVar[
        str
    ] = """
    allowed_with_pets: if it is explicitly allowed to live with pets in the apartment
    availability_date: since when the apartment is available to be leased
    bedroom_number: the number of bedrooms or rooms that could be used as a bedroom (excluding kitchen)
    kitchen_combined_with_living_room: whether the kitchen is combined with a living room (true), or is it a separate room
    occasional_lease: whether the occasional lease agreement (najem okazjonalny) is required
    """


class ListingAIInfoOtodom(ListingAIInfo, ListingAIMetadataOtodom, Saveable):
    TABLE_NAME: typing.ClassVar[str] = "listing_ai_metadata"


def get_rooms_number(txt: str | None) -> int | None:
    if txt is None:
        return None
    dict = {
        "ONE": 1,
        "TWO": 2,
        "THREE": 3,
        "FOUR": 4,
        "FIVE": 5,
        "SIX": 6,
    }
    if txt not in dict:
        return None
    return dict[txt]


def get_district_info(item: dict) -> tuple[str | None, str | None]:
    district_info = ((item.get("location") or {}).get("reverseGeocoding") or {}).get(
        "locations"
    ) or []
    if not district_info:
        return (None, None)
    fine = district_info[-1].get("fullName", "").split(",")[0]
    coarse = district_info[-2].get("fullName", "").split(",")[0]
    return fine, coarse


def get_listings(body: dict) -> list:
    res = body["props"]["pageProps"]["data"]["searchAds"]["items"]
    return res


SEARCH_DICT = {
    "Warsaw": "https://www.otodom.pl/pl/wyniki/wynajem/mieszkanie/mazowieckie/warszawa/warszawa/warszawa?roomsNumber=%5BTHREE%2CFOUR%2CFIVE%2CSIX_OR_MORE%5D&extras=%5BGARAGE%5D&heating=%5BURBAN%5D&by=LATEST&direction=DESC&viewType=listing&page=2",
    "Krakow": "https://www.otodom.pl/pl/wyniki/wynajem/mieszkanie/malopolskie/krakow/krakow/krakow?heating=%5BURBAN%5D&by=LATEST&direction=DESC&viewType=listing&page=2&priceMax=2500",
}
