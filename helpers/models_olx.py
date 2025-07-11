from __future__ import annotations

import typing
import json
import re

from bs4 import BeautifulSoup
import pydantic
from geopy.geocoders import Nominatim

from helpers.connection import NOMINATIM_AGENT
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


class ListingItemOLX(ListingItem):
    TABLE_NAME: typing.ClassVar[str] = "listing_items_olx"

    @classmethod
    def from_jo_single(cls, obj: dict) -> ListingItemOLX:
        # TODO: fixme
        return ListingItemOLX()

    @classmethod
    def from_text(cls, text: str) -> list[ListingItemOLX]:
        listing_items = []
        jo = json.loads(text)
        res = jo["data"]["clientCompatibleListings"]["data"]
        for offer in res:
            if offer["external_url"] is not None:
                continue
            inst = cls.from_text(offer)
            listing_items.append(inst)

        return listing_items


class RegexpInfo(pydantic.BaseModel):
    regexp: str
    convertor_func: typing.Callable[[str], typing.Any] = pydantic.Field(
        default=lambda x: x
    )


def str_to_bool(s: str) -> bool | None:
    s = s.lower()
    if s == "tak":
        return True
    if s == "nie":
        return False
    return None


PROPERTY_TO_REGEXP = {
    "allowed_with_pets": RegexpInfo(
        regexp=r"Zwierzęta: (.*)", convertor_func=str_to_bool
    ),
    "administrative_price": RegexpInfo(
        regexp=r"Czynsz.*:([0-9 ]+) zł",
        convertor_func=lambda x: int(x.replace(" ", "")),
    ),
    "area_m2": RegexpInfo(regexp=r"Powierzchnia: ([0-9]+) m²", convertor_func=int),
    "floor": RegexpInfo(regexp=r"Poziom: ([0-9]+)", convertor_func=int),
    "n_rooms": RegexpInfo(regexp=r"Liczba pokoi: ([0-9]+)", convertor_func=int),
}


class ListingAdditionalInfoOLX(ListingAdditionalInfo):
    allowed_with_pets: bool | None = pydantic.Field(default=None)
    n_rooms: int | None = pydantic.Field(default=None)
    administrative_price: float | None = pydantic.Field(default=None)
    area_m2: float | None = pydantic.Field(default=None)

    TABLE_NAME: typing.ClassVar[str] = "listing_metadata_olx"

    @staticmethod
    def parse_params(soup: BeautifulSoup) -> dict[str, str | None]:
        parameters_cont = soup.find(attrs={"data-testid": "ad-parameters-container"})
        out = {k: None for k in PROPERTY_TO_REGEXP.keys()}
        if parameters_cont is None:
            parameters = soup.find_all("span")
        else:
            parameters = parameters_cont.find_all("p")

        for param in parameters:
            for prop, regexp_info in PROPERTY_TO_REGEXP.items():
                res = re.findall(regexp_info.regexp, param.text)
                if not res:
                    continue
                res = regexp_info.convertor_func(res[0])
                out[prop] = res
                break
        return out

    @classmethod
    def from_text(
        cls, text: str, listing_id: str, city: str
    ) -> ListingAdditionalInfoOLX:
        soup = BeautifulSoup(text, "html.parser")
        scripts = soup.find_all("script")
        for elem in scripts:
            if '"@type":"Product"' not in elem.text:
                continue
            txt = str(elem.next)
            json_desc = json.loads(txt)
            break
        else:
            raise ValueError("Failed to parse JSON")

        out = cls.parse_params(soup)
        inst = cls(
            listing_id=listing_id,
            description_long=json_desc["description"],
            **out,
            raw_info=text,
        )
        return inst


class ListingAIMetadataOLX(ListingAIMetadata):
    allowed_with_pets: bool | None
    availability_date: str | None
    bedroom_number: int | None
    kitchen_combined_with_living_room: bool | None
    occasional_lease: bool | None
    deposit: int | None
    has_ac: bool | None
    has_lift: bool | None
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
    has_lift: whether there is a lift (winda) in the apartment
    street: what street the apartment is located on (przy jakiej ulicy). Put just street name, not building number. Remove any prefixes like ul. or similar
    street_number: what is the number of the building on the street that the apartment is situated on. Put a single number, without address
    """


CITY_TO_LOCAL_NAME = {
    "Warsaw": "Warszawa",
    "Krakow": "Kraków",
}


class ListingAIInfoOLX(ListingAIInfo, ListingAIMetadataOLX, Saveable):
    TABLE_NAME: typing.ClassVar[str] = "listing_ai_metadata_olx"

    latitude: str | None = pydantic.Field(default=None)
    longitude: str | None = pydantic.Field(default=None)
    distance_from_center_km: float | None = pydantic.Field(default=None)

    @property
    def locator(self) -> Nominatim:
        geolocator = Nominatim(user_agent=NOMINATIM_AGENT)
        return geolocator

    def get_lat_lon(self, city: str) -> None:
        if self.street is None:
            return None
        city_localized = CITY_TO_LOCAL_NAME[city]
        address = f"{self.street} {self.street_number or 1}, {city_localized}, Poland"

        # Use geopy to geocode the address
        location = self.locator.geocode(address)
        if location is None:
            return None
        self.latitude = str(location.latitude)
        self.longitude = str(location.longitude)
        return None

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def augment(self, city: str) -> None:
        self.get_lat_lon(city)
        lat, lon = float(self.latitude), float(self.longitude)
        dist = dist_from_root(city, lat, lon)
        self.distance_from_center_km = dist
        return None


BASE_URL = "https://www.olx.pl/apigateway/graphql"

LIMIT = 40


def get_page(page_num: int) -> str:
    search_params_current = SEARCH_PARAMS.copy()
    search_params_current.append({"key": "limit", "value": str(LIMIT)})
    search_params_current.append({"key": "offset", "value": str(page_num * LIMIT)})
    params = {
        "query": OLX_QUERY,
        "variables": {
            "searchParameters": SEARCH_PARAMS,
        },
    }

    res = query_url_as_human(url=BASE_URL, method="POST", body=params)
    text = res.text
    return text
