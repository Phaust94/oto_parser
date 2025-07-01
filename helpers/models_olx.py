from __future__ import annotations

import datetime
import typing
import json

from bs4 import BeautifulSoup
import pydantic

from helpers.models_base import Saveable, ListingItem

__all__ = [
    "Saveable",
    "ListingItemOLX",
    "ListingAdditionalInfo",
    "ListingAIMetadata",
    "ListingAIInfo",
    "ListingGone",
]


class ListingItemOLX(ListingItem):
    TABLE_NAME: typing.ClassVar[str] = "listing_items_olx"

    @classmethod
    def parse_site_data(cls, offer: dict) -> ListingItemOLX:
        offer_id = "-".join(offer["url"].split("-")[-2:]).split(".")[0]
        slug = offer["url"].split("/")[-1].split(".")[0]
        title = offer["name"]
        rent_price = offer["price"]
        district = offer.get("areaServed", {}).get("name")
        inst = cls(
            listing_id=offer_id,
            title=title,
            slug=slug,
            rent_price=rent_price,
            district=district,
            district_specific=district,
        )
        return inst

    @classmethod
    def from_text(cls, text: str) -> list[ListingItemOLX]:
        listing_items = []
        soup = BeautifulSoup(text, "html.parser")
        ress = soup.find_all("script")
        for i, elem in enumerate(ress):
            if '"@type":"Product"' not in elem.text:
                continue
            txt = str(elem.next)
            data = json.loads(txt)
            offers = data["offers"]["offers"]
            for offer in offers:
                item = cls.parse_site_data(offer)
                listing_items.append(item)
        return listing_items


PROPERTY_TO_REGEXP = {
    "animals":
}

class ListingAdditionalInfoOLX(Saveable):
    listing_id: int
    administrative_price: float | None = pydantic.Field(default=None)
    area_m2: float | None = pydantic.Field(default=None)
    floor: int | None

    description_long: str

    raw_info: str

    TABLE_NAME: typing.ClassVar[str] = "listing_metadata_olx"

    @classmethod
    def from_text(cls, text: str, listing_id: int) -> ListingAdditionalInfoOLX:
        soup = BeautifulSoup(text)
        scripts = soup.find_all("script")
        for i, eleme in enumerate(scripts):
            if '"@type":"Product"' not in eleme.text:
                continue
            txt = str(eleme.next)
            json_desc = json.loads(txt)
            break
        else:
            raise ValueError("Failed to parse JSON")

        parameters = soup.find(
            attrs={"data-testid": "ad-parameters-container"}
        ).find_all("p")
        animals_re = r"Zwierzęta: ([a-zA-Z]+)"
        animals_allowed = None
        for param in parameters:
            res = re.findall(animals_re, param.text)
            if res:
                animals_allowed = res[0] == "Tak"
        print(animals_allowed)

        inst = cls(
            listing_id=listing_id,
            description_long=json_desc["description"],
            deposit=tg.get("Deposit"),
            floor=floor,
            floors_total=floor_total,
            has_ac="air_conditioning" in extras,
            has_lift="lift" in extras,
            windows=windows,
            latitude=str(lat),
            longitude=str(lon),
            available_from=available_from,
            raw_info=text,
            distance_from_center_km=dist,
        )
        return inst


class ListingAIMetadata(pydantic.BaseModel):
    administrative_price: float | None = pydantic.Field(default=None)
    allowed_with_pets: bool | None
    availability_date: str | None
    bedroom_number: int | None
    kitchen_combined_with_living_room: bool | None
    occasional_lease: bool | None


class ListingAIInfo(ListingAIMetadata, Saveable):
    listing_id: int

    updated_at: datetime.datetime | None

    TABLE_NAME: typing.ClassVar[str] = "listing_ai_metadata"

    @classmethod
    def from_ai_metadata(
        cls, ai_metadata: ListingAIMetadata, listing_id: int
    ) -> ListingAIInfo:
        inst = cls(
            listing_id=listing_id,
            **ai_metadata.model_dump(mode="python"),
            updated_at=datetime.datetime.utcnow(),
        )
        return inst
