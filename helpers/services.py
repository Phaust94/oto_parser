from __future__ import annotations

import enum
import typing

import pydantic

import helpers.models_base as mb
import helpers.models_otodom as mod
import helpers.models_olx as mox

__all__ = [
    "Service",
]


class AIQueryInfo(pydantic.BaseModel):
    select_columns: str
    select_top_level_additional: str
    joins_additional: str


class Service(enum.Enum):
    Otodom = "otodom"
    OLX = "olx"

    @property
    def listing_item_model_class(self) -> typing.Type[mb.ListingItem]:
        di = {
            self.Otodom: mod.ListingItemOtodom,
            self.OLX: mox.ListingItemOLX,
        }
        return di[self]

    @property
    def listing_metadata_model_class(self) -> typing.Type[mb.ListingAdditionalInfo]:
        di = {
            self.Otodom: mod.ListingAdditionalInfoOtodom,
            self.OLX: mox.ListingAdditionalInfoOLX,
        }
        return di[self]

    @property
    def get_page_function(self) -> typing.Callable[[int], str]:
        di = {
            self.Otodom: mod.get_page,
            self.OLX: mox.get_page,
        }
        return di[self]

    @property
    def listing_ai_metadata_model_class(self) -> typing.Type[mb.ListingAIInfo]:
        di = {
            self.Otodom: mod.ListingAIInfoOtodom,
            self.OLX: mox.ListingAIInfoOLX,
        }
        return di[self]

    @property
    def listing_ai_metadata_schema_class(self) -> typing.Type[mb.ListingAIMetadata]:
        di = {
            self.Otodom: mod.ListingAIMetadataOtodom,
            self.OLX: mox.ListingAIMetadataOLX,
        }
        return di[self]

    @property
    def info_for_ai(self) -> AIQueryInfo:
        di = {
            self.Otodom: AIQueryInfo(
                select_columns="listing_id, min(url) as url",
                select_top_level_additional=", listing_metadata.raw_info",
                joins_additional="""inner join listing_metadata
                on (urls.listing_id = listing_metadata.listing_id)""",
            ),
            self.OLX: AIQueryInfo(
                select_columns="listing_id, min(url) as url, max(description_long) as description_long",
                select_top_level_additional="",
                joins_additional="",
            ),
        }
        return di[self]

    @property
    def ad_parsing_needed(self) -> bool:
        di = {
            self.Otodom: True,
            self.OLX: False,
        }
        return di[self]
