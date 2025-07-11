from __future__ import annotations

import abc
import enum
import typing


import helpers.models_base as mb
import helpers.models_otodom as mod
import helpers.models_olx as mox

__all__ = [
    "Service",
]


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
    def search_url_dict(self) -> str:
        di = {
            self.Otodom: mod.SEARCH_DICT,
            self.OLX: mox.SEARCH_DICT,
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
    def info_for_ai(self) -> str:
        di = {
            self.Otodom: "raw_info",
            self.OLX: "description_long",
        }
        return di[self]
