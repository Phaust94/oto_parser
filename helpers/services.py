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

    def search_url_dict(self, city: str) -> str:
        di = {
            self.Otodom: mod.SEARCH_DICT,
            self.OLX: mox.SEARCH_DICT,
        }
        return di[self]
