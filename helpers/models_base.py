from __future__ import annotations

import abc
import datetime
import enum
import typing

import mysql.connector
import mysql.connector.cursor
import pydantic

__all__ = [
    "Saveable",
    "ListingItem",
    "ListingAdditionalInfo",
    "ListingAIMetadata",
    "ListingAIInfo",
    "ListingGone",
    "Service",
]


class Service(enum.Enum):
    Otodom = "otodom"
    OLX = "olx"


class Saveable(pydantic.BaseModel):

    TABLE_NAME: typing.ClassVar[str]

    def to_db(self, cursor: mysql.connector.cursor.MySQLCursor) -> bool:
        try:
            item_data = self.model_dump(exclude_none=True)  # Pydantic v2+
        except AttributeError:
            item_data = self.dict(exclude_none=True)  # Pydantic v1

        # Prepare column names and values for the INSERT part
        columns = ", ".join(item_data.keys())
        # Use %s as placeholders for values to prevent SQL injection
        placeholders = ", ".join(["%s"] * len(item_data))
        values = list(item_data.values())

        # Prepare the ON DUPLICATE KEY UPDATE part
        # We update all fields except the primary key (listing_id)
        upd_fields_list = [
            f"{col} = VALUES({col})" for col in item_data.keys() if col != "listing_id"
        ]
        if upd_fields_list:
            update_fields = ", ".join(upd_fields_list)
            update_stmt = f"""ON DUPLICATE KEY UPDATE {update_fields}"""
        else:
            update_stmt = ""

        # Construct the full SQL query
        sql = f"""
        INSERT INTO {self.__class__.TABLE_NAME} ({columns})
        VALUES ({placeholders})
        {update_stmt}
        """

        try:
            cursor.execute(sql, values)
            return True
        except mysql.connector.Error as err:
            print(f"Error during upsert for {self}: {err}")
            return False
        else:
            return False

    def is_present_in_db(self, cursor: mysql.connector.cursor.MySQLCursor) -> bool:

        # Construct the full SQL query
        sql = f"""
        select listing_id from {self.__class__.TABLE_NAME}
        where listing_id = %(listing_id)s
        """

        try:
            cursor.execute(sql, {"listing_id": self.listing_id})
            res = cursor.fetchall()
            return bool(res)
        except mysql.connector.Error as err:
            print(f"Error during upsert for {self}: {err}")
            return False
        else:
            return False

    def to_db_patch(self, cursor: mysql.connector.cursor.MySQLCursor) -> bool:
        try:
            item_data = self.model_dump(exclude_none=True)  # Pydantic v2+
        except AttributeError:
            item_data = self.dict(exclude_none=True)  # Pydantic v1

        # Prepare the ON DUPLICATE KEY UPDATE part
        # We update all fields except the primary key (listing_id)
        upd_fields_list = [
            f"{col} = %({col})s" for col in item_data.keys() if col != "listing_id"
        ]
        update_fields = ", ".join(upd_fields_list)
        # Construct the full SQL query
        sql = f"""
        UPDATE {self.__class__.TABLE_NAME}
        SET {update_fields}
        WHERE 1=1
        and listing_id = %(listing_id)s
        """

        try:
            cursor.execute(sql, item_data)
            return True
        except mysql.connector.Error as err:
            print(f"Error during update for {self}: {err}")
            return False
        else:
            return False


class ListingItem(Saveable, abc.ABC):
    listing_id: str
    title: str
    slug: str

    rent_price: float | None = pydantic.Field(default=None)

    district: str | None = pydantic.Field(default=None)

    TABLE_NAME: typing.ClassVar[str] = NotImplemented

    @classmethod
    def from_text(cls, text: str) -> list[ListingItem]:
        raise NotImplementedError()


class ListingAdditionalInfo(Saveable):
    listing_id: str
    floor: int | None
    description_long: str
    raw_info: str

    TABLE_NAME: typing.ClassVar[str] = NotImplemented

    @classmethod
    def from_text(cls, text: str, listing_id: str, city: str) -> ListingAdditionalInfo:
        raise NotImplementedError()

    @property
    def info_for_ai(self) -> str:
        raise NotImplementedError()


class ListingAIMetadata(pydantic.BaseModel):
    allowed_with_pets: bool | None
    availability_date: str | None
    bedroom_number: int | None
    kitchen_combined_with_living_room: bool | None
    occasional_lease: bool | None

    prompt: typing.ClassVar[str] = NotImplemented


class ListingAIInfo(ListingAIMetadata, Saveable, abc.ABC):
    listing_id: str

    updated_at: datetime.datetime | None

    TABLE_NAME: typing.ClassVar[str] = NotImplemented

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def augment(self, city: str) -> None:
        return None

    @classmethod
    def from_ai_metadata(
        cls,
        ai_metadata: ListingAIMetadata,
        listing_id: str,
        city: str,
    ) -> ListingAIInfo:
        inst = cls(
            listing_id=listing_id,
            **ai_metadata.model_dump(mode="python"),
            updated_at=datetime.datetime.utcnow(),
        )
        inst.augment(city=city)
        return inst


class ListingGone(Saveable):
    listing_id: str
    service: str

    TABLE_NAME: typing.ClassVar[str] = "irrelevant_listings"
