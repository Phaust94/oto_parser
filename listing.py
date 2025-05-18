from __future__ import annotations

import datetime
import os
import typing

import pydantic
import mysql.connector
import mysql.connector.cursor
from dotenv import load_dotenv
import requests
import ua_generator
from google import genai

__all__ = [
    "ListingItem",
    "get_db_connection",
    "get_db_credentials",
    "ListingAdditionalInfo",
    "ListingAIInfo",
    "ListingAIMetadata",
    "ListingGone",
    "query_url_as_human",
    "get_ai_client",
]

load_dotenv()

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
    district_info = (((item.get('location') or {}).get('reverseGeocoding') or {}).get('locations') or [])
    if not district_info:
        return (None, None)
    fine = district_info[-1].get('fullName', '').split(',')[0]
    coarse = district_info[-2].get('fullName', '').split(',')[0]
    return fine, coarse


class Saveable(pydantic.BaseModel):

    TABLE_NAME: typing.ClassVar[str]

    def to_db(self, cursor: mysql.connector.cursor.MySQLCursor) -> bool:
        try:
            item_data = self.model_dump(exclude_none=True) # Pydantic v2+
        except AttributeError:
            item_data = self.dict(exclude_none=True) # Pydantic v1

        # Prepare column names and values for the INSERT part
        columns = ', '.join(item_data.keys())
        # Use %s as placeholders for values to prevent SQL injection
        placeholders = ', '.join(['%s'] * len(item_data))
        values = list(item_data.values())

        # Prepare the ON DUPLICATE KEY UPDATE part
        # We update all fields except the primary key (listing_id)
        upd_fields_list = [f"{col} = VALUES({col})" for col in item_data.keys() if col != 'listing_id']
        if upd_fields_list:
            update_fields = ', '.join(upd_fields_list)
            update_stmt = f"""ON DUPLICATE KEY UPDATE {update_fields}"""
        else:
            update_stmt = ''

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
            cursor.execute(sql,  {'listing_id': self.listing_id} )
            res = cursor.fetchall()
            return bool(res)
        except mysql.connector.Error as err:
            print(f"Error during upsert for {self}: {err}")
            return False
        else:
            return False

    def to_db_patch(self, cursor: mysql.connector.cursor.MySQLCursor) -> bool:
        try:
            item_data = self.model_dump(exclude_none=True) # Pydantic v2+
        except AttributeError:
            item_data = self.dict(exclude_none=True) # Pydantic v1

        # Prepare the ON DUPLICATE KEY UPDATE part
        # We update all fields except the primary key (listing_id)
        upd_fields_list = [f"{col} = %({col})s" for col in item_data.keys() if col != 'listing_id']
        update_fields = ', '.join(upd_fields_list)
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


class ListingItem(Saveable):
    listing_id: int
    title: str
    slug: str
    
    rent_price: float | None = pydantic.Field(default=None)
    administrative_price: float | None = pydantic.Field(default=None)

    area_m2: float | None = pydantic.Field(default=None)
    n_rooms: int | None = pydantic.Field(default=None)

    street: str | None = pydantic.Field(default=None)
    street_number: str | None = pydantic.Field(default=None)

    district: str | None = pydantic.Field(default=None)
    district_specific: str | None = pydantic.Field(default=None)

    created_on: datetime.datetime | None = pydantic.Field(default=None)

    TABLE_NAME: typing.ClassVar[str] = 'listing_items'

    @classmethod
    def from_otodom_data(cls, item: dict) -> ListingItem:
        street_info = (((item.get('location') or {}).get('address') or {}).get('street') or {})
        district, district_specific = get_district_info(item)
        created_on = item.get('dateCreatedFirst')
        if created_on:
            created_on = datetime.datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S")

        data = dict(
            listing_id=item['id'],
            title=item['title'],
            slug=item['slug'],
            rent_price=(item.get('totalPrice') or {}).get('value'),
            administrative_price=(item.get('rentPrice') or {}).get('value'),
            area_m2=item.get('areaInSquareMeters'),
            n_rooms=get_rooms_number(item.get('roomsNumber')),
            street=street_info.get('name'),
            street_number=street_info.get('number'),
            district=district,
            district_specific=district_specific,
            created_on=created_on,
        )
        inst = cls(**data)
        return inst


class ListingAdditionalInfo(Saveable):
    listing_id: int
    floor: int | None
    floors_total: int | None
    deposit: int | None
    has_ac: bool
    has_lift: bool
    windows: str | None

    latitude: str
    longitude: str

    description_long: str

    raw_info: str

    TABLE_NAME: typing.ClassVar[str] = 'listing_metadata'


class ListingAIMetadata(pydantic.BaseModel):
    allowed_with_pets: bool | None
    availability_date: str | None
    bedroom_number: int | None
    kitchen_combined_with_living_room: bool | None
    occasional_lease: bool | None


class ListingAIInfo(ListingAIMetadata, Saveable):
    listing_id: int

    updated_at: datetime.datetime | None

    TABLE_NAME: typing.ClassVar[str] = 'listing_ai_metadata'

    @classmethod
    def from_ai_metadata(cls, ai_metadata: ListingAIMetadata, listing_id: int) -> ListingAIInfo:
        inst = cls(
            listing_id=listing_id, **ai_metadata.model_dump(mode='python'),
            updated_at=datetime.datetime.utcnow(),
        )
        return inst



class ListingGone(Saveable):
    listing_id: int

    TABLE_NAME: typing.ClassVar[str] = 'irrelevant_listings'

def get_db_connection(host, port, database, user, password):
    """Establishes a connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None


def get_db_credentials():
    creds = (
        os.environ.get("DB_HOST"),
        os.environ.get("DB_PORT"),
        os.environ.get("DB_DATABASE"),
        os.environ.get("DB_USER"),
        os.environ.get("DB_PASSWORD"),
    )
    return creds


def get_random_user_agent() -> str:
    return ua_generator.generate().text


def query_url_as_human(url):
    """
    Queries a URL using a fake user agent to mimic a human user.

    Args:
        url (str): The URL to query.

    Returns:
        requests.Response or None: The response object if the request was successful,
                                   None otherwise.
    """
    # Define headers to mimic a browser
    headers = {
        'User-Agent': get_random_user_agent(), # Use a random user agent
        'Accept': '*/*',
        # 'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        return response
    except requests.exceptions.RequestException as e:
        return None


def get_ai_client():
    client = genai.Client(api_key=os.environ['AI_PLATFORM_API_KEY'])
    return client