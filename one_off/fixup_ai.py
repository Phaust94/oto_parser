import time
import random

from google import genai
import google.genai.errors

import tqdm

from helpers.connection import get_db_connection, get_db_credentials, get_ai_client
from helpers.models_otodom import ListingAIMetadata, ListingAIInfo


def extract_info(listing_id: int, html_content: str, client) -> ListingAIInfo:

    message = f"""
    Based on the following description, find the following information. 
    If any of the pieces are missing  - return null for that piece.
    allowed_with_pets: if it is explicitly allowed to live with pets in the apartment
    availability_date: since when the apartment is available to be leased
    bedroom_number: the number of bedrooms or rooms that could be used as a bedroom (excluding kitchen)
    occasional_lease: whether the occasional lease agreement (najem okazjonalny) is required
    Description:
    {html_content}
    """

    ai_data = dict(
        # model='gemini-2.5-flash-preview-04-17',
        model="gemini-2.0-flash",
        contents=message,
        config={
            "response_mime_type": "application/json",
            "response_schema": ListingAIMetadata,
        },
    )

    try:
        response = client.models.generate_content(**ai_data)
    except genai.errors.ClientError as e:
        if e.status == "RESOURCE_EXHAUSTED":
            time.sleep(60)
        else:
            raise
        response = client.models.generate_content(**ai_data)
    inst = response.parsed
    ai_info = ListingAIInfo.from_ai_metadata(inst, listing_id=listing_id, city=CITY)
    return ai_info


def get_infos(cursor) -> list[tuple[int, str]]:
    query = """
    select listing_id, description_long
    from listing_metadata
    where 1=1
    and listing_id in (
        select listing_id from listing_ai_metadata
        where 1=1
        and updated_at is null
    )
    and listing_id not in (select listing_id from irrelevant_listings)
    """

    cursor.execute(query)
    results = cursor.fetchall()
    return results


def main():

    creds = get_db_credentials()
    conn = get_db_connection(*creds)
    cursor = conn.cursor()
    client = get_ai_client()

    urls = get_infos(cursor)
    for listing_id, body in tqdm.tqdm(
        urls,
    ):
        metadata = extract_info(listing_id, body, client)
        metadata.to_db_patch(cursor)
        time.sleep(random.randint(0, 1000) / 1000)
        conn.commit()

    conn.close()
    return None


if __name__ == "__main__":
    main()
