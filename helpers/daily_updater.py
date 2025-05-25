import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import random
import json

from bs4 import BeautifulSoup
import tqdm

from helpers.connection import (query_url_as_human, CITY)
from helpers.models import ListingItem

__all__ = [
    "update_listings"
]

SEARCH_DICT = {
    "Warsaw": "https://www.otodom.pl/pl/wyniki/wynajem/mieszkanie/mazowieckie/warszawa/warszawa/warszawa?roomsNumber=%5BTHREE%2CFOUR%2CFIVE%2CSIX_OR_MORE%5D&extras=%5BGARAGE%5D&heating=%5BURBAN%5D&by=LATEST&direction=DESC&viewType=listing&page=2",
    "Krakow": "https://www.otodom.pl/pl/wyniki/wynajem/mieszkanie/malopolskie/krakow/krakow/krakow?heating=%5BURBAN%5D&by=LATEST&direction=DESC&viewType=listing&page=2&priceMax=2500",
}
SEARCH_FIRST_URL = SEARCH_DICT[CITY]

def parse_url_parameters(url):
    """
    Parses a URL string and extracts its query parameters into a dictionary.

    Args:
        url (str): The URL string to parse.

    Returns:
        dict: A dictionary where keys are parameter names and values are lists
              of parameter values. Returns an empty dictionary if no query
              parameters are present.
    """
    # Parse the URL
    parsed_url = urlparse(url)

    # Extract the query string
    query_string = parsed_url.query

    # Parse the query string into a dictionary
    # parse_qs returns a dictionary where values are lists
    parameters = parse_qs(query_string)

    return parameters

def update_and_reconstruct_url(url, param_name, new_value):
    """
    Updates a specific parameter in a URL and reconstructs the URL.

    Args:
        url (str): The original URL string.
        param_name (str): The name of the parameter to update.
        new_value (str or list): The new value(s) for the parameter.

    Returns:
        str: The reconstructed URL with the updated parameter.
    """
    # Parse the original URL
    parsed_url = urlparse(url)

    # Get the existing parameters as a dictionary
    parameters = parse_qs(parsed_url.query)

    # Update the specific parameter
    # parse_qs returns lists, so we should set the new value as a list too
    if isinstance(new_value, list):
        parameters[param_name] = new_value
    else:
        parameters[param_name] = [new_value] # Ensure it's a list

    # Encode the updated parameters back into a query string
    # quote_via=quote_plus handles spaces as '+'
    updated_query_string = urlencode(parameters, doseq=True)

    # Reconstruct the URL with the updated query string
    # urlunparse takes a tuple: (scheme, netloc, path, params, query, fragment)
    reconstructed_url = urlunparse((
        parsed_url.scheme,
        parsed_url.netloc,
        parsed_url.path,
        parsed_url.params,
        updated_query_string,
        parsed_url.fragment
    ))

    return reconstructed_url

PAGES = 36


def get_listings(body: dict) -> list:
    res = body['props']["pageProps"]['data']['searchAds']['items']
    return res

def scrape_page(search_url: str, page_num: int) -> list[ListingItem]:
    listing_items = []
    updated_url = update_and_reconstruct_url(search_url, 'page', str(page_num + 1))
    res = query_url_as_human(updated_url)
    soup = BeautifulSoup(res.text, 'html.parser')
    script = soup.find_all('script')[-1].text
    body = json.loads(script)
    listings = get_listings(body)
    for listing in listings[:-1]:
        inst = ListingItem.from_otodom_data(listing)
        listing_items.append(inst)
    return listing_items

def scrape(search_url: str, pages: int) -> list[ListingItem]:
    listing_items = []
    for i in range(pages):
        li_chunk = scrape_page(search_url, i)
        listing_items.extend(li_chunk)
    return listing_items


def save_to_db(cursor, data: list[ListingItem], conn) -> bool:

    present = {}
    for item in data:
        is_present = item.is_present_in_db(cursor)
        present[item.listing_id] = is_present
        if not is_present:
            item.to_db(cursor=cursor)
    conn.commit()
    return all(present.values())


def update_listings(cursor, conn) -> bool:
    all_present = False
    for i in tqdm.tqdm(range(PAGES)):
        li_chunk = scrape_page(SEARCH_FIRST_URL, i)
        all_present = save_to_db(cursor, li_chunk, conn)
        if all_present:
            break
        time.sleep(10 + random.randint(1, 1000)/1000)
    return all_present