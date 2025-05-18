import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import random
import tqdm

from listing import ListingItem, get_db_connection, get_db_credentials, query_url_as_human

# The URL to parse and update
SEARCH_URL = "https://www.otodom.pl/_next/data/R35u987w9OC-lGee4vx0V/pl/wyniki/wynajem/mieszkanie/mazowieckie/warszawa/warszawa/warszawa.json?limit=36&priceMin=3500&priceMax=7000&roomsNumber=[THREE,FOUR,FIVE,SIX_OR_MORE]&heating=[URBAN]&extras=[GARAGE]&by=LATEST&direction=DESC&viewType=listing&searchingCriteria=wynajem&searchingCriteria=mieszkanie&searchingCriteria=mazowieckie&searchingCriteria=warszawa&searchingCriteria=warszawa&searchingCriteria=warszawa&page=4"
# SEARCH_URL = "https://www.otodom.pl/_next/data/R35u987w9OC-lGee4vx0V/pl/wyniki/wynajem/mieszkanie/mazowieckie/warszawa/warszawa/warszawa.json?limit=36&priceMin=3500&priceMax=7000&roomsNumber=[THREE,FOUR,FIVE,SIX_OR_MORE]&heating=[URBAN]&extras=[GARAGE]&by=LATEST&direction=DESC&viewType=listing&searchingCriteria=wynajem&searchingCriteria=mieszkanie&searchingCriteria=mazowieckie&searchingCriteria=warszawa&searchingCriteria=warszawa&searchingCriteria=warszawa&page=4"


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

PAGES = 17


def get_listings(body: dict) -> list:
    res = body["pageProps"]['data']['searchAds']['items']
    return res

def scrape_page(search_url: str, page_num: int) -> list[ListingItem]:
    listing_items = []
    updated_url = update_and_reconstruct_url(search_url, 'page', str(page_num + 1))
    res = query_url_as_human(updated_url)
    body = res.json()
    listings = get_listings(body)
    for listing in listings:
        inst = ListingItem.from_otodom_data(listing)
        listing_items.append(inst)
    return listing_items


def scrape(search_url: str, pages: int) -> list[ListingItem]:
    listing_items = []
    for i in range(pages):
        li_chunk = scrape_page(search_url, i)
        listing_items.extend(li_chunk)
    return listing_items


def save_to_db(data: list[ListingItem]) -> None:
    db_creds = get_db_credentials()
    conn = get_db_connection(*db_creds)
    cursor = conn.cursor()
    for item in data:
        item.to_db(cursor=cursor)
    conn.commit()
    conn.close()
    return None


def main():
    for i in tqdm.tqdm(range(PAGES)):
        li_chunk = scrape_page(SEARCH_URL, i)
        save_to_db(li_chunk)
        time.sleep(10 + random.randint(1, 1000)/1000)
    return None


if __name__ == '__main__':
    main()
