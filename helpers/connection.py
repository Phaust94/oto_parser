from __future__ import annotations

import os

import mysql.connector
import mysql.connector.cursor
from dotenv import load_dotenv
import requests
import ua_generator
from google import genai

__all__ = [
    "get_db_connection",
    "get_db_credentials",
    "query_url_as_human",
    "get_ai_client",
]

load_dotenv()


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


def get_tg_info():
    res = dict(
        bot_token=os.environ["TG_BOT_TOKEN"],
        chat_id=os.environ["TG_CHAT_ID"],
        update_thread=os.environ["TG_UPDATES_THREAD_ID"]
    )
    return res