from typing import List, Dict, Any
import os
import requests
import asyncio
import aiohttp
from aiohttp import ClientSession

API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'
SALES_ENDPOINT = 'sales'
STATUS_CODE_200 = 200
API_AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")


def unscramble_lists(nested_list: List[Any]) -> List[Any]:
    result = []
    for item in nested_list:
        if isinstance(item, list):
            result.extend(unscramble_lists(item))
        else:
            result.append(item)
    return result


async def fetch_page_data(session: ClientSession,
                          url: str,
                          headers: Dict[str, Any],
                          params: Dict[str, Any],
                          page: int) -> List[Dict[str, Any]]:
    print(f"Page {page} started")
    response = await session.get(url=url, headers=headers, params=params)
    data = await response.json()
    print(f"Page {page} done")
    if response.status == STATUS_CODE_200:
        return data


async def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: data retrieve the data from
    :return: list of records
    """

    get_url = os.path.join(API_URL, SALES_ENDPOINT)
    headers = {'Authorization': API_AUTH_TOKEN}
    async with aiohttp.ClientSession() as session:
        tasks = []
        gathered_results = []
        results = []
        page_start = 1
        page_end = 11
        # loop checks if the previous batch of pages does not contain nulls.
        while results.count(None) == 0:
            # define start range of pages
            pages = range(page_start, page_end)
            for page in pages:
                params = {'date': date, 'page': page}
                tasks.append(fetch_page_data(session, get_url, headers, params, page))
            # convert results to list to check if it contains nulls
            results = list(await asyncio.gather(*tasks))
            # append all results to gathered_results
            gathered_results.append(results)
            # increment range of pages
            page_start += 10
            page_end += 10
        # unscramble nested lists in gathered_results, and remove nulls
        gathered_results = [x for x in unscramble_lists(gathered_results) if x is not None]
        return gathered_results