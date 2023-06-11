import unittest
from unittest.mock import Mock, patch
import asyncio
import os

from lesson_02.ht_template.job1.dal.sales_api import unscramble_lists, get_sales, fetch_page_data

API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'
SALES_ENDPOINT = 'sales'
STATUS_CODE_200 = 200
API_AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")

class TestSalesApi(unittest.IsolatedAsyncioTestCase):

    def test_unscramble_lists(self):
        nested_list = [1, [2, [3, 4], 5], 6]
        expected_result = [1, 2, 3, 4, 5, 6]

        result = unscramble_lists(nested_list)

        self.assertEqual(result, expected_result)

    def test_fetch_page_data(self):
        url = API_URL
        headers = {"Authorization": API_AUTH_TOKEN}
        params = {"date": "2023-06-07", "page": 1}
        page_data = [
            {'client': 'Michael Wilkerson', 'purchase_date': '2022-08-09', 'product': 'Vacuum cleaner', 'price': 346},
            {'client': 'Russell Hill', 'purchase_date': '2022-08-09', 'product': 'Microwave oven', 'price': 446},
            {'client': 'Michael Galloway', 'purchase_date': '2022-08-09', 'product': 'Phone', 'price': 1042}
        ]

        response_mock = Mock()
        response_mock.status = 200
        response_mock.json.return_value = asyncio.Future()
        response_mock.json.return_value.set_result(page_data)

        session_mock = Mock()
        session_mock.get.return_value = asyncio.Future()
        session_mock.get.return_value.set_result(response_mock)

        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(fetch_page_data(session_mock, url, headers, params, 1))

        self.assertEqual(result, page_data)
        session_mock.get.assert_called_once_with(url=url, headers=headers, params=params)
        response_mock.json.assert_called_once()

    @staticmethod
    def fetch_page_data_se(*args):
        return [
            {'client': 'Michael Wilkerson', 'purchase_date': '2022-08-09', 'product': 'Vacuum cleaner', 'price': 346},
            {'client': 'Russell Hill', 'purchase_date': '2022-08-09', 'product': 'Microwave oven', 'price': 446},
            {'client': 'Michael Galloway', 'purchase_date': '2022-08-09', 'product': 'Phone', 'price': 1042}
        ]

    @patch("lesson_02.ht_template.job1.dal.sales_api.unscramble_lists")
    def test_get_sales(self, unscramble_lists_mock):
        date = "2023-06-07"
        expected_result = [
            {'client': 'Michael Wilkerson', 'purchase_date': '2022-08-09', 'product': 'Vacuum cleaner', 'price': 346},
            {'client': 'Russell Hill', 'purchase_date': '2022-08-09', 'product': 'Microwave oven', 'price': 446},
            {'client': 'Michael Galloway', 'purchase_date': '2022-08-09', 'product': 'Phone', 'price': 1042}
        ]

        # Mock fetch_page_data with fetch_page_data_se function
        fetch_page_data = Mock(side_effect=self.fetch_page_data_se)

        # Mock the return value of unscramble_lists
        unscramble_lists_mock.return_value = expected_result

        result = asyncio.run(get_sales(date))

        self.assertEqual(result, expected_result)
        unscramble_lists_mock.assert_called()
