import unittest
from unittest.mock import patch, Mock

from lesson_02.ht_template.job1.bll.sales_api import save_sales_to_local_disk


class TestSalesApi(unittest.TestCase):

    @patch('lesson_02.ht_template.job1.dal.sales_api.get_sales')
    @patch('lesson_02.ht_template.job1.dal.local_disk.save_to_disk')
    def test_save_sales_to_local_disk(self, mock_save_to_disk, mock_get_sales):
        mock_get_sales.return_value = [
            {'client': 'Michael Wilkerson', 'purchase_date': '2022-08-09', 'product': 'Vacuum cleaner', 'price': 346},
            {'client': 'Russell Hill', 'purchase_date': '2022-08-09', 'product': 'Microwave oven', 'price': 446},
            {'client': 'Michael Galloway', 'purchase_date': '2022-08-09', 'product': 'Phone', 'price': 1042}
        ]

        save_sales_to_local_disk('2022-08-09', '/path/to/raw_dir')

        mock_get_sales.assert_called_once_with(date='2022-08-09')
        mock_save_to_disk.assert_called_once_with([
            {'client': 'Michael Wilkerson', 'purchase_date': '2022-08-09', 'product': 'Vacuum cleaner', 'price': 346},
            {'client': 'Russell Hill', 'purchase_date': '2022-08-09', 'product': 'Microwave oven', 'price': 446},
            {'client': 'Michael Galloway', 'purchase_date': '2022-08-09', 'product': 'Phone', 'price': 1042}
        ],
            '/path/to/raw_dir', 'sales_2022-08-09.json')
