import json
import shutil
import unittest
from unittest.mock import patch, MagicMock
import os

from lesson_02.ht_template.job2.bll.transfer_api import transfer_from_raw_to_stg


class TestTransferApi(unittest.TestCase):

    def read_data_se(*args):
        test_json_data = [
            {'client': 'Michael Wilkerson', 'purchase_date': '2022-08-09', 'product': 'Vacuum cleaner', 'price': 346},
            {'client': 'Russell Hill', 'purchase_date': '2022-08-09', 'product': 'Microwave oven', 'price': 446},
            {'client': 'Michael Galloway', 'purchase_date': '2022-08-09', 'product': 'Phone', 'price': 1042}
        ]
        file_name = 'sales_2022-08-09.json'
        data_list = [(test_json_data, file_name)]
        return data_list

    def load_data_se(*args):
        return True

    @patch('lesson_02.ht_template.job2.bll.transfer_api.read_data')
    @patch('lesson_02.ht_template.job2.bll.transfer_api.load_data')
    def test_transfer_from_raw_to_stg(self, mock_load_data: MagicMock, mock_read_data: MagicMock):
        test_json_data = [
            {'client': 'Michael Wilkerson', 'purchase_date': '2022-08-09', 'product': 'Vacuum cleaner', 'price': 346},
            {'client': 'Russell Hill', 'purchase_date': '2022-08-09', 'product': 'Microwave oven', 'price': 446},
            {'client': 'Michael Galloway', 'purchase_date': '2022-08-09', 'product': 'Phone', 'price': 1042}
        ]
        file_name = 'sales_2022-08-09.json'
        data_list = [(test_json_data, file_name)]

        mock_read_data.side_effect = self.read_data_se
        mock_load_data.return_value = self.load_data_se
        raw_dir = 'lesson_02/ht_template/job2/tests/sales/raw'
        stg_dir = 'lesson_02/ht_template/job2/tests/sales/stg'

        transfer_from_raw_to_stg(raw_dir, stg_dir)

        mock_read_data.assert_called()
        mock_read_data.assert_called_with('lesson_02/ht_template/job2/tests/sales/raw')

        mock_load_data.assert_called_with('lesson_02/ht_template/job2/tests/sales/stg', data_list)
        mock_load_data.assert_called()



    @classmethod
    def tearDown(self) -> None:
        path_to_delete = [
            'lesson_02/ht_template/job2/tests/sales'
        ]
        for path in path_to_delete:
            if os.path.exists(path):
                shutil.rmtree(path)
