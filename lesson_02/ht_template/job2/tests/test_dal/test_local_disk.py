import json
import shutil
import unittest
from unittest.mock import patch, MagicMock
import os

from lesson_02.ht_template.job2.dal.local_disk import read_data, cleanup_folder, load_data


class TestLocalDisk(unittest.TestCase):

    def test_read_data(self):
        test_json_data = [
            {'client': 'Michael Wilkerson', 'purchase_date': '2022-08-09', 'product': 'Vacuum cleaner', 'price': 346},
            {'client': 'Russell Hill', 'purchase_date': '2022-08-09', 'product': 'Microwave oven', 'price': 446},
            {'client': 'Michael Galloway', 'purchase_date': '2022-08-09', 'product': 'Phone', 'price': 1042}
        ]
        raw_dir = 'lesson_02/ht_template/job2/tests/sales/raw'
        file_name = 'sales_2022-08-09.json'

        os.makedirs(raw_dir)

        expected_result = [(test_json_data, file_name)]

        full_json_path = os.path.join(raw_dir, file_name)
        json_content = json.dumps(test_json_data)
        with open(full_json_path, 'w') as file:
            file.write(json_content)

        res = read_data(raw_dir)

        self.assertEqual(res, expected_result)

    def test_load_data(self):
        test_json_data = [
            {'client': 'Michael Wilkerson', 'purchase_date': '2022-08-09', 'product': 'Vacuum cleaner', 'price': 346},
            {'client': 'Russell Hill', 'purchase_date': '2022-08-09', 'product': 'Microwave oven', 'price': 446},
            {'client': 'Michael Galloway', 'purchase_date': '2022-08-09', 'product': 'Phone', 'price': 1042}
        ]
        file_name = 'sales_2022-08-09.json'
        data_list = [(test_json_data, file_name)]

        stg_dir = 'lesson_02/ht_template/job2/tests/sales/stg'
        os.makedirs(stg_dir)

        res = load_data(stg_dir, data_list)
        file_exists = os.path.exists(os.path.join(stg_dir, 'sales_2022-08-09.avro'))
        self.assertEqual(file_exists, True)
        self.assertIsNone(res)

    @classmethod
    def tearDown(self) -> None:
        path_to_delete = [
            'lesson_02/ht_template/job2/tests/sales'
        ]
        for path in path_to_delete:
            if os.path.exists(path):
                shutil.rmtree(path)