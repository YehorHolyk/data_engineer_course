import shutil
import unittest
from unittest.mock import Mock, patch
import asyncio
import os
import json

from lesson_02.ht_template.job1.dal.local_disk import save_to_disk, cleanup_folder


class SaveToDiskTestCase(unittest.TestCase):

    def test_cleanup_folder(self):
        test_path = "lesson_02/ht_template/job1/tests/test_dal/non_empty_folder/"
        test_path_with_subfolder = "lesson_02/ht_template/job1/tests/test_dal/non_empty_folder/subfolder"
        non_existing_folder = "lesson_02/ht_template/job1/tests/test_dal/non_existing_folder/"
        os.makedirs(test_path)
        test_file_path = os.path.join(test_path_with_subfolder, "test_file.txt")
        cleanup_folder(test_path)

        self.assertEqual(os.path.exists(test_path_with_subfolder), False)
        self.assertEqual(os.path.exists(test_file_path), False)

        res = cleanup_folder(non_existing_folder)

        self.assertIsNone(res)


    def test_save_to_disk(self):
        json_content = [
            {'client': 'Michael Wilkerson', 'purchase_date': '2022-08-09', 'product': 'Vacuum cleaner', 'price': 346},
            {'client': 'Russell Hill', 'purchase_date': '2022-08-09', 'product': 'Microwave oven', 'price': 446},
            {'client': 'Michael Galloway', 'purchase_date': '2022-08-09', 'product': 'Phone', 'price': 1042}
        ]
        file_name = "sales-2022-08-09.json"
        folder_path = "lesson_02/ht_template/job1/tests/test_dal/sales/"
        save_to_disk(json_content, folder_path, file_name)

        self.assertEqual(os.path.exists(os.path.join(folder_path, file_name)), True)
        self.assertEqual(os.path.exists(folder_path), True)
        save_to_disk(json_content, folder_path, file_name)
        self.assertEqual(os.path.exists(os.path.join(folder_path, file_name)), True)


    @classmethod
    def tearDown(self) -> None:
        paths_to_delete = [
            "lesson_02/ht_template/job1/tests/test_dal/non_empty_folder/",
            "lesson_02/ht_template/job1/tests/test_dal/sales/sales-2022-08-09.json"
        ]
        for path in paths_to_delete:
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)

