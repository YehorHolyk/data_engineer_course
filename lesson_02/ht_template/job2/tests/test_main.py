from unittest import TestCase, mock

# NB: avoid relative imports when you will write your code
from .. import main

class MainFunctionTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()


    @mock.patch('lesson_02.ht_template.job2.main.transfer_from_raw_to_stg')
    def test_return_400_stg_dir_param_missed(
            self,
            transfer_from_raw_to_stg_mock: mock.MagicMock
        ):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/test/raw/',
                # no 'stg_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)
        transfer_from_raw_to_stg_mock.assert_not_called()

    @mock.patch('lesson_02.ht_template.job2.main.transfer_from_raw_to_stg')
    def test_return_400_raw_dir_param_missed(self, transfer_from_raw_to_stg_mock: mock.MagicMock):
        resp = self.client.post(
            '/',
            json={
                'stg_dir': '/test/stg/',
                # no 'raw_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)
        transfer_from_raw_to_stg_mock.assert_not_called()

    @mock.patch('lesson_02.ht_template.job2.main.transfer_from_raw_to_stg')
    def test_return_201_when_all_is_ok(
            self,
            transfer_from_raw_to_stg_mock: mock.MagicMock
    ):
        transfer_from_raw_to_stg_mock.return_value = None
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/test/raw/',
                'stg_dir': '/test/stg/'
            },
        )

        transfer_from_raw_to_stg_mock.assert_called_once_with('/test/raw/', '/test/stg/')
        self.assertEqual(201, resp.status_code)
