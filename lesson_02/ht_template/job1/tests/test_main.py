from unittest import TestCase, mock

# NB: avoid relative imports when you will write your code
from .. import main


class MainFunctionTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()


    @mock.patch('lesson_02.ht_template.job1.main.save_sales_to_local_disk')
    def test_return_400_date_param_missed(
            self,
            get_sales_mock: mock.MagicMock
        ):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/foo/bar/',
                # no 'date' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_return_400_raw_dir_param_missed(self):
        resp = self.client.post(
            '/',
            json={
                'date': '2022-08-09',
                # no 'raw_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    @mock.patch('lesson_02.ht_template.job1.main.save_sales_to_local_disk')
    def test_return_201_when_all_is_ok(
            self,
            get_sales_mock: mock.MagicMock
    ):
        get_sales_mock.return_value = None
        resp = self.client.post(
            '/',
            json={
                'date': '2022-08-09',
                'raw_dir': '/foo/bar/'
            },
        )

        self.assertEqual(201, resp.status_code)
        get_sales_mock.assert_called_with(date='2022-08-09', raw_dir='/foo/bar/')
