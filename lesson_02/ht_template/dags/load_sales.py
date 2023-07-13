import os

from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
import json
from typing import List, Any, Sequence
from utils import cleanup_folder


class LoadSalesOperator(SimpleHttpOperator):
    template_fields: Sequence[str] = ("execution_date_str", "raw_dir", "folder_path", "file_name", "file_path",)

    @apply_defaults
    def __init__(self, execution_date_str: str, raw_dir: str, **kwargs):
        super().__init__(**kwargs)
        self.execution_date_str = execution_date_str
        self.raw_dir = raw_dir
        self.folder_path = os.path.join(self.raw_dir, self.execution_date_str)
        self.file_name = f"sales_{self.execution_date_str}.json"
        self.file_path = os.path.join(self.folder_path, self.file_name)


    def unscramble_lists(self, nested_list: List[Any]) -> List[Any]:
        result = []
        for item in nested_list:
            if isinstance(item, list):
                result.extend(self.unscramble_lists(item))
            else:
                result.append(item)
        return result

    def execute(self, context: Context):
        ti = context['ti']
        print(f"HEADERS: {self.headers}")

        self.data['date'] = self.execution_date_str
        responses = []
        try:
            page = 1
            while True:
                self.data['page'] = page
                print(f"DATA: {self.data}")
                response = super().execute(context)
                responses.append(json.loads(response))
                page += 1
        except AirflowException:
            print("Pages amount limit reached")
        if len(responses) == 0:
            raise AirflowException('No Data')

        responses = self.unscramble_lists(responses)
        print(responses)

        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
        else:
            cleanup_folder(self.folder_path)

        with open(self.file_path, 'w') as file:
            json_content = json.dumps(responses)
            file.write(json_content)
        print(f"Data saved to: {self.file_path}")

        ti.xcom_push('execution_date', self.execution_date_str)
        ti.xcom_push('raw_file_path', self.file_path)
