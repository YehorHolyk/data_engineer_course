import os

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
import json
import fastavro
from typing import Any, Sequence
from utils import cleanup_folder

AVRO_SCHEMA = {
    'name': 'Sales',
    'type': 'record',
    'fields': [
        {'name': 'client', 'type': 'string'},
        {'name': 'purchase_date', 'type': 'string'},
        {'name': 'product', 'type': 'string'},
        {'name': 'price', 'type': 'int'}
    ]
}


class ConvertToAvroOperator(BaseOperator):
    template_fields: Sequence[str] = ("execution_date_str", "raw_file_path",
                                      "stg_dir", "stg_folder", "stg_file_path",
                                      "raw_file_path", "stg_file_name",)
    @apply_defaults
    def __init__(self, execution_date_str: str, raw_file_path: str, stg_dir: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.execution_date_str = execution_date_str
        self.raw_file_path = raw_file_path
        self.stg_dir = stg_dir
        self.stg_folder = os.path.join(self.stg_dir, self.execution_date_str)
        self.stg_file_name = f"sales_{execution_date_str}.avro"
        self.stg_file_path = os.path.join(self.stg_folder, self.stg_file_name)
        self.raw_file_path = raw_file_path

    def execute(self, context: Context) -> Any:

        if not os.path.exists(self.stg_folder):
            os.makedirs(self.stg_folder)
        else:
            cleanup_folder(self.stg_folder)

        with open(self.raw_file_path, 'r') as json_file:
            data = json.load(json_file)
            print(f"Data read from: {self.raw_file_path}")

        with open(self.stg_file_path, 'wb') as avro_file:
            fastavro.writer(avro_file, AVRO_SCHEMA, data)
            print(f"Data write to: {self.stg_file_path}")
