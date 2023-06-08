from typing import List, Tuple, Dict, Any
import fastavro
import os
import json
from lesson_02.ht_template.job1.dal.local_disk import cleanup_folder

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


def read_data(raw_dir: str) -> List[Tuple[Any, str]]:
    data_list = []
    if os.path.exists(raw_dir) is True and len(os.listdir(raw_dir)) != 0:
        for root, dirs, files in os.walk(raw_dir):
            for file in files:
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as json_file:
                    data = json.load(json_file)
                data_list.append((data, file))
                print(f"File {file} read from {raw_dir}")
    return data_list


def load_data(stg_dir: str, data_list: List[Tuple[Any, str]]) -> None:
    if not os.path.exists(stg_dir):
        os.makedirs(stg_dir)
    else:
        cleanup_folder(stg_dir)

    for data, file in data_list:
        file = file.replace('.json', '.avro')
        out_path = os.path.join(stg_dir, file)
        with open(out_path, 'wb') as avro_file:
            fastavro.writer(avro_file, AVRO_SCHEMA, data)
        print(f"File {file} writen to {stg_dir}")
