from lesson_02.ht_template.job1.dal import local_disk, sales_api
import os
import asyncio

def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    # 1. get data from the API
    # 2. save data to disk
    print("\tI'm in get_sales(...) function!")
    json_content = asyncio.run(sales_api.get_sales(date=date))
    file_name = f"sales_{date}.json"
    local_disk.save_to_disk(json_content, raw_dir, file_name)
    pass
