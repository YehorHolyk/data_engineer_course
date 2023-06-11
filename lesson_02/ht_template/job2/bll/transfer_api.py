from lesson_02.ht_template.job2.dal.local_disk import read_data, load_data


def transfer_from_raw_to_stg(raw_dir: str, stg_dir: str) -> None:
    data_list = read_data(raw_dir)
    load_data(stg_dir, data_list)
