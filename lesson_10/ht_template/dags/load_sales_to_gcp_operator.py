import os
import re
import pendulum

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.decorators import apply_defaults


class LoadSalesFromLocalFilesystemToGCSOperator(LocalFilesystemToGCSOperator):
    template_fields = (
        "execution_date",
        "execution_date_year",
        "execution_date_month",
        "execution_date_day",
        "file_name",
        "src",
        "dst",
    )

    @apply_defaults
    def __init__(self,
                 execution_date: str,
                 execution_date_year: str,
                 execution_date_month: str,
                 execution_date_day: str,
                 **kwargs):

        super().__init__(**kwargs)

        self.execution_date = execution_date
        self.execution_date_year = execution_date_year
        self.execution_date_month = execution_date_month
        self.execution_date_day = execution_date_day

        self.file_name = f"sales_{execution_date}.json"
        self.src = os.path.join(self.src, self.execution_date, self.file_name)
        self.dst = os.path.join(self.dst,
                                f"year={self.execution_date_year}",
                                f"month={self.execution_date_month}",
                                f"day={self.execution_date_day}",
                                self.file_name)
