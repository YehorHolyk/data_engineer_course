"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
from flask import Flask, request
from flask import typing as flask_typing

from lesson_02.ht_template.job2.bll.transfer_api import transfer_from_raw_to_stg

app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "raw_dir: "/path/to/my_dir/raw/sales/2022-08-09"
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    raw_dir = input_data.get('raw_dir')
    stg_dir = input_data.get('stg_dir')

    if not stg_dir:
        return {
                   "message": "stg_dir parameter missed",
               }, 400

    if not raw_dir:
        return {
                   "message": "raw_dir parameter missed",
               }, 400

    transfer_from_raw_to_stg(raw_dir, stg_dir)

    return {
               "message": "Data transfered successfully from API",
           }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
