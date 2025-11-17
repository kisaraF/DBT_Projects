import requests
import json
import duckdb
import logging
import pandas as pd
from datetime import datetime
import os


def init_logger() -> None:
    log_date = datetime.now().strftime("%Y%m%d")
    log_file_path = f"{os.getcwd()}/logs/{log_date}.log"
    logging.basicConfig(
        level=logging.DEBUG,
        filename=log_file_path,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logging.info("Log File Created!")


def init_duckdb() -> None:
    con = duckdb.connect("fda_animals_raw.duckdb")
    # logging.debug("Connection to DB initiated")
    return con


def ot_schema_creation() -> None:
    conn = init_duckdb()

    query = """
    create schema if not exists raw_fda_animal; 
"""

    conn.execute(query)
    logging.info("Schema `raw_fda_animal` newly created")


def table_creation() -> None:
    conn = init_duckdb()

    query = """
CREATE TABLE IF NOT EXISTS raw_fda_animal.raw_data(
    raw_payload JSON,
    _created_at TIMESTAMP,
    _hash_id VARCHAR
); 
"""

    conn.execute(query)
    logging.info("Table `raw_fda_animal.raw_data` newly created")


def retrieve_construct(skip_i: int) -> str:
    url = f"https://api.fda.gov/animalandveterinary/event.json?skip={skip_i}&limit=100"
    logging.debug(f"Link created as {url}")
    return url


def get_data(url: str) -> list[dict]:
    res = requests.get(url)
    data = res.json()
    logging.debug("Results successfully extracted")
    return data["results"]


def get_record_count() -> int:
    conn = init_duckdb()

    query = """
SELECT COUNT(distinct _hash_id)
FROM raw_fda_animal.raw_data
"""
    row_count = conn.execute(query).fetchone()[0]
    return row_count


def insert_to_table(data_json: list[dict]) -> None:
    rows = [
        json.dumps(r) for r in data_json
    ]  # This will make sure that inconsistent data for each keys in the JSON will correctly get inserted
    df = pd.DataFrame({"raw_payload": rows})
    logging.debug(f"{len(df)} rows are extracted and about to be inserted")

    before_count = get_record_count()

    conn = init_duckdb()

    query = """
INSERT INTO raw_fda_animal.raw_data
SELECT 
    raw_payload,
    CURRENT_LOCALTIMESTAMP() as _created_at,
    SHA256(raw_payload) as _hash_id
FROM df
WHERE _hash_id not in(
    SELECT DISTINCT _hash_id
    FROM raw_fda_animal.raw_data
)
"""

    conn.execute(query)

    after_count = get_record_count()

    logging.info(
        f"Insertion completed. A total of {after_count} rows exists. {(after_count - before_count) - len(df)} number of records scd retained"
    )


if __name__ == "__main__":
    # ot_schema_creation()
    # table_creation()

    init_logger()

    for i in range(0, 10001, 100):
        url_to_pass = retrieve_construct(i)
        results = get_data(url_to_pass)
        insert_to_table(results)

    # get_status = main()
    # print(get_status)
