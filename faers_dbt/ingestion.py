import duckdb
import polars as pl
import logging
import os

# import subprocess
from datetime import datetime
from zipfile import ZipFile
# import io


def init_logger() -> None:
    log_fp = f"{os.getcwd()}/logs/log_ingestion.log"
    logging.basicConfig(
        level=logging.DEBUG,
        filename=log_fp,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info("Ingestion log file initiated")


def get_current_year() -> int:
    curr_year = datetime.now().date().year
    logging.info(f"Current Year : {curr_year}")
    return curr_year


def get_files(year_i: int) -> list:
    data_path = f"{os.getcwd()}/Data/faers_{year_i}"
    all_files = [f"{data_path}/{i}" for i in os.listdir(data_path)]
    logging.info(f"Found {len(all_files)} untracked files for ingestion @ {data_path}")
    return all_files


def process_file(file: str) -> dict:
    allowable_file_names = ["DEMO", "DRUG", "OUTC", "REAC"]
    mem_read_file = ZipFile(file)
    txt_files_only = [
        file
        for file in mem_read_file.namelist()
        if file.endswith(".txt")
        and file.startswith("ASCII")
        and any(name in file for name in allowable_file_names)
    ]

    dict_data = {
        f"SRC_{fname.split('/')[-1].split('.')[0][:4]}": mem_read_file.read(fname)
        for fname in txt_files_only
    }

    return dict_data


def init_db() -> None:
    conn = duckdb.connect("faers_source.duckdb")
    logging.info("Conection to DuckDB initialized")
    return conn


def write_to_tables(data_dict: dict) -> None:
    con = init_db()

    # Creating the schema if not exists
    schema_name = "SOURCE_FAERS"
    create_schema = f"""CREATE SCHEMA IF NOT EXISTS {schema_name};"""
    con.execute(create_schema)
    logging.info(f"Schema {schema_name} created")

    # List tables
    tables_ls = (
        con.execute(f"""SHOW TABLES from {schema_name};""").df()["name"].tolist()
    )
    logging.info(f"{len(tables_ls)} tables are located in the database")

    for file in data_dict.keys():
        logging.debug(f"processig {file}")

        df = pl.read_csv(
            data_dict[file], separator="$", has_header=True, infer_schema_length=0
        )
        logging.debug(f"{file} in polars data frame, has {len(df)} rows")

        if file not in tables_ls:
            logging.debug(
                f"The table {file} is not available. Hence creating a new one..."
            )

            query = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{file} AS
            SELECT * FROM df;
            """
            con.execute(query)
            logging.info(f"Created {file} and Inserted {len(df)} records")
        elif file in tables_ls:
            logging.debug(f"The table {file} is available. Inserting new data..")

            query = f"""
            INSERT INTO {schema_name}.{file}
            SELECT * FROM df;
            """
            con.execute(query)
            logging.info(f"Inserted {len(df)} records to table {file}")


if __name__ == "__main__":
    init_logger()

    curr_year = get_current_year()

    all_files = get_files(curr_year)

    for batch in all_files:
        logging.info(f"Processing batch -> {batch}")
        data_dict = process_file(batch)
        write_to_tables(data_dict)
