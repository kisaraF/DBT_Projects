import duckdb
import polars as pl
import logging
import os
import subprocess
from datetime import datetime
from zipfile import ZipFile


def init_logger() -> None:
    log_date = datetime.now().strftime("%Y%m%d%H%M%S")
    log_fp = f"{os.getcwd()}/logs/log_ingestion_{log_date}.log"
    logging.basicConfig(
        level=logging.DEBUG,
        filename=log_fp,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info("Ingestion log file initiated")


def get_files() -> list:
    data_path = f"{os.getcwd()}/Data/faers"
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
    logging.debug("Conection to DuckDB initialized")
    return conn


def get_hash_cols(tb_name: str) -> str:
    if tb_name.upper() == "SRC_DEMO":
        hash_cols = "concat(primaryid, init_fda_dt, fda_dt)"
    elif tb_name.upper() == "SRC_DRUG":
        hash_cols = "concat(primaryid, role_cod, drug_seq, drugname)"
    elif tb_name.upper() == "SRC_REAC":
        hash_cols = "concat(primaryid, pt)"
    elif tb_name.upper() == "SRC_OUTC":
        hash_cols = "concat(primaryid, outc_cod)"

    return hash_cols


def get_record_count(schema_name_i: str, tb_name: str) -> int:
    conn = init_db()

    get_count_q = f"""
    SELECT COUNT(*)
    FROM {schema_name_i}.{tb_name}
    """
    row_count = conn.execute(get_count_q).fetchone()[0]
    return row_count


def write_to_tables(data_dict: dict) -> None:
    con = init_db()

    # Creating the schema if not exists
    schema_name = "SOURCE_FAERS"
    create_schema = f"""CREATE SCHEMA IF NOT EXISTS {schema_name};"""
    con.execute(create_schema)

    # List tables
    tables_ls = (
        con.execute(f"""SHOW TABLES from {schema_name};""").df()["name"].tolist()
    )
    logging.info(f"{len(tables_ls)} tables are located in the database")

    for file in data_dict.keys():
        logging.debug(f"Processig {file}")

        df = pl.read_csv(
            data_dict[file],
            separator="$",
            has_header=True,
            infer_schema_length=0,  # infer_schema_length to make every data point string to skip conversion issues
        )
        logging.debug(f"{file} in polars data frame, has {len(df)} rows")

        file_hash_cols_str = get_hash_cols(file)

        if file not in tables_ls:
            logging.info(
                f"The table {file} is not available. Hence creating a new one..."
            )

            query = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{file} AS
            SELECT 
                *,
                SHA256(CONCAT({file_hash_cols_str})) AS _hash_id,
                GET_CURRENT_TIMESTAMP() as _created_at
            FROM df;
            """
            con.execute(query)
            logging.info(f"Created {file} and Inserted {len(df)} records")
        elif file in tables_ls:
            rec_count_prev = get_record_count(schema_name, file)
            logging.debug(
                f"The table {file} is available and has {rec_count_prev} records. Inserting new data.."
            )

            query = f"""
            INSERT INTO {schema_name}.{file}
            SELECT 
                *,
                SHA256(CONCAT({file_hash_cols_str})) AS _hash_id,
                GET_CURRENT_TIMESTAMP() as _created_at 
            FROM df
            WHERE _hash_id not in (
                SELECT DISTINCT _hash_id
                FROM {schema_name}.{file}
            );
            """
            con.execute(query)

            rec_count_aft = get_record_count(schema_name, file)
            logging.info(
                f"Inserted {rec_count_aft - rec_count_prev} records to table {file}. Deduplicated {(rec_count_aft - rec_count_prev) - len(df)} records"
            )


def move_after_writing(file_name: str) -> None:
    move_destination = f"{os.getcwd()}/Data/Archive/"
    cmd_keywords = ["mv", file_name, move_destination]
    subprocess.run(cmd_keywords)
    logging.info(f"{file_name} moved to {move_destination} after writing")


if __name__ == "__main__":
    init_logger()

    all_files = get_files()

    for batch in all_files:
        logging.info(f"Processing batch -> {batch}")
        data_dict = process_file(batch)
        write_to_tables(data_dict)
        move_after_writing(batch)
