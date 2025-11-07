import duckdb
import polars as pl
import logging
import os
import subprocess
from datetime import datetime
from zipfile import ZipFile
import io

def init_logger() -> None:
    log_fp = f'{os.getcwd()}/logs/log_ingestion.log'
    logging.basicConfig(level=logging.DEBUG, filename = log_fp, filemode='w',
    format = '%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Ingestion log file initiated")

def get_current_year() -> int:
    curr_year = datetime.now().date().year
    logging.info(f"Current Year : {curr_year}")
    return curr_year

def get_files(year_i:int) -> list:
    data_path = f'{os.getcwd()}/Data/faers_{year_i}'
    all_files = [f'{data_path}/{i}' for i in os.listdir(data_path)]
    logging.info(f'Found {len(all_files)} untracked files for ingestion @ {data_path}')
    return all_files

def process_file(file:str) -> dict:
    allowable_file_names = ['DEMO', 'DRUG', 'OUTC', 'REAC']
    mem_read_file = ZipFile(file)
    txt_files_only = [
        file for file in mem_read_file.namelist() if file.endswith('.txt') and file.startswith('ASCII') 
        and any(name in file for name in allowable_file_names)]

    dict_data = {f'SRC_{fname.split('/')[-1].split('.')[0][:4]}': mem_read_file.read(fname) for fname in txt_files_only}

    return dict_data

def init_db() -> None:
    conn = duckdb.connect('faers_source.duckdb')
    return conn

def write_to_tables(data_dict:dict) -> None:
    con = init_db()
    
    # List tables
    tables_ls = con.execute('''SHOW TABLES;''').df()['name'].tolist()
    print(tables_ls)
    
    for file in data_dict.keys():
        print(file)
        df = pl.read_csv(data_dict[file], separator='$', has_header=True, infer_schema_length=0)
        print(f'{file} raw df has {len(df)} rows')
        if file not in tables_ls:
            print(f'{file} does not exist in the database')
            query = f'''
            CREATE TABLE IF NOT EXISTS {file} AS
            SELECT * FROM df;
            '''
            con.execute(query)
        elif file in tables_ls:
            print(f'{file} does exist in the database')
            query = f'''
            INSERT INTO {file}
            SELECT * FROM df;
            '''
            con.execute(query)


if __name__ == '__main__':
    init_logger()

    curr_year = get_current_year()

    all_files = get_files(curr_year)
    print(all_files)
    print('\n\n')

    for batch in all_files:
        print(f'Processing {batch}')
        data_dict = process_file(batch)
        write_to_tables(data_dict)

