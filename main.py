from config import FILENAME_PATTERN, DB_PARAMS, DATA_FOLDER
import psycopg2
from datetime import datetime, timezone
import os

def parse_file_name(file_name):
    """
    - extracts data file name and ensures correct format and date
    - return date string as "YYYY-MM-DD"
    """
    stem, ext = os.path.splitext(file_name)

    if (ext != ".csv"):
        raise Exception(f"{file_name} Input must be CSV.")
    
    if (not FILENAME_PATTERN.match(stem)):
        raise Exception(f"{file_name} must be a valid file name (btcusd-YYYY-MM-DD.csv).")

    return '-'.join(stem.split('-')[1:])

def connect_db(db_params):
    """
    - connect to TimescaleDB using env DB_PARAMS from config.py
    - returns connection and cursor
    """
    conn = psycopg2.connect(**db_params)
    return conn, conn.cursor()

def sort_files_by_date(folder_path):
    """
    - Sorts file names in ascending order to process chronologically
    """
    files = [f for f in os.listdir(folder_path)]
    files.sort()
    return files

def get_max_timestamp(cursor):
    """
    - gets max timestamp currently in db to ensure incremental
    - returns datetimetz with max time
    """
    cursor.execute("SELECT MAX(timestamp) FROM bitcoin_prices;")
    timestamp = cursor.fetchone()[0]

    if (timestamp is None):
        return datetime.min.replace(tzinfo=timezone.utc)
    if (timestamp.tzinfo is None):
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp


def copy_csv_into_db(file_name, folder_path, date_str, cursor):
    """
    - copies data from CSV into database
    - inserts csv directly into temp table
    - prepends date to time and keeps datetime as primary key
    - Inserts into DB
    """

    # temp table make
    create_temp_table = f"""CREATE TEMP TABLE temp_bitcoin_prices (
        time_text       text PRIMARY KEY,
        open            NUMERIC,
        high            NUMERIC,
        low             NUMERIC,
        close           NUMERIC,
        volume_btc      NUMERIC,
        volume_usd      NUMERIC,
        weighted_price  NUMERIC
        ) ON COMMIT DROP;"""
    cursor.execute(create_temp_table)

    # copy csv to temp
    with open(f"{folder_path}/{file_name}", 'r') as f:
        cursor.copy_expert(f"""
            COPY temp_bitcoin_prices("time_text", "open", "high", "low", "close", "volume_btc", "volume_usd", "weighted_price")
            FROM STDIN WITH (FORMAT csv, HEADER, NULL '');
            """, f)
    
    # paste modified temp to db
    insert_sql = f"""
    INSERT INTO bitcoin_prices (timestamp, "open", "high", "low", "close", "volume_btc", "volume_usd", "weighted_price")
    SELECT
        ('{date_str}'::date + (time_text::interval))::timestamptz,
        "open",
        "high",
        "low",
        "close",
        "volume_btc",
        "volume_usd",
        "weighted_price"
    FROM
        temp_bitcoin_prices
    WHERE
        "open" IS NOT NULL AND
        "high" IS NOT NULL AND
        "low" IS NOT NULL AND
        "close" IS NOT NULL AND
        "volume_btc" IS NOT NULL AND
        "volume_usd" IS NOT NULL AND
        "weighted_price" IS NOT NULL
    ON CONFLICT (timestamp) DO NOTHING;
    """

    cursor.execute(insert_sql)


def main():
    """
    - establish db connections, get files, and run csv parsing and storing
    """
    try:
        conn, cursor = connect_db(DB_PARAMS)
        
        folder_path = DATA_FOLDER

        if (conn.closed == 1):
            raise Exception("DB connection is closed")
        
        if (cursor.closed == 1):
            raise Exception("DB cursor is closed")
        
        file_names = sort_files_by_date(folder_path)
        max_timestamp = get_max_timestamp(cursor)

        for file_name in file_names:
            try:
                date = parse_file_name(file_name)
                curr_file_date = datetime.strptime(date, '%Y-%m-%d').date()
                if (max_timestamp.date() >= curr_file_date):
                    print(f"Skipping {file_name}")
                    continue
                print(f"Parsing {file_name}")
                copy_csv_into_db(file_name, folder_path, date, cursor)
                conn.commit()
            except Exception as e:
                print(f"Problem reading file {file_name}: {e}.")
    except Exception as e:
        print(f"Problem setting up database: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()