import os
from dotenv import load_dotenv
import re

load_dotenv()

DB_PARAMS = {
    "dbname" : os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", 5432)
}

DATA_FOLDER = os.getenv("DATA_FOLDER")


FILENAME_PATTERN = re.compile(r"^btcusd-(\d{4})-(\d{2})-(\d{2})")