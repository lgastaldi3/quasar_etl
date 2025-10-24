# ETL Coging Challenge

# Requirements

- Python 3.9+
- docker and docker-compose

# Files

|-- main.py # run program \
|-- dataset/ # **where to store btc files** \
|-- .env # env variables included for ease \
|-- config.py # store env vars \
|-- init_db.sql # database table and hypertable init \
|-- docker-compose.yml # container setup \
|-- requirements.txt # deps \
|-- scratch.ipynb # scratch work and iteration

# Instructions (in terminal)

- git clone <repo url>
- cd quasar_etl
- python3.11 -m venv venv # optional
- source venv/bin/activate # optional
- pip install -r requirements.txt
- docker-compose up -d
- **add data files to dataset/**
- python main.py
