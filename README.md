# ETL Coging Challenge

# Requirements

- Python 3.9+
- docker and docker-compose

# Files

|-- main.py # run program \n
|-- dataset/ # **where to store btc files** \n
|-- .env # env variables included for ease \n
|-- config.py # store env vars \n
|-- init_db.sql # database table and hypertable init \n
|-- docker-compose.yml # container setup \n
|-- requirements.txt # deps \n
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
