# mle_projects/mle-project-sprint-1-v001/part2_dvc/scripts/data.py
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import yaml

def create_connection():
    load_dotenv()
    host = os.environ.get('DB_DESTINATION_HOST')
    port = os.environ.get('DB_DESTINATION_PORT')
    db = os.environ.get('DB_DESTINATION_NAME')
    username = os.environ.get('DB_DESTINATION_USER')
    password = os.environ.get('DB_DESTINATION_PASSWORD')
    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    return conn

def get_data():
    conn = create_connection()
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    data = pd.read_sql('select * from clean_data_set', conn, index_col=params['index_col'])
    conn.dispose()
    os.makedirs('data', exist_ok=True)
    # Сохраняем данные с индексом (столбцом "id")
    data.to_csv('data/initial_data.csv', index=True)

if __name__ == '__main__':
    get_data()