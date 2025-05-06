# plugins/steps/real_estate.py
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, Integer, Float, String, Boolean
from sqlalchemy import inspect
import pandas as pd
import logging
#from dotenv import load_dotenv, find_dotenv

#load_dotenv()

# Считываем все креды
#src_host = os.environ.get('DB_SOURCE_HOST')
#src_port = os.environ.get('DB_SOURCE_PORT')
#src_username = os.environ.get('DB_SOURCE_USER')
#src_password = os.environ.get('DB_SOURCE_PASSWORD')
#src_db = os.environ.get('DB_SOURCE_NAME') 

#dst_host = os.environ.get('DB_DESTINATION_HOST')
#dst_port = os.environ.get('DB_DESTINATION_PORT')
#dst_username = os.environ.get('DB_DESTINATION_USER')
#dst_password = os.environ.get('DB_DESTINATION_PASSWORD')
#dst_db = os.environ.get('DB_DESTINATION_NAME')

#s3_bucket = os.environ.get('S3_BUCKET_NAME')
#s3_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
#s3_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

#mle_telegram_chat_id = os.environ.get('MLE_TELEGRAM_CHAT_ID')
#mle_telegram_token = os.environ.get('MLE_TELEGRAM_TOKEN')

logger = logging.getLogger(__name__)

def create_table(**kwargs):
    """Создает таблицу для объединенных данных"""
    try:
        hook = PostgresHook(postgres_conn_id='destination_db')
        engine = hook.get_sqlalchemy_engine()

        inspector = inspect(engine)
        if inspector.has_table('merged_flats_buildings'):
            logger.info("Таблица merged_flats_buildings уже существует")
            return

        metadata = MetaData()
        table = Table(
            'merged_flats_buildings', metadata,
            Column('flat_id', Integer, primary_key=True),
            Column('building_id', Integer),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Boolean),
            Column('studio', Boolean),
            Column('total_area', Float),
            Column('price', Float),
            Column('build_year', Integer),
            Column('building_type', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean)
        )

        with engine.begin() as connection:
            metadata.create_all(connection)
            
        logger.info("Таблица merged_flats_buildings создана")

    except Exception as e:
        logger.error(f"Ошибка создания таблицы: {str(e)}")
        raise


def extract(**kwargs):
    """Извлекает данные из обеих таблиц небольшими партиями."""
    ti = kwargs['ti']
    hook = PostgresHook(postgres_conn_id='destination_db')
    engine = hook.get_sqlalchemy_engine()

    limit = 10000  # Размер партии
    offset = 0
    chunks = []

    while True:
        sql = f"""
            SELECT f.*,
                   b.build_year, b.building_type_int AS building_type,
                   b.latitude, b.longitude, b.ceiling_height,
                   b.flats_count, b.floors_total, b.has_elevator
            FROM flats f
            LEFT JOIN buildings b
            ON f.building_id = b.id
            LIMIT {limit} OFFSET {offset};
        """

        chunk = pd.read_sql(sql, engine)
        if len(chunk) == 0:
            break
        chunks.append(chunk)
        offset += limit

    data = pd.concat(chunks)
    # Установка уникального индекса перед сериализацией
    data.reset_index(drop=True, inplace=True)
    ti.xcom_push(key='raw_data', value=data.to_json())
    
def transform(**kwargs):
    """Обработка данных"""
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract_data', key='raw_data')
    data = pd.read_json(raw_data)

    # Обработка пропусков
    data['ceiling_height'] = data['ceiling_height'].fillna(2.7)
    data['has_elevator'] = data['has_elevator'].fillna(False)

    # Преобразование типов (при необходимости)
    # data['is_apartment'] = data['is_apartment'].astype(bool)
    # data['studio'] = data['studio'].astype(bool)

    ti.xcom_push(key='processed_data', value=data.to_json())


def load(**kwargs):
    """Загрузка в целевую таблицу"""
    ti = kwargs['ti']
    data = pd.read_json(ti.xcom_pull(task_ids='transform_data', key='processed_data'))

    hook = PostgresHook(postgres_conn_id='destination_db')
    engine = hook.get_sqlalchemy_engine()

    data.to_sql(
        name='merged_flats_buildings',
        con=engine,
        if_exists='replace',
        index=False
    )
    logger.info("Данные успешно загружены")