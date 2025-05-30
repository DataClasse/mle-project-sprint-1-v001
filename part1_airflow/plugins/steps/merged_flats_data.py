# plugins/steps/merged_flats_data.py
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, Integer, Float, String, Boolean, DateTime, UniqueConstraint
from sqlalchemy import inspect
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def create_table(**kwargs):
    """Создает таблицу для объединенных данных"""
    try:
        hook = PostgresHook(postgres_conn_id='destination_db')
        engine = hook.get_sqlalchemy_engine()

        inspector = inspect(engine)
        if inspector.has_table('merged_flats_dataset'):
            logger.info("Таблица merged_flats_dataset уже существует")
            return

        metadata = MetaData()
        table = Table(
            'merged_flats_dataset', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
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
            Column('has_elevator', Boolean),
            UniqueConstraint("id", name="uq_id")
        )

        with engine.begin() as connection:
            metadata.create_all(connection)
            
        logger.info("Таблица merged_flats_dataset создана")

    except Exception as e:
        logger.error(f"Ошибка создания таблицы: {str(e)}")
        raise


def extract(**kwargs):
    """Извлекает данные из обеих таблиц"""
    ti = kwargs['ti']
    hook = PostgresHook(postgres_conn_id='destination_db')
    engine = hook.get_sqlalchemy_engine()

    sql = """
        SELECT f.*, b.build_year, b.building_type_int AS building_type,
               b.latitude, b.longitude, b.ceiling_height,
               b.flats_count, b.floors_total, b.has_elevator
        FROM flats f
        LEFT JOIN buildings b ON f.building_id = b.id;   # зменил на LEFT JOIN
    """

    data = pd.read_sql(sql, engine)
    ti.xcom_push(key='raw_data', value=data.to_json())

def transform(**kwargs):
    """Обработка данных"""
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract_data', key='raw_data')
    data = pd.read_json(raw_data)

    ti.xcom_push(key='processed_data', value=data.to_json())

def load(**kwargs):
    """Загрузка в целевую таблицу"""
    ti = kwargs['ti']
    data = pd.read_json(ti.xcom_pull(task_ids='transform_data', key='processed_data'))

    hook = PostgresHook(postgres_conn_id='destination_db')
    engine = hook.get_sqlalchemy_engine()

    data.to_sql(
        name='merged_flats_dataset',
        con=engine,
        if_exists='replace',
        index=False
    )
    logger.info("Данные успешно загружены")