# plugins/steps/clean_flats_data.py
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
        INNER JOIN buildings b ON f.building_id = b.id;
    """

    data = pd.read_sql(sql, engine)
    ti.xcom_push(key='raw_data', value=data.to_json())


def clean_data(**kwargs):
    
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract_data', key='raw_data')
    data = pd.read_json(raw_data)

    # 1. Удаление дубликатов по ID
    duplicates_mask = data.duplicated(subset=['id'], keep=False)
    logger.info(f"Найдено дубликатов по ID: {duplicates_mask.sum()}")
    data = data.drop_duplicates(subset=['id'], keep='first')

    # 2. Обработка пропусков с учетом типа данных
    # Для апартаментов разрешаем нулевую площадь кухни
    kitchen_mask = (~data['is_apartment']) & (data['kitchen_area'].isna())
    data.loc[kitchen_mask, 'kitchen_area'] = data.loc[kitchen_mask, 'kitchen_area'].fillna(0)

    numerical_columns = [
        'price', 'total_area', 'ceiling_height', 
        'floor', 'build_year', 'flats_count'
    ]
    
    # Заполнение медианой с логированием
    for col in numerical_columns:
        before = data[col].isna().sum()
        median_val = data[col].median()
        data[col].fillna(median_val, inplace=True)
        logger.info(f"Заполнено пропусков в {col}: {before} -> {data[col].isna().sum()}")

    # 3. Обработка выбросов с использованием 1% и 99% квантилей
    outlier_config = {
        'price': {'q_low': 0.01, 'q_high': 0.99},
        'total_area': {'q_low': 0.01, 'q_high': 0.99},
        'ceiling_height': {'q_low': 0.01, 'q_high': 0.99},
        'floor': {'q_low': 0.01, 'q_high': 0.99},
        'build_year': {'q_low': 0.01, 'q_high': 0.99},
        'flats_count': {'q_low': 0.05, 'q_high': 0.95}  # Особый случай
    }

    initial_count = len(data)
    for col, config in outlier_config.items():
        q_low = data[col].quantile(config['q_low'])
        q_high = data[col].quantile(config['q_high'])
        
        before = len(data)
        data = data[(data[col] >= q_low) & (data[col] <= q_high)]
        removed = before - len(data)
        logger.info(f"Удалено выбросов в {col}: {removed} (границы: {q_low:.2f}-{q_high:.2f})")

    logger.info(f"Всего удалено записей по выбросам: {initial_count - len(data)}")

    # 4. Логические проверки
    # Этажи
    floor_issues = data[data['floor'] > data['floors_total']]
    data = data.drop(floor_issues.index)
    logger.info(f"Удалено некорректных этажей: {len(floor_issues)}")

    # Годы постройки
    current_year = pd.Timestamp.now().year
    year_issues = data[(data['build_year'] < 1850) | (data['build_year'] > current_year)]
    data = data.drop(year_issues.index)
    logger.info(f"Удалено нереальных годов постройки: {len(year_issues)}")

    # Координаты (пример для Москвы)
    coord_mask = (
        data['latitude'].between(55.0, 56.5) & 
        data['longitude'].between(36.5, 38.5)
    )
    geo_issues = data[~coord_mask]
    data = data[coord_mask]
    logger.info(f"Удалено неверных координат: {len(geo_issues)}")

    # 5. Сохранение результатов
    ti.xcom_push(key='cleaned_data', value=data.to_json())
    logger.info(f"Финальный размер данных после очистки: {len(data)}")


def load(**kwargs):
    """Загрузка в целевую таблицу"""
    ti = kwargs['ti']
    data = pd.read_json(ti.xcom_pull(task_ids='clean_data', key='cleaned_data'))

    hook = PostgresHook(postgres_conn_id='destination_db')
    engine = hook.get_sqlalchemy_engine()

    data.to_sql(
        name='clean_data_set',
        con=engine,
        if_exists='replace',
        index=False
    )
    logger.info("Очищенные данные успешно загружены в таблицу clean_data_set")