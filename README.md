# Проект 1 спринта

Добро пожаловать в репозиторий-шаблон Практикума для проекта 1 спринта. Цель проекта — создать базовое решение для предсказания стоимости квартир Яндекс Недвижимости.

Полное описание проекта хранится в уроке «Проект. Разработка пайплайнов подготовки данных и обучения модели» на учебной платформе.

Здесь укажите имя вашего бакета: s3-student-mle-20250228-981403deb1

Этап 1. Сбор данных - выполнен.
- код DAG, оформленный в репозитории GitHub (DataClasse /mle-project-sprint-1-v001/part1_airflow)
- таблица с собранным датасетом в БД. Собран dataset: merged_flats_dataset
DAG:
    - dags/merge_flats_ETL.py
plugins/steps:
    - part1_airflow/plugins/steps/messages.py
    Функции обработки:
    - `send_success`, `send_failure`
    - /part1_airflow/plugins/steps/merged_flats_data.py
    Функции обработки:
    - `create_table`, `extract`, `transform` и `load`

Этап 2. Очистка данных - выполнен.
DAG:
    - plugins/steps/clean_flats_data.py
plugins/steps:
    - part1_airflow/plugins/steps/messages.py
    Функции обработки:
    - `send_success`, `send_failure`
    - plugins/steps/clean_flats_data.py
    Функции обработки:
    - `create_table`, `extract`,`clean_data`,`load`
- функции, которые исправляют ошибки в данных, оформленные в Jupyter Notebook и сохранённые в репозитории GitHub (удаление дубликатов, пропусков, выбросов),
- код DAG, оформленный в репозитории GitHub,
- таблица с очищенным датасетом в БД. Собран dataset: clean_data_set

Этап 3. Создание DVC-пайплайна обучения модели - выполнен
    путь до файлов с конфигурацией DVC-пайплайна dvc.yaml, params.yaml, dvc.lock - mle-project-sprint-1-v001/part2_dvc
    путь до файлов с Python-кодом для этапов DVC-пайплайна - mle-project-sprint-1-v001/part2_dvc/scripts
- файлы с конфигурациями DVC-пайплайна, сохранённые в репозитории GitHub,
- обученная модель, сохранённая в хранилище S3.

Для оценки качества обученной модели были выбраны три метрики:
    - MAE (средняя абсолютная ошибка);
    - MSE (среднеквадратичная ошибка);
    - R² (коэффициент детерминации).

