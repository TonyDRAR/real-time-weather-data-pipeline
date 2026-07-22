from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather.raw")
LATITUDE = os.getenv("OPEN_METEO_LATITUDE", "48.8566")
LONGITUDE = os.getenv("OPEN_METEO_LONGITUDE", "2.3522")
SILVER_PATH = Path(
    os.getenv("SILVER_PATH", "/opt/pipeline/data/silver")
)

WEATHER_DB_HOST = os.getenv("WEATHER_DB_HOST", "weather-db")
WEATHER_DB_PORT = int(os.getenv("WEATHER_DB_PORT", "5432"))
WEATHER_DB_NAME = os.getenv("WEATHER_DB_NAME", "weather")
WEATHER_DB_USER = os.getenv("WEATHER_DB_USER", "weather_user")
WEATHER_DB_PASSWORD = os.getenv(
    "WEATHER_DB_PASSWORD", "change_me_weather"
)


def check_open_meteo() -> None:
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "current": "temperature_2m",
            "timezone": "UTC",
        },
        timeout=20,
    )
    response.raise_for_status()

    if "current" not in response.json():
        raise AirflowException("Réponse Open-Meteo invalide")


def ensure_kafka_topic() -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="airflow-weather-topic-admin",
    )

    try:
        admin.create_topics(
            [
                NewTopic(
                    name=KAFKA_TOPIC,
                    num_partitions=3,
                    replication_factor=1,
                )
            ]
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def check_recent_kafka_event() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=65_000,
        group_id=None,
    )

    try:
        records = consumer.poll(timeout_ms=60_000, max_records=1)
        if not records:
            raise AirflowException(
                "Aucun nouvel événement Kafka reçu pendant le contrôle"
            )
    finally:
        consumer.close()


def check_silver_output() -> None:
    if not SILVER_PATH.exists():
        raise AirflowException(f"Répertoire Silver absent : {SILVER_PATH}")

    parquet_files = list(SILVER_PATH.rglob("*.parquet"))
    if not parquet_files:
        raise AirflowException("Aucun fichier Parquet dans la zone Silver")

    newest = max(parquet_files, key=lambda path: path.stat().st_mtime)
    age_seconds = datetime.now().timestamp() - newest.stat().st_mtime

    if age_seconds > 15 * 60:
        raise AirflowException(
            f"Le dernier fichier Silver a plus de 15 minutes : {newest}"
        )


def check_weather_database() -> None:
    connection = psycopg2.connect(
        host=WEATHER_DB_HOST,
        port=WEATHER_DB_PORT,
        dbname=WEATHER_DB_NAME,
        user=WEATHER_DB_USER,
        password=WEATHER_DB_PASSWORD,
        connect_timeout=10,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*), MAX(ingested_at)
                FROM weather_observations_dedup
                """
            )
            count, latest_ingested_at = cursor.fetchone()

        if count == 0 or latest_ingested_at is None:
            raise AirflowException(
                "PostgreSQL ne contient aucune observation météo"
            )
    finally:
        connection.close()


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="weather_pipeline_monitoring",
    description="Contrôle Open-Meteo, Kafka, Spark Silver et PostgreSQL",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["portfolio", "kafka", "spark", "postgresql"],
) as dag:
    api_health = PythonOperator(
        task_id="check_open_meteo_api",
        python_callable=check_open_meteo,
    )

    kafka_topic = PythonOperator(
        task_id="ensure_kafka_topic",
        python_callable=ensure_kafka_topic,
    )

    kafka_event = PythonOperator(
        task_id="check_recent_kafka_event",
        python_callable=check_recent_kafka_event,
    )

    silver_output = PythonOperator(
        task_id="check_silver_output",
        python_callable=check_silver_output,
    )

    database_output = PythonOperator(
        task_id="check_weather_database",
        python_callable=check_weather_database,
    )

    api_health >> kafka_topic >> kafka_event >> silver_output >> database_output
