import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
LOGGER = logging.getLogger("weather-producer")

RUNNING = True

API_URL = "https://api.open-meteo.com/v1/forecast"
CITY_NAME = os.getenv("CITY_NAME", "Paris")
LATITUDE = float(os.getenv("LATITUDE", "48.8566"))
LONGITUDE = float(os.getenv("LONGITUDE", "2.3522"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather.raw")


def stop(_signum: int, _frame: Any) -> None:
    global RUNNING
    RUNNING = False
    LOGGER.info("Arrêt demandé")


def build_producer(max_attempts: int = 30) -> KafkaProducer:
    for attempt in range(1, max_attempts + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda key: key.encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=100,
            )
            LOGGER.info("Connexion Kafka établie")
            return producer
        except NoBrokersAvailable:
            LOGGER.warning(
                "Kafka indisponible, tentative %s/%s", attempt, max_attempts
            )
            time.sleep(min(attempt * 2, 10))

    raise RuntimeError("Impossible de se connecter à Kafka")


def fetch_weather(session: requests.Session) -> dict[str, Any]:
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "current": ",".join(
            [
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "precipitation",
                "weather_code",
                "wind_speed_10m",
            ]
        ),
        "timezone": "UTC",
    }

    response = session.get(API_URL, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    current = payload.get("current")

    if not isinstance(current, dict):
        raise ValueError("Champ 'current' absent de la réponse Open-Meteo")

    return {
        "event_id": str(uuid.uuid4()),
        "city": CITY_NAME,
        "latitude": float(payload.get("latitude", LATITUDE)),
        "longitude": float(payload.get("longitude", LONGITUDE)),
        "observed_at": current.get("time"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "temperature_2m": current.get("temperature_2m"),
        "relative_humidity_2m": current.get("relative_humidity_2m"),
        "apparent_temperature": current.get("apparent_temperature"),
        "precipitation": current.get("precipitation"),
        "weather_code": current.get("weather_code"),
        "wind_speed_10m": current.get("wind_speed_10m"),
    }


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    producer = build_producer()
    session = requests.Session()

    try:
        while RUNNING:
            started_at = time.monotonic()

            try:
                event = fetch_weather(session)
                future = producer.send(
                    KAFKA_TOPIC,
                    key=CITY_NAME,
                    value=event,
                )
                metadata = future.get(timeout=15)
                LOGGER.info(
                    "Événement envoyé topic=%s partition=%s offset=%s "
                    "temperature=%s",
                    metadata.topic,
                    metadata.partition,
                    metadata.offset,
                    event["temperature_2m"],
                )
            except (requests.RequestException, ValueError, KafkaError) as exc:
                LOGGER.exception("Échec d'un cycle d'ingestion : %s", exc)

            elapsed = time.monotonic() - started_at
            time.sleep(max(0, POLL_INTERVAL_SECONDS - elapsed))
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)
        session.close()
        LOGGER.info("Producteur arrêté proprement")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOGGER.exception("Erreur fatale")
        sys.exit(1)
