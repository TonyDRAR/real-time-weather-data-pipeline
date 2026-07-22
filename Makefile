.PHONY: up down ps logs producer-logs spark-logs airflow-logs consume validate clean

up:
	docker compose up --build -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

producer-logs:
	docker compose logs -f weather-producer

spark-logs:
	docker compose logs -f spark-streaming

airflow-logs:
	docker compose logs -f airflow-scheduler

consume:
	docker compose exec kafka kafka-console-consumer \
		--bootstrap-server kafka:29092 \
		--topic weather.raw \
		--from-beginning \
		--max-messages 10

validate:
	python scripts/validate.py
	docker compose config --quiet

clean:
	docker compose down -v
	rm -rf data/bronze/* data/silver/* data/checkpoints/*
