# Dépannage

## Afficher l'état général

```bash
docker compose ps
```

Un service en `Restarting` ou `Exited` nécessite l'analyse de ses logs.

## Le tag Spark Bitnami est introuvable

Le projet utilise l'image :

```text
apache/spark:3.5.3
```

et le chemin :

```text
/opt/spark/bin/spark-submit
```

## `kafka.vendor.six.moves` est introuvable

Utiliser `kafka-python-ng`, pas l'ancien `kafka-python==2.0.2`.

```text
kafka-python-ng==2.2.3
```

Reconstruire le service concerné :

```bash
docker compose build --no-cache weather-producer
docker compose up -d weather-producer
```

## Le DAG Airflow n'apparaît pas

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs --tail=200 airflow-scheduler
```

## Spark ne charge pas le driver PostgreSQL

Au démarrage, les logs doivent contenir :

```text
org.postgresql#postgresql;42.7.5
```

Sinon, vérifier `spark/Dockerfile`, puis :

```bash
docker compose build --no-cache spark-streaming
docker compose up -d spark-streaming
```

## La table PostgreSQL n'existe pas

`postgres/init.sql` est exécuté uniquement lorsque le volume est neuf.

Pour un environnement local jetable :

```bash
docker compose down -v
docker compose up -d weather-db
```

Attention : cette commande supprime les données des volumes.

## Grafana ne se connecte pas

Depuis Grafana, l'hôte PostgreSQL est :

```text
weather-db:5432
```

Depuis DBeaver sur Windows :

```text
localhost:5433
```

## Le port Grafana est occupé

Modifie dans `.env` :

```dotenv
GRAFANA_PORT=3002
```

Puis :

```bash
docker compose up -d --force-recreate grafana
```

## La table est vide

Vérifier dans cet ordre :

```bash
docker compose logs --tail=50 weather-producer
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic weather.raw \
  --from-beginning \
  --max-messages 3
docker compose logs --tail=200 spark-streaming
```

Puis interroger PostgreSQL :

```sql
SELECT *
FROM weather_observations
ORDER BY ingested_at DESC
LIMIT 20;
```

## Les warnings `KafkaDataConsumer ... KAFKA-1894`

Ce sont des avertissements connus et souvent non bloquants. Chercher surtout :

```text
ERROR
StreamingQueryException
Caused by
exited with code 1
```

## Le dashboard ne montre rien

- choisir une période qui contient les données ;
- vérifier la variable `Ville` ;
- vérifier que PostgreSQL contient des lignes ;
- lancer la requête dans Grafana Explore ;
- utiliser `ingested_at` comme axe temporel.
