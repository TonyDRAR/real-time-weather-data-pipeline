# Cours — Comprendre le pipeline

## 1. Qu'est-ce qu'un pipeline de données ?

Un pipeline déplace des données d'une source vers une destination en appliquant éventuellement des transformations.

Dans ce projet :

```text
Open-Meteo → Kafka → Spark → fichiers/PostgreSQL → Grafana
```

Le pipeline est **near real-time** : les événements sont traités quelques secondes après leur arrivée, mais il ne s'agit pas d'un système à latence garantie en millisecondes.

## 2. ETL et ELT

### ETL

- **Extract** : récupérer les données de l'API ;
- **Transform** : typer, valider et nettoyer avec Spark ;
- **Load** : écrire dans Parquet et PostgreSQL.

### ELT

Dans un ELT, les données sont chargées avant d'être transformées dans la destination, par exemple avec dbt dans un entrepôt. Notre projet est surtout un ETL streaming, avec quelques transformations Gold effectuées ensuite dans PostgreSQL.

## 3. API REST

Le producteur envoie une requête HTTP à Open-Meteo. La réponse est du JSON. Il extrait les champs utiles puis ajoute :

- un identifiant d'événement ;
- la ville ;
- l'heure d'ingestion.

Aucune clé API n'est placée dans le dépôt.

## 4. Kafka

Kafka est le journal d'événements du pipeline.

### Topic

`weather.raw` contient les événements météo bruts.

### Partition

Le topic possède trois partitions. Une partition est un journal ordonné. La clé Kafka est le nom de la ville, ce qui permet de conserver les événements d'une même ville dans la même partition.

### Offset

Chaque message possède une position appelée offset. Les offsets permettent au consommateur de savoir jusqu'où il a lu.

### Pourquoi Kafka ?

Kafka découple la production et la consommation :

- le producteur ne connaît pas Spark ;
- Spark peut être redémarré ;
- d'autres consommateurs pourraient être ajoutés : alertes, archivage, machine learning.

## 5. Spark Structured Streaming

Spark lit Kafka sous forme de DataFrame streaming.

Le job fonctionne en micro-batches :

1. lire les nouveaux offsets ;
2. parser le JSON ;
3. appliquer le schéma ;
4. filtrer les valeurs incohérentes ;
5. écrire les résultats ;
6. enregistrer la progression dans un checkpoint.

### Checkpoint

Le checkpoint stocke l'état et les offsets traités dans :

```text
data/checkpoints/
```

Ne supprime pas ce dossier pendant que Kafka conserve ses messages, sauf si tu souhaites volontairement rejouer les événements.

## 6. Architecture Bronze, Silver, Gold

### Bronze

Données les plus proches possible de la source. Elles servent à rejouer ou auditer le pipeline.

### Silver

Données propres :

- types cohérents ;
- dates converties ;
- valeurs invalides filtrées ;
- partitions logiques ;
- format Parquet adapté à l'analytique.

### Gold

Données prêtes pour un besoin métier. Ici, les vues SQL :

- donnent la dernière valeur par ville ;
- calculent les statistiques horaires ;
- dédupliquent les événements rejoués.

## 7. PostgreSQL

PostgreSQL fournit une interface SQL simple pour Grafana et les analystes.

Le conteneur utilise un volume Docker. `docker compose down` arrête le service mais conserve le volume. `docker compose down -v` supprime le volume.

La table est append-only : Spark ajoute des lignes. La vue de déduplication protège les usages analytiques contre un éventuel rejeu.

## 8. Grafana

Grafana ne stocke pas les observations. Il interroge PostgreSQL.

Le projet provisionne automatiquement :

- la datasource PostgreSQL ;
- le dashboard ;
- le rafraîchissement toutes les 30 secondes.

Le graphique principal utilise `ingested_at`, car cette colonne représente le moment où le pipeline a collecté chaque événement.

## 9. Airflow

Airflow orchestre des tâches planifiées. Il ne transporte pas les événements et ne remplace ni Kafka ni Spark.

Son DAG vérifie régulièrement que les différentes briques continuent de fonctionner.

## 10. Docker

Chaque composant tourne dans un conteneur isolé.

### Réseau

Tous les services partagent `weather-net`. À l'intérieur de Docker :

```text
weather-db:5432
kafka:29092
```

Depuis Windows :

```text
localhost:5433
localhost:9092
```

Le premier port d'un mapping est le port de la machine ; le second est le port du conteneur.

### Volumes

Les volumes nommés conservent Kafka, PostgreSQL, Airflow et Grafana. Les dossiers `data/` sont montés directement depuis le dépôt.

## 11. Cycle complet d'un événement

1. Le producteur appelle Open-Meteo.
2. Il crée un événement JSON.
3. Kafka l'enregistre dans `weather.raw`.
4. Spark lit l'événement.
5. Spark conserve le JSON brut en Bronze.
6. Spark valide et écrit le Parquet Silver.
7. Spark insère la ligne dans PostgreSQL.
8. Les vues Gold la rendent exploitable.
9. Grafana exécute une requête SQL et actualise le dashboard.
10. Airflow vérifie périodiquement la santé de la chaîne.

## 12. Commandes à retenir

```bash
docker compose up --build -d
docker compose ps
docker compose logs -f spark-streaming
docker compose stop spark-streaming
docker compose up -d spark-streaming
docker compose down
docker compose down -v
```

`Ctrl+C` après `docker compose logs -f` arrête seulement le suivi des logs.

## 13. Évolutions possibles

- ingestion multi-villes ;
- couche Gold matérialisée avec dbt ;
- alertes Grafana ;
- tests de données avec Soda ou Great Expectations ;
- métriques Prometheus ;
- stockage objet S3/MinIO ;
- schéma Avro et Schema Registry ;
- déploiement Kubernetes ;
- CI avec tests d'intégration ;
- stratégie d'upsert exactement-once côté PostgreSQL.
