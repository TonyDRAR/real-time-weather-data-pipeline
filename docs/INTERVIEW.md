# Présenter le projet en entretien

## Pitch de 30 secondes

> J'ai construit une plateforme météo near real-time. Un producteur Python interroge Open-Meteo et publie des événements dans Kafka. Spark Structured Streaming les valide et les stocke dans une architecture Bronze/Silver ainsi que dans PostgreSQL. Des vues Gold alimentent un dashboard Grafana, tandis qu'Airflow supervise la disponibilité de chaque étape. L'ensemble est reproductible avec Docker Compose.

## Problème métier

Le système permet de suivre automatiquement plusieurs métriques météo et fournit une base réutilisable pour :

- surveiller des conditions environnementales ;
- déclencher des alertes ;
- comparer plusieurs villes ;
- corréler la météo avec des ventes, du trafic ou des capteurs.

## Pourquoi Kafka ?

- découplage entre producteurs et consommateurs ;
- conservation des événements ;
- possibilité de rejouer ;
- extensibilité vers plusieurs consommateurs ;
- montée en charge par partitions.

## Pourquoi Spark ?

- API DataFrame ;
- traitement streaming et batch avec le même moteur ;
- schéma explicite ;
- capacité à traiter des volumes supérieurs à un simple script Python ;
- checkpointing.

## Pourquoi PostgreSQL alors que Silver existe ?

Parquet est efficace pour le data lake. PostgreSQL est plus pratique pour les requêtes interactives de Grafana et pour les analystes SQL. Les deux répondent à des usages différents.

## Pourquoi Airflow ?

Airflow apporte l'orchestration, les retries, la planification et la visibilité opérationnelle. Le streaming lui-même reste géré par Kafka et Spark.

## Difficultés rencontrées

- incompatibilité de l'ancien paquet `kafka-python` avec Python 3.12 ;
- tag d'image Spark indisponible ;
- configuration des noms d'hôtes Docker ;
- ajout du driver JDBC PostgreSQL ;
- distinction entre ports internes et ports exposés ;
- persistance par volumes et checkpoints.

## Limites à reconnaître honnêtement

- Open-Meteo ne produit pas forcément une nouvelle mesure toutes les 30 secondes ;
- architecture locale mono-broker et mono-nœud Spark ;
- mots de passe de développement ;
- pas de chiffrement TLS ;
- garanties exactement-once vers PostgreSQL à renforcer ;
- absence actuelle de tests d'intégration complets.

## Formulation CV

> Conception d'un pipeline de données near real-time avec Python, Kafka, Spark Structured Streaming, PostgreSQL, Airflow, Grafana et Docker. Mise en place d'une architecture Bronze/Silver/Gold, d'un dashboard provisionné et de contrôles automatisés de santé du pipeline.
