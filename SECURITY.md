# Security

Ce dépôt est un projet de démonstration destiné à un environnement local.

- Ne publie jamais le fichier `.env`.
- Remplace tous les mots de passe d'exemple avant un déploiement partagé.
- N'expose pas Kafka, PostgreSQL, Airflow ou Grafana directement sur Internet.
- Utilise un gestionnaire de secrets, TLS, un pare-feu et une authentification adaptée en production.
- En cas de secret publié par erreur, révoque ou remplace immédiatement le secret, puis nettoie l'historique Git.
