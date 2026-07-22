from __future__ import annotations

import ast
import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def validate_python() -> int:
    paths = list(ROOT.rglob("*.py"))
    for path in paths:
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return len(paths)


def validate_compose() -> int:
    compose_path = ROOT / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    required_services = {
        "kafka",
        "kafka-ui",
        "weather-db",
        "weather-producer",
        "spark-streaming",
        "airflow-webserver",
        "airflow-scheduler",
        "grafana",
    }

    services = set(compose.get("services", {}))
    missing = required_services - services
    if missing:
        raise ValueError(f"Services manquants : {sorted(missing)}")

    return len(services)


def validate_dashboard() -> None:
    dashboard_path = ROOT / "grafana/dashboards/weather-dashboard.json"
    dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))

    if dashboard.get("uid") != "weather-monitoring":
        raise ValueError("UID Grafana inattendu")

    if not dashboard.get("panels"):
        raise ValueError("Le dashboard ne contient aucun panel")


def validate_required_files() -> None:
    required = [
        ".env.example",
        ".gitignore",
        "README.md",
        "LICENSE",
        "postgres/init.sql",
        "grafana/provisioning/datasources/postgres.yml",
        "grafana/provisioning/dashboards/dashboard.yml",
    ]

    missing = [item for item in required if not (ROOT / item).exists()]
    if missing:
        raise ValueError(f"Fichiers manquants : {missing}")


def main() -> None:
    validate_required_files()
    python_count = validate_python()
    service_count = validate_compose()
    validate_dashboard()

    print(
        f"Validation OK : {python_count} fichiers Python, "
        f"{service_count} services Docker et dashboard Grafana valide."
    )


if __name__ == "__main__":
    main()
