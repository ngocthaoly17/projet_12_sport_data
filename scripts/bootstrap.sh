#!/usr/bin/env bash
set -euo pipefail

if [ ! -f .env ]; then
  cp .env.example .env
  echo ".env créé depuis .env.example. Ajoutez SLACK_WEBHOOK_URL si besoin."
fi

echo "1/4 - Démarrage de l'architecture..."
docker compose up -d --build

echo "2/4 - Attente de PostgreSQL/API..."
sleep 8

echo "3/4 - Chargement des référentiels PostgreSQL..."
docker compose exec api python scripts/load_data.py

echo "4/4 - Génération de l'historique sportif..."
docker compose exec api python scripts/generate_activities.py

echo
echo "Architecture démarrée."
echo "API Swagger      : http://localhost:8001/docs"
echo "Redpanda Console : http://localhost:8088"
echo "MinIO Console    : http://localhost:9001"
echo "Grafana          : http://localhost:3000"
echo "Prometheus       : http://localhost:9090"
echo
echo "Test temps réel : docker compose exec api python scripts/generate_live_activity.py"
echo "Logs Slack       : docker compose logs -f slack-consumer"
