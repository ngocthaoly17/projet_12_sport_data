$ErrorActionPreference = "Stop"

if (-not (Test-Path .env)) {
    Copy-Item .env.example .env
    Write-Host ".env créé depuis .env.example. Ajoute SLACK_WEBHOOK_URL si besoin."
}

Write-Host "1/4 - Démarrage de l'architecture..."
docker compose up -d --build

Write-Host "2/4 - Attente de PostgreSQL/API..."
Start-Sleep -Seconds 8

Write-Host "3/4 - Chargement des référentiels PostgreSQL..."
docker compose exec api python scripts/load_data.py

Write-Host "4/4 - Génération de l'historique sportif..."
docker compose exec api python scripts/generate_activities.py

Write-Host ""
Write-Host "Architecture démarrée."
Write-Host "API Swagger      : http://localhost:8001/docs"
Write-Host "Redpanda Console : http://localhost:8088"
Write-Host "MinIO Console    : http://localhost:9001"
Write-Host "Grafana          : http://localhost:3000"
Write-Host "Prometheus       : http://localhost:9090"
Write-Host ""
Write-Host "Test temps réel : docker compose exec api python scripts/generate_live_activity.py"
Write-Host "Logs Slack       : docker compose logs -f slack-consumer"
