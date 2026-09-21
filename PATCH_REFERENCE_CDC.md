# Patch — Référentiels versionnés par Debezium

## Flux ajouté

PostgreSQL (`raw.employees`, `raw.employee_sports`) → WAL → Debezium → Redpanda → `reference-watcher` → MinIO.

À chaque rafale de changements d'un référentiel (debounce 2 s), le watcher relit la table complète et écrit :

- historique : `reference-source/company/employees_<timestamp>.csv`
- courant : `references/company/employees.csv`
- historique : `reference-source/sports/employee_sports_<timestamp>.csv`
- courant : `references/sports/employee_sports.csv`

Spark Silver relit désormais les fichiers `references` à chaque micro-batch afin d'utiliser la version courante sans redémarrage.

## Installation

Copier les fichiers du patch à la racine du projet en conservant les dossiers, puis :

```powershell
docker compose build api slack-consumer reference-watcher
docker compose up -d debezium debezium-init reference-watcher
docker compose up -d spark-silver
```

Le PUT de `debezium-init` met à jour le connecteur `activities-cdc` pour inclure aussi les deux tables de référentiel.

## Vérification

```powershell
docker compose logs --tail=100 reference-watcher
curl.exe http://localhost:8083/connectors/activities-cdc/status
```

Modifier ensuite une ligne de `raw.employees` ou `raw.employee_sports`. Après environ 2 à 5 secondes, un nouveau CSV horodaté doit apparaître dans `reference-source`, et le fichier courant correspondant dans `references` doit être remplacé.
