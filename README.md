# Projet 12 — Sport Data Solution, architecture événementielle

Cette version reconstruit le POC autour de l'architecture demandée par le mentor : **CDC PostgreSQL avec Debezium, Redpanda, deux traitements Spark, Delta Lake, référentiels S3 et notification Slack**.

Les éléments utiles de l'ancien projet sont conservés : les CSV RH/sportifs, PostgreSQL, le simulateur d'activités, FastAPI, Slack, Prometheus/Grafana, le profiling et les tests de qualité. **Airflow n'est plus dans le chemin principal** ; l'ancien DAG est conservé dans `legacy/airflow/` uniquement comme trace.

## Architecture

```text
Python / API "Strava-like"
          |
          v
   PostgreSQL 16
   (Data System)
          |
       WAL / CDC
          |
          v
      Debezium
          |
          v
      Redpanda
   topic raw.activities
       /       \
      /         \
 Python         Spark #1
 Slack          Streaming
 consumer          |
    |              v
    v          Delta Bronze
  Slack             |
                    v
                 Spark #2
                  /   \
                 /     \
      réf. entreprise  réf. sportif
           S3/MinIO     S3/MinIO
                 \       /
                  \     /
                   v   v
                Delta Silver
                     |
                     v
                  Power BI
```

Dans ce POC local, **MinIO joue le rôle de S3**. Les deux référentiels sont chargés dans le bucket `references` et les tables Delta dans le bucket `lakehouse`.

## Rôle de chaque composant

| Composant | Rôle |
|---|---|
| `api` / scripts Python | simulent Strava et insèrent dans PostgreSQL |
| PostgreSQL | Data System transactionnel ; son WAL constitue le journal de changements |
| Debezium | capture uniquement le différentiel de `raw.activities` |
| Redpanda | broker Kafka-compatible ; topic `sport_data.raw.activities` |
| `slack-consumer` | lit le topic et envoie les activités `source=live` dans Slack |
| Spark Bronze | lit Redpanda en streaming et écrit les événements bruts en Delta |
| MinIO | S3 local pour référentiels et Delta Lake |
| Spark Silver | enrichit Bronze avec les référentiels entreprise et sportif |
| Power BI | consomme la couche analytique Silver ; CSV de compatibilité fourni pour Desktop local |

## Démarrage sous Windows PowerShell

Depuis la racine du projet :

```powershell
Copy-Item .env.example .env
# Optionnel : renseigner SLACK_WEBHOOK_URL dans .env
.\scripts\bootstrap.ps1
```

Ou manuellement :

```powershell
docker compose up -d --build
docker compose exec api python scripts/load_data.py
docker compose exec api python scripts/generate_activities.py
```

Le premier démarrage des conteneurs Spark peut prendre plusieurs minutes car Spark télécharge les connecteurs Delta/Kafka/S3 Maven.

## Vérifier que le CDC fonctionne

Créer **une nouvelle activité live** :

```powershell
docker compose exec api python scripts/generate_live_activity.py
```

Puis suivre la notification :

```powershell
docker compose logs -f slack-consumer
```

Si `SLACK_WEBHOOK_URL` est renseignée, le message est envoyé dans Slack. Sinon le consommateur affiche exactement la notification dans ses logs, ce qui permet de tester sans webhook.

## Interfaces utiles

- API Swagger : `http://localhost:8001/docs`
- Redpanda Console : `http://localhost:8088`
- Kafka Connect / Debezium : `http://localhost:8083/connectors`
- MinIO Console : `http://localhost:9001`

Identifiants MinIO par défaut : ceux du fichier `.env` (`minioadmin` / `minioadmin123`).

## Ce que vous devez voir dans Redpanda

Le topic principal est :

```text
sport_data.raw.activities
```

Un `INSERT` dans `raw.activities` devient automatiquement un message grâce à Debezium. Le code applicatif **ne publie pas lui-même dans Redpanda** : c'est important, car cela démontre bien le CDC.

## Delta Lake

Les chemins logiques sont :

```text
s3://lakehouse/bronze/activities
s3://lakehouse/silver/employee_activities
```

Bronze contient les événements proches de la source. Silver contient notamment :

- identité de l'employé ;
- business unit ;
- type de contrat ;
- sport déclaré ;
- sport de l'activité ;
- date et durée de l'activité ;
- distance en kilomètres ;
- métadonnées Kafka / CDC.

## Power BI Desktop local

Power BI Desktop ne lit pas simplement un Delta Lake MinIO local comme une plateforme Fabric/Databricks. Pour la soutenance locale, un export de compatibilité est donc prévu :

```powershell
docker compose --profile tools run --rm powerbi-export
```

Le fichier produit est :

```text
powerbi/output/sport_activity_powerbi.csv
```

Il se charge avec **Power BI Desktop > Obtenir les données > Texte/CSV**. L'architecture cible reste bien Delta Lake ; le CSV est uniquement une passerelle locale de démonstration.

## Commandes de diagnostic

État des conteneurs :

```powershell
docker compose ps
```

État du connecteur Debezium :

```powershell
curl http://localhost:8083/connectors/activities-cdc/status
```

Voir les logs Spark :

```powershell
docker compose logs -f spark-bronze
docker compose logs -f spark-silver
```

Voir les activités en base :

```powershell
docker compose exec db psql -U sport_user -d sport_data -c "select activity_id, employee_id, sport_type, source from raw.activities order by activity_id desc limit 10;"
```

## Réinitialiser complètement le POC

Pour recommencer avec des volumes vides, y compris le slot Debezium et le Lakehouse :

```powershell
docker compose down -v
```

Puis relancer `bootstrap.ps1`.

## Différence avec l'ancienne version

Avant, le pipeline principal était piloté en batch et Slack était appelé directement par l'API. Maintenant, l'API **ne fait qu'écrire dans le Data System**. Debezium capture le changement, Redpanda le distribue, puis Slack et Spark le consomment indépendamment. C'est ce découplage qui correspond au schéma du mentor.

## Mise à jour 2026-09 — consolidation du POC

Cette version consolide les améliorations validées dans le POC récent tout en conservant l'architecture event-driven Debezium → Redpanda → Spark → Delta/MinIO.

Ajouts :
- validation Google Maps des trajets domicile → entreprise (`scripts/validate_commutes.py`, `sql/002_commute_validation.sql`) ;
- règles RH paramétrées : 5 jours wellness à partir de 15 activités sur 12 mois et bonus mobilité de 5 % (`sql/003_benefit_eligibility.sql`) ;
- 8 règles Data Quality techniques et métier (`sql/004_activity_data_quality.sql`) ;
- normalisation `Runing` → `Running` et distances uniquement pour Marche/Running/Randonnée/Triathlon ;
- consumer Slack enrichi avec prénom/nom, durée et distance, uniquement pour les événements `source=live` ;
- consumer Slack en `auto.offset.reset=latest` avec commit après traitement pour ne pas rejouer l'historique lors d'un nouveau groupe.

Après initialisation d'une base existante, appliquer les migrations SQL 002 à 004 manuellement si le volume PostgreSQL existe déjà.
