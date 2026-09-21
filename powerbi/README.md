# Power BI

Le stockage analytique principal est le **Delta Lake Silver** dans MinIO :
`s3://lakehouse/silver/employee_activities`.

Pour un POC local avec Power BI Desktop, le moyen le plus simple est de produire un CSV de compatibilité :

```powershell
docker compose --profile tools run --rm powerbi-export
```

Le fichier final est créé dans :

`powerbi/output/sport_activity_powerbi.csv`

Dans Power BI Desktop : **Obtenir les données > Texte/CSV** puis sélectionner ce fichier.

> En production, Power BI se connecterait au Lakehouse via une plateforme exposant Delta (Fabric, Databricks, Synapse/ADLS, etc.). Le CSV ici est uniquement un pont local pour la démonstration.
