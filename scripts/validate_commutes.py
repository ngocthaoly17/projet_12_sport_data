import os
import time
import requests
import psycopg
from commute_rules import get_commute_rule

DATABASE_URL=os.getenv("DATABASE_URL","postgresql://sport_user:SportData2026Local@localhost:55432/sport_data")
GOOGLE_MAPS_API_KEY=os.getenv("GOOGLE_MAPS_API_KEY","").strip()
COMPANY_ADDRESS=os.getenv("COMPANY_ADDRESS","1362 Av. des Platanes, 34970 Lattes")
DELAY=float(os.getenv("GOOGLE_MAPS_REQUEST_DELAY_SECONDS","0.05"))
URL="https://routes.googleapis.com/directions/v2:computeRoutes"


def route_distance(origin, travel_mode):
    headers={"Content-Type":"application/json","X-Goog-Api-Key":GOOGLE_MAPS_API_KEY,"X-Goog-FieldMask":"routes.distanceMeters,routes.duration"}
    body={"origin":{"address":origin},"destination":{"address":COMPANY_ADDRESS},"travelMode":travel_mode,"computeAlternativeRoutes":False,"languageCode":"fr-FR","units":"METRIC"}
    r=requests.post(URL,headers=headers,json=body,timeout=20)
    r.raise_for_status()
    routes=r.json().get("routes",[])
    if not routes: raise RuntimeError("Aucun itinéraire Google Maps trouvé")
    return int(routes[0]["distanceMeters"])

if not GOOGLE_MAPS_API_KEY:
    raise RuntimeError("GOOGLE_MAPS_API_KEY n'est pas définie")

with psycopg.connect(DATABASE_URL) as conn:
    rows=conn.execute("SELECT employee_id,home_address,commute_mode FROM raw.employees ORDER BY employee_id").fetchall()
    counts={"VALID":0,"ANOMALY":0,"NOT_APPLICABLE":0,"ERROR":0}
    for employee_id,home_address,commute_mode in rows:
        rule=get_commute_rule(commute_mode)
        status="NOT_APPLICABLE"; distance_m=None; distance_km=None; threshold=None; reason=None; api_error=None; travel_mode=None
        if rule:
            travel_mode=rule["travel_mode"]; threshold=rule["threshold_km"]
            try:
                distance_m=route_distance(home_address,travel_mode); distance_km=round(distance_m/1000,2)
                status="VALID" if distance_km <= threshold else "ANOMALY"
                if status=="ANOMALY": reason=f"Distance {distance_km} km supérieure au seuil de {threshold} km."
            except Exception as exc:
                status="ERROR"; api_error=str(exc)
            time.sleep(DELAY)
        conn.execute("""
        INSERT INTO monitoring.commute_validation(employee_id,home_address,company_address,commute_mode,google_travel_mode,distance_m,distance_km,threshold_km,validation_status,anomaly_reason,api_error,checked_at)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
        ON CONFLICT(employee_id) DO UPDATE SET home_address=excluded.home_address,company_address=excluded.company_address,commute_mode=excluded.commute_mode,google_travel_mode=excluded.google_travel_mode,distance_m=excluded.distance_m,distance_km=excluded.distance_km,threshold_km=excluded.threshold_km,validation_status=excluded.validation_status,anomaly_reason=excluded.anomaly_reason,api_error=excluded.api_error,checked_at=now()
        """,(employee_id,home_address,COMPANY_ADDRESS,commute_mode,travel_mode,distance_m,distance_km,threshold,status,reason,api_error))
        counts[status]+=1
print(counts)
