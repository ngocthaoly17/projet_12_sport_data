MODE_RULES = {
    "Marche/running": {"travel_mode": "WALK", "threshold_km": 15},
    "Vélo/Trottinette/Autres": {"travel_mode": "BICYCLE", "threshold_km": 25},
}

def get_commute_rule(commute_mode):
    return MODE_RULES.get((commute_mode or "").strip())
