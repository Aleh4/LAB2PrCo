from statistics import mean

def kpis_volt(hum, umbral=90.0):
    """KPIs de voltaje: n, min, max, prom, alertas y %."""
    hum = [float(v) for v in hum if v is not None]
    n = len(hum)
    if n == 0:
        return {"n":0,"min":None,"max":None,"prom":None,"alerts":0,"alerts_pct":0.0}
    alerts = sum(v > umbral for v in hum)
    return {
        "n": n,
        "min": min(hum),
        "max": max(hum),
        "prom": mean(hum),
        "alerts": alerts,
        "alerts_pct": 100.0 * alerts / n
    }