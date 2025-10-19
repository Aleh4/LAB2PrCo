from statistics import mean

def kpis_volt(temp, umbral=90.0):
    """KPIs de voltaje: n, min, max, prom, alertas y %."""
    temp = [float(v) for v in temp if v is not None]
    n = len(temp)
    if n == 0:
        return {"n":0,"min":None,"max":None,"prom":None,"alerts":0,"alerts_pct":0.0}
    alerts = sum(v > umbral for v in temp)
    return {
        "n": n,
        "min": min(temp),
        "max": max(temp),
        "prom": mean(temp),
        "alerts": alerts,
        "alerts_pct": 100.0 * alerts / n
    }