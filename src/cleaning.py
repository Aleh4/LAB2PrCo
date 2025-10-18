import csv
from datetime import datetime
from pathlib import Path
from .IO_Utils import detectar_delimitador

NA_TOKENS = {"", "na", "n/a", "nan", "null", "none", "error"}

def parse_ts(s: str):
    """Normaliza a datetime (acepta ISO 'YYYY-MM-DDTHH:MM:SS' y 'dd/mm/YYYY HH:MM:SS')."""
    s = (s or "").strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    if "T" in s and len(s) >= 19:
        try:
            return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None
    return None

def parse_v(s: str):
    """Convierte a float, admitiendo coma decimal y tokens NA."""
    if s is None:
        return None
    s = str(s).strip().replace(",", ".").lower()
    if s in NA_TOKENS:
        return None
    try:
        return float(s)
    except ValueError:
        return None
    
def clean_file(in_path: Path, out_path: Path, ts_col="timestamp", v_col_candidates=("voltage_V","voltaje","value")):
    """
    Lee un CSV crudo, limpia y escribe un CSV con encabezado: timestamp,voltage_V
    Devuelve: (ts_list, volts_list, stats_dict)
    """
    delim = detectar_delimitador(in_path)
    ts_list, volts_list, Humedad = [], [], []
    total = kept = bad_ts = bad_val = 0

    with in_path.open("r", encoding="utf-8", newline="") as fin, \
         out_path.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin, delimiter=delim)
        writer = csv.DictWriter(fout, fieldnames=["timestamp", "voltage_V","humedad"])
        writer.writeheader()

        for row in reader:
            total += 1
            ts_raw = (row.get(ts_col) or "").strip()
            t = parse_ts(ts_raw)
            if t is None:
                bad_ts += 1; continue

            v_raw = None
            for cand in v_col_candidates:
                if row.get(cand) is not None:
                    v_raw = row.get(cand)
                    break
            v = parse_v(v_raw)
            if v is None:
                bad_val += 1; continue
            #ecuacion lineal de calibracion -
            hum=2+(100-2)*(v-0.4)/(7.0-0.4)
            
            writer.writerow({
                "timestamp": t.strftime("%Y-%m-%dT%H:%M:%S"),
                "voltage_V": f"{v:.3f}",
                "humedad": f"{hum:.3f}"
            })
            
          
            ts_list.append(t); volts_list.append(v); Humedad.append(hum); kept += 1

    stats = {
        "filas_totales": total,
        "filas_validas": kept,
        "descartes_timestamp": bad_ts,
        "descartes_valor": bad_val,
        "%descartadas": round(((bad_ts + bad_val) / total * 100.0) if total else 0.0, 2),
    }

    return ts_list, volts_list,Humedad,stats