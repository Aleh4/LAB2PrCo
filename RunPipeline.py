from pathlib import Path
import csv
from src.pipeline import (Root, ensure_dirs, 
                          list_raw_csvs,make_clean_name,safe_stem, 
                          clean_file, kpis_volt, plot_voltage_line, 
                          plot_voltage_hist, plot_boxplot_by_sensor)
# === Parámetros ===
ROOT = Root(__file__)
RAW_DIR = ROOT / "DATA" / "RAW"
PROC_DIR = ROOT / "DATA" / "PROCCESING"
PLOTS_DIR = ROOT / "plots"
REPORTS_DIR = ROOT / "Reportes"
UMBRAL_V = 90

ensure_dirs(RAW_DIR, PROC_DIR, PLOTS_DIR, REPORTS_DIR)

def main():
    raw_files = list_raw_csvs(RAW_DIR, pattern="*.csv")
    if not raw_files:
        print(f"No hay CSV en crudo en {RAW_DIR}"); return

    resumen_kpis = []
    sensor_to_volts = {}  # para el boxplot global

    for in_path in raw_files:
        # Nombre de salida limpio
        clean_name = make_clean_name(in_path)
        out_path = PROC_DIR / clean_name

        # 1) Limpiar y escribir CSV limpio
        ts, volts, humedad, stats = clean_file(in_path, out_path)
        if not ts:
            print("Sin datos válidos:", in_path.name)
            continue

        # 2) KPIs por archivo (voltaje)
        kv = kpis_volt(humedad, umbral=UMBRAL_V)
        resumen_kpis.append({
            "archivo": in_path.name,
            "salida": out_path.name,
            **stats,  # calidad
            "n": kv["n"], "min": kv["min"], "max": kv["max"],
            "prom": kv["prom"], "alerts": kv["alerts"], "alerts_pct": kv["alerts_pct"]
        })

        # 3) Gráficos por archivo
        stem_safe = safe_stem(out_path)
        plot_voltage_line(
            ts, humedad, UMBRAL_V,
            title=f"Humedad vs Tiempo — {out_path.name}",
            out_path=PLOTS_DIR / f"{stem_safe}__volt_line__{UMBRAL_V:.1f}%.png"
        )
        plot_voltage_hist(
            humedad,
            title=f"Histograma Voltaje — {out_path.name}",
            out_path=PLOTS_DIR / f"{stem_safe}__volt_hist.png",
            bins=20
        )

        # 4) Acumular para boxplot global (sensor = id en nombre si aplica)
        # si tus archivos siguen formato 'voltaje_sensor_100XY.csv', etiqueta con 'S-100XY'
        name = out_path.stem
        sensor_id = name.replace("voltaje_sensor_", "")
        sensor_key = f"S-{sensor_id}" if sensor_id != name else name
        sensor_to_volts.setdefault(sensor_key, []).extend(humedad)

    # 5) Guardar reporte KPIs
    rep_csv = REPORTS_DIR / "kpis_por_archivo.csv"
    with rep_csv.open("w", encoding="utf-8", newline="") as f:
        cols = ["archivo","salida","filas_totales","filas_validas","descartes_timestamp",
                "descartes_valor","%descartadas","n","min","max","prom","alerts","alerts_pct"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for row in resumen_kpis:
            w.writerow(row)
    print("Reporte KPIs:", rep_csv)

    # 6) Boxplot global por sensor
    if sensor_to_volts:
        plot_voltage_box = PLOTS_DIR / "boxplot_todos_sensores.png"
        plot_boxplot_by_sensor(sensor_to_volts, plot_voltage_box)
        print("Boxplot global:", plot_voltage_box)

if __name__ == "__main__":
    main()