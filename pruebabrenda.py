from pathlib import Path
import csv
from src.pipeline import (Root, ensure_dirs, 
                          list_raw_csvs, make_clean_name, safe_stem, 
                          # ASUMIMOS que clean_file ahora transforma V -> T
                          clean_file, 
                          # ASUMIMOS que estas funciones ahora trabajan con Temperatura (T)
                          kpis_temp, plot_temp_line, 
                          plot_temp_hist, plot_boxplot_by_sensor)
# === Parámetros ===
# Cambios en las rutas para usar 'data/' en lugar de 'DATA/'
ROOT = Root(_file_)
RAW_DIR = ROOT / "data" / "raw"
PROC_DIR = ROOT / "data" / "processed"
PLOTS_DIR = ROOT / "plots"
REPORTS_DIR = ROOT / "reports"
# Umbral de Temperatura: T% > 80 (como se indica en los requisitos)
UMBRAL_T = 80.0 

# Parámetros de Calibración (V1, T1) y (V2, T2) para la transformación V -> T
# H(0.4V) = -30C, H(5.6V) = 120C
CALIB_P1 = (0.4, -30.0) # (V1, T1)
CALIB_P2 = (5.6, 120.0) # (V2, T2)


ensure_dirs(RAW_DIR, PROC_DIR, PLOTS_DIR, REPORTS_DIR)

def main():
    raw_files = list_raw_csvs(RAW_DIR, pattern="*.csv")
    if not raw_files:
        print(f"No hay CSV en crudo en {RAW_DIR}"); return

    resumen_kpis = []
    sensor_to_temps = {}  # Acumulador para el boxplot global (Temp)

    for in_path in raw_files:
        # Nombre de salida limpio
        clean_name = make_clean_name(in_path)
        out_path = PROC_DIR / clean_name

        # 1) Limpiar, Transformar (Voltaje -> Temperatura) y escribir CSV limpio
        # Se asume que clean_file ahora recibe los puntos de calibración
        # y devuelve la lista de Temperaturas (T) en lugar de Voltajes (V/Humedad)
        try:
            ts, temp_c, stats = clean_file(in_path, out_path, CALIB_P1, CALIB_P2)
        except TypeError:
             # Si clean_file no se actualizó para aceptar P1, P2, asume la versión anterior:
             ts, temp_c, _, stats = clean_file(in_path, out_path)

        if not ts:
            print("Sin datos válidos (después de limpieza y transformación):", in_path.name)
            continue

        # 2) KPIs por archivo (Temperatura)
        # Se renombra kpis_volt -> kpis_temp para reflejar el uso de temperatura
        kt = kpis_temp(temp_c, umbral=UMBRAL_T)
        resumen_kpis.append({
            "archivo": in_path.name,
            "salida": out_path.name,
            **stats,  # métricas de calidad
            "n": kt["n"], "min": kt["max"], "max": kt["max"],
            "prom": kt["prom"], 
            "alerts": kt["alerts"], 
            "alerts_pct": kt["alerts_pct"]
        })

        # 3) Gráficos por archivo (Temperatura)
        stem_safe = safe_stem(out_path)
        
        # Gráfico de Línea
        plot_temp_line(
            ts, temp_c, UMBRAL_T, # temp_c en lugar de humedad
            title=f"Temperatura vs Tiempo (°C) — {out_path.name}",
            out_path=PLOTS_DIR / f"{stem_safe}_temp_line_{UMBRAL_T:.1f}C.png"
        )
        
        # Histograma
        plot_temp_hist(
            temp_c, # temp_c en lugar de humedad
            title=f"Histograma Temperatura (°C) — {out_path.name}",
            out_path=PLOTS_DIR / f"{stem_safe}__temp_hist.png",
            bins=20
        )

        # 4) Acumular para boxplot global (Temperatura)
        name = out_path.stem
        # Asumiendo la lógica de identificación de sensor original
        sensor_id = name.replace("voltaje_sensor_", "")
        sensor_key = f"S-{sensor_id}" if sensor_id != name else name
        sensor_to_temps.setdefault(sensor_key, []).extend(temp_c)

    # 5) Guardar reporte KPIs
    rep_csv = REPORTS_DIR / "kpis_por_archivo.csv"
    with rep_csv.open("w", encoding="utf-8", newline="") as f:
        # Actualización de columnas para reflejar Temperaturas y métricas de calidad detalladas
        cols = ["archivo","salida","filas_totales","filas_validas","descartes_timestamp",
                "descartes_valor","%descartadas","n","min","max","prom","alerts","alerts_pct"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(resumen_kpis)
    print("Reporte KPIs:", rep_csv)

    # 6) Boxplot global por sensor
    if sensor_to_temps:
        plot_voltage_box = PLOTS_DIR / "boxplot_todos_sensores_temp.png"
        plot_boxplot_by_sensor(sensor_to_temps, plot_voltage_box) # sensor_to_temps en lugar de sensor_to_volts
        print("Boxplot global (Temperatura):", plot_voltage_box)

if _name_ == "_main_":
    main()