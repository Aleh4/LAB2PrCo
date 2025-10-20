"""Generador de CSV tipo 'humedad_sucio.csv' — sin pandas.
Genera filas con timestamp (YYYY-MM-DDTHH:MM:SS) y un valor aleatorio
entre -50.00 y 50.00 con 2 decimales.

Uso:
    python Gen.py --out data/raw/humedad_sucio_sim.csv --rows 100 --freq 60
"""
from pathlib import Path
import argparse
import csv
from datetime import datetime, timedelta
import random
import sys

def generate_csv(path: Path, rows: int, start: datetime, freq_seconds: int, seed: int | None = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    if seed is not None:
        random.seed(seed)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        # Encabezado simple similar a humedad_sucio.csv (ajustar si tu archivo original difiere)
        writer.writerow(["timestamp", "value"])
        ts = start
        for _ in range(rows):
            val = round(random.uniform(0, 6), 2)
            # timestamp normalizado a YYYY-MM-DDTHH:MM:SS
            writer.writerow([ts.strftime("%Y-%m-%dT%H:%M:%S"), f"{val:.2f}"])
            ts += timedelta(seconds=freq_seconds)
    return path

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Generar CSV con timestamps y valores aleatorios [-50,50].")
    p.add_argument("--out", "-o", type=str, default="data/raw/humedad_sucio_sim.csv", help="Ruta de salida (CSV).")
    p.add_argument("--rows", "-n", type=int, default=200, help="Número de filas a generar.")
    p.add_argument("--start", "-s", type=str, default=None,
                   help="Fecha/hora inicial en formato 'YYYY-MM-DDTHH:MM:SS'. Por defecto ahora.")
    p.add_argument("--freq", "-f", type=int, default=60, help="Segundos entre filas (frecuencia).")
    p.add_argument("--seed", type=int, default=None, help="Semilla aleatoria (opcional).")
    return p.parse_args(argv)

def main(argv=None):
    args = parse_args(argv)
    out_path = Path(args.out)
    if args.start:
        try:
            start_dt = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            print("Formato de --start inválido. Use YYYY-MM-DDTHH:MM:SS", file=sys.stderr)
            sys.exit(2)
    else:
        start_dt = datetime.now().replace(microsecond=0)
    csv_path = generate_csv(out_path, args.rows, start_dt, args.freq, args.seed)
    print(f"CSV generado: {csv_path}  (filas={args.rows})")

if __name__ == "__main__":
    main()