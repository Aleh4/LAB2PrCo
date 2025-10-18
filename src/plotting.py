import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

def plot_voltage_line(ts, volts, umbral_v: float, title: str, out_path: Path):
    plt.figure(figsize=(9,4))
    plt.plot(ts, volts, label="humedad (%)")
    alerts_t = [t for t, v in zip(ts, volts) if v > umbral_v]
    alerts_v = [v for v in volts if v > umbral_v]
    plt.scatter(alerts_t, alerts_v,color="#f40404d2",label=f"Alertas (> {umbral_v} V)")
    ax = plt.gca()
    for t, v in zip(alerts_t, alerts_v):
        ax.annotate(f"{v:.2f}V",               # Permite ver los puntos donde se pasa del umbral
                    xy=(t, v),                 # punto a anotar
                    xytext=(0, 8),             # desplazamiento del texto (px)
                    textcoords="offset points",
                    ha="center", va="bottom",
                    fontsize=8)
    plt.axhline(umbral_v, linestyle="--", label=f"Umbral {umbral_v} %")
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.title(title); plt.xlabel("Tiempo"); plt.ylabel("hum")
    plt.grid(True); plt.legend(); plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150); plt.show()

def plot_voltage_hist(volts, title: str, out_path: Path, bins: int = 20):
    plt.figure(figsize=(6,4))
    plt.hist(volts, bins=bins)
    plt.title(title); plt.xlabel("hum"); plt.ylabel("Frecuencia")
    plt.grid(True); plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150); plt.show()

def plot_boxplot_by_sensor(sensor_to_volts: dict[str, list[float]], out_path: Path):
    labels = list(sensor_to_volts.keys())
    data = [sensor_to_volts[k] for k in labels if sensor_to_volts[k]]
    if not data:
        raise RuntimeError("No hay datos para boxplot.")
    horizontal = len(labels) > 10
    plt.figure(figsize=(max(8, len(labels)*0.8) if not horizontal else 8,
                        5 if not horizontal else max(6, len(labels)*0.6)))
    plt.boxplot(data, vert=not horizontal, showmeans=True)
    if horizontal:
        plt.yticks(range(1, len(labels)+1), labels)
        plt.xlabel("Humedad (%)"); plt.ylabel("Sensor")
    else:
        plt.xticks(range(1, len(labels)+1), labels, rotation=60)
        plt.ylabel("Humedad (%)"); plt.xlabel("Sensor")
    plt.title("Boxplot de Voltaje por Sensor")
    plt.grid(True, axis="y" if not horizontal else "x")
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150); plt.show()