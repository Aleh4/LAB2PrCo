#comandos utiles para gestionar carpetas de entradas y salidas.
from pathlib import Path
import re

def Root(file=__file__) -> Path:
    """permite ubicar la carpeta ROOT del archivo de trabajo

    Args:
        file (codigo base)
    """
    return Path(file).resolve().parents[0]

def ensure_dirs(*dirs: Path) -> None:
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        

def safe_stem(p: Path) -> str:
    """Nombre seguro (sin espacios/acentos/símbolos raros) para salidas."""
    base = p.stem.strip().replace(" ", "_")
    base = re.sub(r"[^A-Za-z0-9_\-]+", "", base)
    return base or "archivo"

def detectar_delimitador(path: Path) -> str:
    """Detecta ';' o ',' a partir de la primera línea."""
    with path.open("r", encoding="utf-8", newline="") as f:
        head = f.readline()
    return ";" if head.count(";") > head.count(",") else ","

def list_raw_csvs(raw_dir: Path, pattern: str = "*.csv"):
    """Lista CSV crudos por patrón."""
    return sorted(raw_dir.glob(pattern), key=lambda p: p.name.lower())

def make_clean_name(p:Path) -> str:
    """
    Propone nombre 'limpio' a partir del crudo:
    - si contiene 'sucio'/'sucios' -> reemplaza por 'limpio'/'limpios'
    - si no, añade sufijo '_limpio'
    """
    nombre=p.stem
    if "sucio" in nombre:
        nombre = nombre.replace("sucios", "limpios").replace("sucio", "limpio")
    else:
        nombre = f"{nombre}_limpio"
    return f"{nombre}.csv"