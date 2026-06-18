"""
02_extract.py
=============
Fase de Extracción del pipeline ETL para el proyecto Big Data Entel Perú 2026.
Lee todas las fuentes de datos crudos (CSV de OSIPTEL y datos sintéticos)
y las convierte al formato Parquet para procesamiento eficiente con PySpark.

Fuentes procesadas:
    - data/raw/osiptel/reclamos_osiptel_2024.csv    → Parquet
    - data/raw/synthetic/clientes.csv               → Parquet
    - data/raw/synthetic/reclamos.csv               → Parquet
    - data/raw/synthetic/logs_app.json              → Parquet
    - data/raw/synthetic/call_center.csv            → Parquet

Autor: Choque Martinez Gabriel / Jacobo Pachas Giancarlo
Curso: Big Data y Analytics — UPSJB VIII Ciclo 2026
"""

import os
import json
import pandas as pd
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# Configuración inicial
# ─────────────────────────────────────────────────────────────
load_dotenv()

DATA_RAW_PATH = os.getenv("DATA_RAW_PATH", "data/raw")
DATA_PROCESSED_PATH = os.getenv("DATA_PROCESSED_PATH", "data/processed")
os.makedirs(DATA_PROCESSED_PATH, exist_ok=True)


# ─────────────────────────────────────────────────────────────
# Función auxiliar: guardar como Parquet
# ─────────────────────────────────────────────────────────────
def guardar_parquet(df: pd.DataFrame, nombre: str) -> str:
    """
    Guarda un DataFrame en formato Parquet en la carpeta de datos procesados.

    Args:
        df (pd.DataFrame): DataFrame a guardar.
        nombre (str): Nombre del archivo Parquet (sin extensión).

    Returns:
        str: Ruta completa del archivo Parquet generado.
    """
    ruta = os.path.join(DATA_PROCESSED_PATH, f"{nombre}.parquet")
    try:
        df.to_parquet(ruta, index=False, engine="pyarrow", compression="snappy")
        print(f"[GUARDADO] {ruta} — {len(df):,} registros — {os.path.getsize(ruta) / 1024:.1f} KB")
        return ruta
    except Exception as e:
        print(f"[ERROR] No se pudo guardar {nombre}.parquet: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# Función 1: Extraer datos OSIPTEL
# ─────────────────────────────────────────────────────────────
def extraer_osiptel() -> pd.DataFrame | None:
    """
    Lee el CSV de reclamos reales descargado desde el repositorio de OSIPTEL.
    Si el archivo no existe, registra advertencia y retorna None.

    Returns:
        pd.DataFrame | None: DataFrame con datos de OSIPTEL o None si no existe.
    """
    ruta = os.path.join(DATA_RAW_PATH, "osiptel", "reclamos_osiptel_2024.csv")

    if not os.path.exists(ruta):
        print(f"[ADVERTENCIA] Archivo OSIPTEL no encontrado en: {ruta}")
        print("  → Descargarlo desde: https://repositorio.osiptel.gob.pe")
        print("  → Continuando con datos sintéticos únicamente...")
        return None

    try:
        print(f"[INFO] Leyendo datos OSIPTEL desde: {ruta}")
        # Intentar con codificación UTF-8, luego latin-1 si falla
        try:
            df = pd.read_csv(ruta, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(ruta, encoding="latin-1")

        print(f"[OK] OSIPTEL leído: {len(df):,} registros, {len(df.columns)} columnas")
        print(f"     Columnas: {list(df.columns)}")
        return df

    except Exception as e:
        print(f"[ERROR] Error al leer datos OSIPTEL: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# Función 2: Extraer clientes sintéticos
# ─────────────────────────────────────────────────────────────
def extraer_clientes() -> pd.DataFrame:
    """
    Lee el CSV de clientes sintéticos generado por 01_generate_data.py.

    Returns:
        pd.DataFrame: DataFrame con datos de clientes.
    """
    ruta = os.path.join(DATA_RAW_PATH, "synthetic", "clientes.csv")
    try:
        print(f"[INFO] Leyendo clientes desde: {ruta}")
        df = pd.read_csv(ruta, encoding="utf-8")
        print(f"[OK] Clientes leídos: {len(df):,} registros")
        return df
    except FileNotFoundError:
        print(f"[ERROR] Archivo no encontrado: {ruta}")
        print("  → Ejecutar primero: python scripts/01_generate_data.py")
        raise
    except Exception as e:
        print(f"[ERROR] Error al leer clientes: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# Función 3: Extraer reclamos sintéticos
# ─────────────────────────────────────────────────────────────
def extraer_reclamos() -> pd.DataFrame:
    """
    Lee el CSV de reclamos sintéticos generado por 01_generate_data.py.

    Returns:
        pd.DataFrame: DataFrame con datos de reclamos.
    """
    ruta = os.path.join(DATA_RAW_PATH, "synthetic", "reclamos.csv")
    try:
        print(f"[INFO] Leyendo reclamos desde: {ruta}")
        df = pd.read_csv(ruta, encoding="utf-8")
        print(f"[OK] Reclamos leídos: {len(df):,} registros")
        return df
    except FileNotFoundError:
        print(f"[ERROR] Archivo no encontrado: {ruta}")
        print("  → Ejecutar primero: python scripts/01_generate_data.py")
        raise
    except Exception as e:
        print(f"[ERROR] Error al leer reclamos: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# Función 4: Extraer logs de app móvil (JSON → DataFrame)
# ─────────────────────────────────────────────────────────────
def extraer_logs_app() -> pd.DataFrame:
    """
    Lee el archivo JSON de logs de la app móvil y lo convierte a DataFrame.
    El campo detalle_json (dict anidado) se serializa como string para Parquet.

    Returns:
        pd.DataFrame: DataFrame con logs de la app.
    """
    ruta = os.path.join(DATA_RAW_PATH, "synthetic", "logs_app.json")
    try:
        print(f"[INFO] Leyendo logs de app desde: {ruta}")
        with open(ruta, "r", encoding="utf-8") as f:
            logs = json.load(f)

        df = pd.DataFrame(logs)

        # Serializar campo anidado a string para compatibilidad con Parquet
        df["detalle_json"] = df["detalle_json"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else "{}"
        )

        print(f"[OK] Logs de app leídos: {len(df):,} registros")
        return df

    except FileNotFoundError:
        print(f"[ERROR] Archivo no encontrado: {ruta}")
        print("  → Ejecutar primero: python scripts/01_generate_data.py")
        raise
    except Exception as e:
        print(f"[ERROR] Error al leer logs de app: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# Función 5: Extraer registros de call center
# ─────────────────────────────────────────────────────────────
def extraer_call_center() -> pd.DataFrame:
    """
    Lee el CSV de atenciones de call center generado por 01_generate_data.py.

    Returns:
        pd.DataFrame: DataFrame con datos de call center.
    """
    ruta = os.path.join(DATA_RAW_PATH, "synthetic", "call_center.csv")
    try:
        print(f"[INFO] Leyendo call center desde: {ruta}")
        df = pd.read_csv(ruta, encoding="utf-8")
        print(f"[OK] Call center leído: {len(df):,} registros")
        return df
    except FileNotFoundError:
        print(f"[ERROR] Archivo no encontrado: {ruta}")
        print("  → Ejecutar primero: python scripts/01_generate_data.py")
        raise
    except Exception as e:
        print(f"[ERROR] Error al leer call center: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# Función principal
# ─────────────────────────────────────────────────────────────
def main():
    """
    Función principal que coordina la extracción de todas las fuentes
    y su conversión al formato Parquet en data/processed/.
    """
    print("=" * 60)
    print("FASE DE EXTRACCIÓN — PIPELINE ETL ENTEL PERÚ 2026")
    print("=" * 60)

    archivos_generados = []

    try:
        # 1. Datos reales OSIPTEL
        df_osiptel = extraer_osiptel()
        if df_osiptel is not None:
            ruta = guardar_parquet(df_osiptel, "osiptel_reclamos_raw")
            archivos_generados.append(ruta)

        # 2. Clientes sintéticos
        df_clientes = extraer_clientes()
        ruta = guardar_parquet(df_clientes, "clientes_raw")
        archivos_generados.append(ruta)

        # 3. Reclamos sintéticos
        df_reclamos = extraer_reclamos()
        ruta = guardar_parquet(df_reclamos, "reclamos_raw")
        archivos_generados.append(ruta)

        # 4. Logs de app móvil
        df_logs = extraer_logs_app()
        ruta = guardar_parquet(df_logs, "logs_app_raw")
        archivos_generados.append(ruta)

        # 5. Call center
        df_callcenter = extraer_call_center()
        ruta = guardar_parquet(df_callcenter, "call_center_raw")
        archivos_generados.append(ruta)

        # Resumen
        print("\n" + "=" * 60)
        print("RESUMEN DE EXTRACCIÓN:")
        for archivo in archivos_generados:
            tamaño_kb = os.path.getsize(archivo) / 1024
            print(f"  ✓ {os.path.basename(archivo)} — {tamaño_kb:.1f} KB")
        print(f"\n  Total archivos Parquet: {len(archivos_generados)}")
        print(f"  Ruta: {DATA_PROCESSED_PATH}/")
        print("=" * 60)
        print("[COMPLETADO] Fase de extracción finalizada.")

    except Exception as e:
        print(f"\n[ERROR CRÍTICO] La extracción falló: {e}")
        raise


if __name__ == "__main__":
    main()