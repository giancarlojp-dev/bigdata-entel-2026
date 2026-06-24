"""
03_transform.py
===============
Fase de Transformación del pipeline ETL para el proyecto Big Data Entel Perú 2026.
Aplica reglas de limpieza, normalización y enriquecimiento sobre los datos crudos
usando Apache Spark con PySpark como motor de procesamiento distribuido.

Reglas aplicadas:
    - Eliminación de duplicados por id_reclamo
    - Estandarización de fechas a formato YYYY-MM-DD HH:MM:SS
    - Normalización de tipos de reclamo al catálogo estándar
    - Imputación de valores nulos en canal_ingreso → "desconocido"
    - Eliminación de registros sin id_cliente o sin fecha_apertura
    - Cálculo de duracion_horas (TTR)
    - Cálculo de resuelto_primer_contacto
    - Enriquecimiento: join reclamos ← clientes

Autor: Choque Martinez Gabriel / Jacobo Pachas Giancarlo
Curso: Big Data y Analytics — UPSJB VIII Ciclo 2026
"""

import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# Configuración inicial
# ─────────────────────────────────────────────────────────────
load_dotenv()

DATA_PROCESSED_PATH = os.getenv("DATA_PROCESSED_PATH", "data/processed")

# Catálogo estándar de tipos de reclamo
# Mapea variantes de texto libre al catálogo oficial del proyecto
CATALOGO_TIPO_RECLAMO = {
    # Facturación y cobro
    "facturacion":      "facturación",
    "facturación":      "facturación",
    "cobro":            "facturación",
    "cobro indebido":   "facturación",
    "cargo":            "facturación",
    # Cobertura y señal
    "cobertura":        "cobertura",
    "señal":            "cobertura",
    "sin señal":        "cobertura",
    "sin cobertura":    "cobertura",
    # Velocidad de internet
    "velocidad":        "velocidad",
    "internet lento":   "velocidad",
    "ancho de banda":   "velocidad",
    # Portabilidad
    "portabilidad":     "portabilidad",
    "numero portado":   "portabilidad",
    # Equipos y dispositivos
    "equipos":          "equipos",
    "dispositivo":      "equipos",
    "chip":             "equipos",
    "sim":              "equipos",
    # Otros
    "otro":             "otro",
    "otros":            "otro",
    "general":          "otro",
}


def crear_spark_session() -> SparkSession:
    """
    Crea y configura la sesión de Apache Spark para el procesamiento local.
    Ajusta la memoria disponible según el entorno académico.

    Returns:
        SparkSession: Sesión activa de Spark.
    """
    print("[INFO] Iniciando sesión de Apache Spark...")
    spark = (
        SparkSession.builder
        .appName("EntelBigData_Transform_2026")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    # Reducir verbosidad de logs de Spark en consola
    spark.sparkContext.setLogLevel("ERROR")
    print(f"[OK] Spark iniciado — versión: {spark.version}")
    return spark


# ─────────────────────────────────────────────────────────────
# Funciones de transformación
# ─────────────────────────────────────────────────────────────

def transformar_reclamos(spark: SparkSession) -> None:
    """
    Aplica el conjunto completo de transformaciones sobre el dataset de reclamos:
    deduplicación, normalización de fechas, catálogo de tipos, campos derivados
    y enriquecimiento con datos de clientes mediante join.

    Args:
        spark (SparkSession): Sesión activa de Spark.
    """
    print("\n" + "=" * 55)
    print("TRANSFORMANDO: reclamos")
    print("=" * 55)

    ruta_entrada = os.path.join(DATA_PROCESSED_PATH, "reclamos_raw.parquet")
    ruta_clientes = os.path.join(DATA_PROCESSED_PATH, "clientes_raw.parquet")
    ruta_salida   = os.path.join(DATA_PROCESSED_PATH, "reclamos", "reclamos_clean.parquet")

    try:
        # --- Lectura ---
        print(f"[INFO] Leyendo: {ruta_entrada}")
        df = spark.read.parquet(ruta_entrada)
        total_inicial = df.count()
        print(f"[INFO] Registros iniciales: {total_inicial:,}")

        # ── Regla 1: Eliminar duplicados por id_reclamo ──────────────
        df = df.dropDuplicates(["id_reclamo"])
        tras_dedup = df.count()
        print(f"[R1] Deduplicación → {total_inicial - tras_dedup:,} duplicados eliminados")

        # ── Regla 2: Eliminar registros sin id_cliente o fecha_apertura ─
        df = df.filter(
            F.col("id_cliente").isNotNull() &
            F.col("fecha_apertura").isNotNull()
        )
        tras_nulos = df.count()
        print(f"[R2] Registros sin id_cliente/fecha_apertura eliminados: {tras_dedup - tras_nulos:,}")

        # ── Regla 3: Estandarizar fechas a TimestampType ─────────────
        df = df.withColumn(
            "fecha_apertura",
            F.to_timestamp(F.col("fecha_apertura"), "yyyy-MM-dd HH:mm:ss")
        ).withColumn(
            "fecha_cierre",
            F.to_timestamp(F.col("fecha_cierre"), "yyyy-MM-dd HH:mm:ss")
        )

        # Eliminar registros con fechas inválidas tras la conversión
        df = df.filter(
            F.col("fecha_apertura").isNotNull() &
            F.col("fecha_cierre").isNotNull()
        )

        # Eliminar reclamos con fecha_cierre anterior a fecha_apertura (datos corruptos)
        df = df.filter(F.col("fecha_cierre") >= F.col("fecha_apertura"))
        print(f"[R3] Fechas estandarizadas a TimestampType")

        # ── Regla 4: Normalizar tipos de reclamo al catálogo estándar ─
        # Crear mapa de normalización desde el diccionario Python
        catalogo_expr = F.create_map(
            *[item for par in
              [(F.lit(k), F.lit(v)) for k, v in CATALOGO_TIPO_RECLAMO.items()]
              for item in par]
        )
        df = df.withColumn(
            "tipo_reclamo",
            F.coalesce(
                catalogo_expr[F.lower(F.trim(F.col("tipo_reclamo")))],
                F.lit("otro")
            )
        )
        print(f"[R4] Tipos de reclamo normalizados al catálogo estándar")

        # ── Regla 5: Imputar nulos en canal_ingreso → "desconocido" ──
        df = df.withColumn(
            "canal_ingreso",
            F.when(
                F.col("canal_ingreso").isNull() | (F.trim(F.col("canal_ingreso")) == ""),
                F.lit("desconocido")
            ).otherwise(F.col("canal_ingreso"))
        )
        print(f"[R5] Nulos en canal_ingreso → 'desconocido'")

        # ── Regla 6: Calcular duracion_horas (TTR) ───────────────────
        # Diferencia en segundos entre cierre y apertura, convertida a horas
        df = df.withColumn(
            "duracion_horas",
            F.round(
                (F.unix_timestamp("fecha_cierre") - F.unix_timestamp("fecha_apertura"))
                / 3600.0,
                2
            ).cast(DoubleType())
        )
        # Filtrar duraciones negativas o cero (datos inconsistentes)
        df = df.filter(F.col("duracion_horas") > 0)
        print(f"[R6] Campo duracion_horas calculado (TTR en horas)")

        # ── Regla 7: Calcular resuelto_primer_contacto ────────────────
        df = df.withColumn(
            "resuelto_primer_contacto",
            (F.col("num_reescalamientos") == 0).cast(BooleanType())
        )
        print(f"[R7] Campo resuelto_primer_contacto calculado")

        # ── Regla 8: Enriquecimiento con dimensión clientes ───────────
        print(f"[INFO] Leyendo clientes para join: {ruta_clientes}")
        df_clientes = spark.read.parquet(ruta_clientes)

        # Seleccionar solo campos necesarios del cliente para enriquecer
        df_clientes_dim = df_clientes.select(
            F.col("id_cliente"),
            F.col("nombre").alias("nombre_cliente"),
            F.col("tipo_plan").alias("tipo_plan_cliente"),
            F.col("region").alias("region_cliente"),
            F.col("fecha_alta").alias("fecha_alta_cliente")
        )

        # Left join: se mantienen todos los reclamos, se enriquecen los que tienen cliente
        df = df.join(df_clientes_dim, on="id_cliente", how="left")
        print(f"[R8] Join con clientes completado — campos añadidos: nombre_cliente, tipo_plan_cliente")

        # ── Guardar resultado ─────────────────────────────────────────
        total_final = df.count()
        print(f"\n[INFO] Registros finales tras transformación: {total_final:,}")
        print(f"[INFO] Registros eliminados en total: {total_inicial - total_final:,}")

        os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
        df.write.mode("overwrite").parquet(ruta_salida)
        print(f"[GUARDADO] {ruta_salida}")

    except Exception as e:
        print(f"[ERROR] Transformación de reclamos fallida: {e}")
        raise


def transformar_clientes(spark: SparkSession) -> None:
    """
    Aplica limpieza básica al dataset de clientes:
    deduplicación por id_cliente y eliminación de registros sin campos críticos.

    Args:
        spark (SparkSession): Sesión activa de Spark.
    """
    print("\n" + "=" * 55)
    print("TRANSFORMANDO: clientes")
    print("=" * 55)

    ruta_entrada = os.path.join(DATA_PROCESSED_PATH, "clientes_raw.parquet")
    ruta_salida  = os.path.join(DATA_PROCESSED_PATH, "clientes", "clientes_clean.parquet")

    try:
        df = spark.read.parquet(ruta_entrada)
        print(f"[INFO] Registros iniciales: {df.count():,}")

        # Deduplicar por id_cliente
        df = df.dropDuplicates(["id_cliente"])

        # Eliminar registros sin id_cliente o nombre
        df = df.filter(
            F.col("id_cliente").isNotNull() &
            F.col("nombre").isNotNull()
        )

        # Estandarizar texto: mayúsculas en region y departamento
        df = df.withColumn("region", F.trim(F.col("region")))
        df = df.withColumn("departamento", F.trim(F.col("departamento")))
        df = df.withColumn("tipo_plan", F.lower(F.trim(F.col("tipo_plan"))))

        total_final = df.count()
        print(f"[INFO] Registros finales: {total_final:,}")

        os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
        df.write.mode("overwrite").parquet(ruta_salida)
        print(f"[GUARDADO] {ruta_salida}")

    except Exception as e:
        print(f"[ERROR] Transformación de clientes fallida: {e}")
        raise


def transformar_logs_app(spark: SparkSession) -> None:
    """
    Aplica limpieza al dataset de logs de la app móvil:
    deduplicación, estandarización de timestamps y filtrado de registros sin cliente.

    Args:
        spark (SparkSession): Sesión activa de Spark.
    """
    print("\n" + "=" * 55)
    print("TRANSFORMANDO: logs_app")
    print("=" * 55)

    ruta_entrada = os.path.join(DATA_PROCESSED_PATH, "logs_app_raw.parquet")
    ruta_salida  = os.path.join(DATA_PROCESSED_PATH, "logs_app", "logs_app_clean.parquet")

    try:
        df = spark.read.parquet(ruta_entrada)
        print(f"[INFO] Registros iniciales: {df.count():,}")

        # Deduplicar por id_log
        df = df.dropDuplicates(["id_log"])

        # Estandarizar timestamp
        df = df.withColumn(
            "timestamp",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        )

        # Eliminar registros sin id_cliente o timestamp
        df = df.filter(
            F.col("id_cliente").isNotNull() &
            F.col("timestamp").isNotNull()
        )

        # Normalizar nombre de evento a minúsculas sin espacios extra
        df = df.withColumn("evento", F.lower(F.trim(F.col("evento"))))

        total_final = df.count()
        print(f"[INFO] Registros finales: {total_final:,}")

        os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
        df.write.mode("overwrite").parquet(ruta_salida)
        print(f"[GUARDADO] {ruta_salida}")

    except Exception as e:
        print(f"[ERROR] Transformación de logs_app fallida: {e}")
        raise


def transformar_call_center(spark: SparkSession) -> None:
    """
    Aplica limpieza al dataset de call center:
    deduplicación, estandarización de fechas y filtrado de duraciones inválidas.

    Args:
        spark (SparkSession): Sesión activa de Spark.
    """
    print("\n" + "=" * 55)
    print("TRANSFORMANDO: call_center")
    print("=" * 55)

    ruta_entrada = os.path.join(DATA_PROCESSED_PATH, "call_center_raw.parquet")
    ruta_salida  = os.path.join(DATA_PROCESSED_PATH, "call_center", "call_center_clean.parquet")

    try:
        df = spark.read.parquet(ruta_entrada)
        print(f"[INFO] Registros iniciales: {df.count():,}")

        # Deduplicar por id_atencion
        df = df.dropDuplicates(["id_atencion"])

        # Estandarizar fecha_llamada
        df = df.withColumn(
            "fecha_llamada",
            F.to_timestamp(F.col("fecha_llamada"), "yyyy-MM-dd HH:mm:ss")
        )

        # Eliminar tiempos de espera o atención negativos (datos corruptos)
        df = df.filter(
            (F.col("tiempo_espera_seg") >= 0) &
            (F.col("tiempo_atencion_seg") > 0)
        )

        # Calcular campo derivado: tiempo total de la llamada en minutos
        df = df.withColumn(
            "tiempo_total_min",
            F.round(
                (F.col("tiempo_espera_seg") + F.col("tiempo_atencion_seg")) / 60.0,
                2
            )
        )

        # Normalizar resultado a minúsculas
        df = df.withColumn("resultado", F.lower(F.trim(F.col("resultado"))))

        total_final = df.count()
        print(f"[INFO] Registros finales: {total_final:,}")

        os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
        df.write.mode("overwrite").parquet(ruta_salida)
        print(f"[GUARDADO] {ruta_salida}")

    except Exception as e:
        print(f"[ERROR] Transformación de call_center fallida: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# Función principal
# ─────────────────────────────────────────────────────────────
def main():
    """
    Función principal que orquesta la transformación completa de todos
    los datasets del proyecto Big Data Entel Perú 2026 con PySpark.
    """
    print("=" * 55)
    print("FASE DE TRANSFORMACIÓN — PYSPARK — ENTEL PERÚ 2026")
    print("=" * 55)

    spark = crear_spark_session()

    try:
        transformar_reclamos(spark)
        transformar_clientes(spark)
        transformar_logs_app(spark)
        transformar_call_center(spark)

        print("\n" + "=" * 55)
        print("RESUMEN — ARCHIVOS PARQUET PROCESADOS:")
        carpetas = [
            "reclamos/reclamos_clean.parquet",
            "clientes/clientes_clean.parquet",
            "logs_app/logs_app_clean.parquet",
            "call_center/call_center_clean.parquet",
        ]
        for carpeta in carpetas:
            ruta = os.path.join(DATA_PROCESSED_PATH, carpeta)
            if os.path.exists(ruta):
                print(f"  ✓ {carpeta}")
            else:
                print(f"  ✗ {carpeta} — NO encontrado")
        print("=" * 55)
        print("[COMPLETADO] Transformación PySpark finalizada.")

    except Exception as e:
        print(f"\n[ERROR CRÍTICO] La transformación falló: {e}")
        raise
    finally:
        spark.stop()
        print("[INFO] Sesión Spark cerrada.")


if __name__ == "__main__":
    main()