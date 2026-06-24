"""
04_load.py
==========
Fase de Carga del pipeline ETL para el proyecto Big Data Entel Perú 2026.
Lee los archivos Parquet procesados e inserta los datos en las colecciones
de MongoDB, creando índices sobre los campos analíticos más consultados.

Colecciones cargadas:
    - reclamos              ← reclamos_clean.parquet
    - clientes              ← clientes_clean.parquet
    - logs_app              ← logs_app_clean.parquet
    - atenciones_callcenter ← call_center_clean.parquet

Índices creados:
    - reclamos: fecha_apertura, canal_ingreso, region, tipo_reclamo, id_cliente
    - clientes: id_cliente (único), region
    - logs_app: id_cliente, timestamp, evento
    - atenciones_callcenter: id_cliente, fecha_llamada, resultado

Autor: Choque Martinez Gabriel / Jacobo Pachas Giancarlo
Curso: Big Data y Analytics — UPSJB VIII Ciclo 2026
"""

import os
import math
import pandas as pd
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError, ConnectionFailure
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# Configuración inicial
# ─────────────────────────────────────────────────────────────
load_dotenv()

MONGO_URI      = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/")
MONGO_DB_NAME  = os.getenv("MONGO_DB_NAME", "entel_bigdata")
DATA_PATH      = os.getenv("DATA_PROCESSED_PATH", "data/processed")

# Tamaño del lote para inserciones en MongoDB (optimiza memoria RAM)
BATCH_SIZE = 5000


def conectar_mongodb() -> MongoClient:
    """
    Establece conexión con MongoDB y verifica que el servidor responde.

    Returns:
        MongoClient: Cliente MongoDB activo.

    Raises:
        ConnectionFailure: Si MongoDB no está disponible.
    """
    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Verificar conectividad
        cliente.server_info()
        print(f"[OK] MongoDB conectado en: {MONGO_URI}")
        return cliente
    except ConnectionFailure as e:
        print(f"[ERROR] No se pudo conectar a MongoDB: {e}")
        print("  → Verificar que Docker está corriendo: docker ps")
        print("  → Verificar MONGO_URI en el archivo .env")
        raise


def insertar_en_lotes(coleccion, documentos: list, nombre: str) -> int:
    """
    Inserta una lista de documentos en una colección MongoDB en lotes
    para evitar timeouts con grandes volúmenes de datos.

    Args:
        coleccion: Colección de MongoDB destino.
        documentos (list): Lista de documentos a insertar.
        nombre (str): Nombre de la colección (para logs).

    Returns:
        int: Total de documentos insertados exitosamente.
    """
    total = len(documentos)
    num_lotes = math.ceil(total / BATCH_SIZE)
    insertados = 0

    print(f"[INFO] Insertando {total:,} documentos en {num_lotes} lotes de {BATCH_SIZE:,}...")

    for i in range(num_lotes):
        inicio = i * BATCH_SIZE
        fin    = min(inicio + BATCH_SIZE, total)
        lote   = documentos[inicio:fin]

        try:
            resultado = coleccion.insert_many(lote, ordered=False)
            insertados += len(resultado.inserted_ids)

            # Log de progreso cada 10 lotes
            if (i + 1) % 10 == 0 or (i + 1) == num_lotes:
                print(f"  → Lote {i+1}/{num_lotes} — {insertados:,}/{total:,} documentos")

        except BulkWriteError as bwe:
            # Registrar errores sin detener la carga (inserción parcial)
            escritos = bwe.details.get("nInserted", 0)
            insertados += escritos
            print(f"  [ADVERTENCIA] Lote {i+1}: {escritos} insertados, algunos con error (posibles duplicados)")

    return insertados


def parquet_a_documentos(ruta_parquet: str) -> list:
    """
    Lee un archivo Parquet y lo convierte a lista de diccionarios
    compatible con el formato de documentos MongoDB.
    Reemplaza valores NaN/NaT de Pandas por None (null en MongoDB).

    Args:
        ruta_parquet (str): Ruta al archivo Parquet.

    Returns:
        list: Lista de diccionarios listos para insertar en MongoDB.
    """
    print(f"[INFO] Leyendo Parquet: {ruta_parquet}")
    df = pd.read_parquet(ruta_parquet, engine="pyarrow")

    # Reemplazar NaN y NaT por None para compatibilidad con MongoDB
    df = df.where(pd.notnull(df), None)

    documentos = df.to_dict(orient="records")
    print(f"[OK] {len(documentos):,} documentos preparados para carga")
    return documentos


# ─────────────────────────────────────────────────────────────
# Funciones de carga por colección
# ─────────────────────────────────────────────────────────────

def cargar_reclamos(db) -> None:
    """
    Carga la colección 'reclamos' desde el Parquet procesado
    y crea índices sobre los campos más consultados para KPIs.

    Args:
        db: Base de datos MongoDB activa.
    """
    print("\n" + "=" * 55)
    print("CARGANDO COLECCIÓN: reclamos")
    print("=" * 55)

    ruta = os.path.join(DATA_PATH, "reclamos", "reclamos_clean.parquet")
    coleccion = db["reclamos"]

    try:
        # Limpiar colección si ya existe (permite re-ejecución sin duplicados)
        coleccion.drop()
        print("[INFO] Colección 'reclamos' limpiada para recarga")

        documentos = parquet_a_documentos(ruta)
        insertados = insertar_en_lotes(coleccion, documentos, "reclamos")
        print(f"[OK] Reclamos cargados: {insertados:,} documentos")

        # Crear índices para optimizar las consultas de KPIs
        print("[INFO] Creando índices en 'reclamos'...")
        coleccion.create_index([("fecha_apertura", ASCENDING)],  name="idx_fecha_apertura")
        coleccion.create_index([("canal_ingreso",  ASCENDING)],  name="idx_canal")
        coleccion.create_index([("region",         ASCENDING)],  name="idx_region")
        coleccion.create_index([("tipo_reclamo",   ASCENDING)],  name="idx_tipo_reclamo")
        coleccion.create_index([("id_cliente",     ASCENDING)],  name="idx_id_cliente")
        coleccion.create_index([("estado",         ASCENDING)],  name="idx_estado")
        # Índice compuesto para el KPI de volumen por canal y región
        coleccion.create_index(
            [("canal_ingreso", ASCENDING), ("region", ASCENDING), ("fecha_apertura", DESCENDING)],
            name="idx_canal_region_fecha"
        )
        print("[OK] Índices creados en 'reclamos': fecha_apertura, canal, region, tipo_reclamo, id_cliente, estado, compuesto")

    except Exception as e:
        print(f"[ERROR] Carga de reclamos fallida: {e}")
        raise


def cargar_clientes(db) -> None:
    """
    Carga la colección 'clientes' desde el Parquet procesado
    y crea índice único sobre id_cliente.

    Args:
        db: Base de datos MongoDB activa.
    """
    print("\n" + "=" * 55)
    print("CARGANDO COLECCIÓN: clientes")
    print("=" * 55)

    ruta = os.path.join(DATA_PATH, "clientes", "clientes_clean.parquet")
    coleccion = db["clientes"]

    try:
        coleccion.drop()
        print("[INFO] Colección 'clientes' limpiada para recarga")

        documentos = parquet_a_documentos(ruta)
        insertados = insertar_en_lotes(coleccion, documentos, "clientes")
        print(f"[OK] Clientes cargados: {insertados:,} documentos")

        # Índice único para garantizar integridad referencial
        print("[INFO] Creando índices en 'clientes'...")
        coleccion.create_index([("id_cliente", ASCENDING)], name="idx_id_cliente_unico", unique=True)
        coleccion.create_index([("region",     ASCENDING)], name="idx_region")
        coleccion.create_index([("tipo_plan",  ASCENDING)], name="idx_tipo_plan")
        print("[OK] Índices creados en 'clientes': id_cliente (único), region, tipo_plan")

    except Exception as e:
        print(f"[ERROR] Carga de clientes fallida: {e}")
        raise


def cargar_logs_app(db) -> None:
    """
    Carga la colección 'logs_app' desde el Parquet procesado
    y crea índices para consultas temporales y por cliente.

    Args:
        db: Base de datos MongoDB activa.
    """
    print("\n" + "=" * 55)
    print("CARGANDO COLECCIÓN: logs_app")
    print("=" * 55)

    ruta = os.path.join(DATA_PATH, "logs_app", "logs_app_clean.parquet")
    coleccion = db["logs_app"]

    try:
        coleccion.drop()
        print("[INFO] Colección 'logs_app' limpiada para recarga")

        documentos = parquet_a_documentos(ruta)
        insertados = insertar_en_lotes(coleccion, documentos, "logs_app")
        print(f"[OK] Logs app cargados: {insertados:,} documentos")

        print("[INFO] Creando índices en 'logs_app'...")
        coleccion.create_index([("id_cliente", ASCENDING)],              name="idx_id_cliente")
        coleccion.create_index([("timestamp",  DESCENDING)],             name="idx_timestamp")
        coleccion.create_index([("evento",     ASCENDING)],              name="idx_evento")
        coleccion.create_index([("id_cliente", ASCENDING), ("evento", ASCENDING)], name="idx_cliente_evento")
        print("[OK] Índices creados en 'logs_app': id_cliente, timestamp, evento, compuesto")

    except Exception as e:
        print(f"[ERROR] Carga de logs_app fallida: {e}")
        raise


def cargar_call_center(db) -> None:
    """
    Carga la colección 'atenciones_callcenter' desde el Parquet procesado
    y crea índices para análisis de tiempos de atención.

    Args:
        db: Base de datos MongoDB activa.
    """
    print("\n" + "=" * 55)
    print("CARGANDO COLECCIÓN: atenciones_callcenter")
    print("=" * 55)

    ruta = os.path.join(DATA_PATH, "call_center", "call_center_clean.parquet")
    coleccion = db["atenciones_callcenter"]

    try:
        coleccion.drop()
        print("[INFO] Colección 'atenciones_callcenter' limpiada para recarga")

        documentos = parquet_a_documentos(ruta)
        insertados = insertar_en_lotes(coleccion, documentos, "atenciones_callcenter")
        print(f"[OK] Atenciones call center cargadas: {insertados:,} documentos")

        print("[INFO] Creando índices en 'atenciones_callcenter'...")
        coleccion.create_index([("id_cliente",   ASCENDING)],  name="idx_id_cliente")
        coleccion.create_index([("fecha_llamada", DESCENDING)], name="idx_fecha_llamada")
        coleccion.create_index([("resultado",    ASCENDING)],  name="idx_resultado")
        coleccion.create_index([("agente",       ASCENDING)],  name="idx_agente")
        print("[OK] Índices creados en 'atenciones_callcenter': id_cliente, fecha_llamada, resultado, agente")

    except Exception as e:
        print(f"[ERROR] Carga de call_center fallida: {e}")
        raise


def verificar_carga(db) -> None:
    """
    Ejecuta consultas de verificación sobre MongoDB para confirmar
    que la carga fue exitosa. Imprime conteos por colección.

    Args:
        db: Base de datos MongoDB activa.
    """
    print("\n" + "=" * 55)
    print("VERIFICACIÓN DE CARGA EN MONGODB")
    print("=" * 55)

    colecciones = {
        "reclamos":              {"esperado_min": 140000},
        "clientes":              {"esperado_min": 49000},
        "logs_app":              {"esperado_min": 190000},
        "atenciones_callcenter": {"esperado_min": 29000},
    }

    for nombre, config in colecciones.items():
        try:
            conteo = db[nombre].count_documents({})
            estado = "✓" if conteo >= config["esperado_min"] else "⚠"
            print(f"  {estado} {nombre:<30} {conteo:>10,} documentos")

            # Consulta de muestra: primer documento de reclamos
            if nombre == "reclamos":
                muestra = db[nombre].find_one({}, {"_id": 0, "id_reclamo": 1, "tipo_reclamo": 1, "duracion_horas": 1})
                print(f"    → Muestra: {muestra}")

            # KPI rápido: promedio de duracion_horas (TTR)
            if nombre == "reclamos":
                pipeline_ttr = [
                    {"$group": {"_id": None, "ttr_promedio": {"$avg": "$duracion_horas"}}}
                ]
                resultado = list(db[nombre].aggregate(pipeline_ttr))
                if resultado:
                    ttr = round(resultado[0]["ttr_promedio"], 2)
                    print(f"    → TTR promedio global: {ttr} horas")

        except Exception as e:
            print(f"  ✗ {nombre}: Error en verificación — {e}")

    print("=" * 55)


# ─────────────────────────────────────────────────────────────
# Función principal
# ─────────────────────────────────────────────────────────────
def main():
    """
    Función principal que coordina la carga completa de los datos
    procesados hacia las colecciones de MongoDB.
    """
    print("=" * 55)
    print("FASE DE CARGA — MONGODB — ENTEL PERÚ 2026")
    print("=" * 55)

    try:
        cliente = conectar_mongodb()
        db = cliente[MONGO_DB_NAME]
        print(f"[INFO] Base de datos: {MONGO_DB_NAME}")

        cargar_reclamos(db)
        cargar_clientes(db)
        cargar_logs_app(db)
        cargar_call_center(db)
        verificar_carga(db)

        print("\n[COMPLETADO] Carga en MongoDB finalizada.")

    except Exception as e:
        print(f"\n[ERROR CRÍTICO] La carga falló: {e}")
        raise
    finally:
        if 'cliente' in locals():
            cliente.close()
            print("[INFO] Conexión MongoDB cerrada.")


if __name__ == "__main__":
    main()
