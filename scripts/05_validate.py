"""
05_validate.py
==============
Validación de calidad de datos post-carga para el proyecto Big Data Entel Perú 2026.
Verifica integridad, consistencia y rangos válidos sobre las colecciones MongoDB.

Validaciones ejecutadas:
    - Cero duplicados por ID en cada colección
    - Cero nulos en campos críticos (id_reclamo, id_cliente, fecha_apertura, tipo_reclamo)
    - Rangos válidos de fechas (no anteriores a 2020, no futuras)
    - Rangos válidos de duracion_horas (entre 0.1 y 8760 horas = 1 año)
    - Rangos válidos de csat_score (entre 1.0 y 5.0)
    - Tipos de reclamo dentro del catálogo estándar
    - Consistencia referencial: reclamos apuntan a clientes existentes (muestreo)
    - Conteos mínimos esperados por colección

Autor: Choque Martinez Gabriel / Jacobo Pachas Giancarlo
Curso: Big Data y Analytics — UPSJB VIII Ciclo 2026
"""

import os
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv
 
# ─────────────────────────────────────────────────────────────
# Configuración inicial
# ─────────────────────────────────────────────────────────────
load_dotenv()

MONGO_URI     = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "entel_bigdata")

# Valores válidos según catálogo del proyecto
TIPOS_RECLAMO_VALIDOS  = {"facturación", "cobertura", "velocidad", "portabilidad", "equipos", "otro"}
CANALES_INGRESO_VALIDOS = {"call_center", "app_movil", "portal_web", "redes_sociales", "tienda_fisica", "desconocido"}
FECHA_MINIMA = datetime(2020, 1, 1)
FECHA_MAXIMA = datetime.now()

# Contadores globales del reporte
resultados = {"PASS": 0, "WARN": 0, "FAIL": 0}


def log_resultado(regla: str, estado: str, detalle: str) -> None:
    """
    Registra el resultado de una validación con formato estandarizado.

    Args:
        regla (str): Nombre de la regla validada.
        estado (str): Estado del resultado — PASS, WARN o FAIL.
        detalle (str): Descripción del resultado con valores concretos.
    """
    icono = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}.get(estado, "?")
    resultados[estado] = resultados.get(estado, 0) + 1
    print(f"  {icono} [{estado}] {regla}: {detalle}")


# ─────────────────────────────────────────────────────────────
# Validaciones
# ─────────────────────────────────────────────────────────────

def validar_conteos(db) -> None:
    """Verifica que cada colección tiene el volumen mínimo esperado."""
    print("\n[1] VALIDACIÓN DE CONTEOS MÍNIMOS")
    print("-" * 45)

    esperados = {
        "reclamos":              140000,
        "clientes":              49000,
        "logs_app":              190000,
        "atenciones_callcenter": 29000,
    }

    for coleccion, minimo in esperados.items():
        try:
            conteo = db[coleccion].count_documents({})
            if conteo >= minimo:
                log_resultado(f"Conteo {coleccion}", "PASS", f"{conteo:,} documentos (mínimo: {minimo:,})")
            elif conteo > minimo * 0.9:
                log_resultado(f"Conteo {coleccion}", "WARN", f"{conteo:,} documentos — ligeramente bajo (mínimo: {minimo:,})")
            else:
                log_resultado(f"Conteo {coleccion}", "FAIL", f"{conteo:,} documentos — MUY bajo (mínimo: {minimo:,})")
        except Exception as e:
            log_resultado(f"Conteo {coleccion}", "FAIL", f"Error al consultar: {e}")


def validar_duplicados(db) -> None:
    """Verifica ausencia de duplicados por campo ID en cada colección."""
    print("\n[2] VALIDACIÓN DE DUPLICADOS")
    print("-" * 45)

    verificaciones = [
        ("reclamos",              "id_reclamo"),
        ("clientes",              "id_cliente"),
        ("logs_app",              "id_log"),
        ("atenciones_callcenter", "id_atencion"),
    ]

    for nombre_col, campo_id in verificaciones:
        try:
            # Detectar duplicados con aggregation pipeline
            pipeline = [
                {"$group": {"_id": f"${campo_id}", "conteo": {"$sum": 1}}},
                {"$match": {"conteo": {"$gt": 1}}},
                {"$count": "total_duplicados"}
            ]
            resultado = list(db[nombre_col].aggregate(pipeline))
            duplicados = resultado[0]["total_duplicados"] if resultado else 0

            if duplicados == 0:
                log_resultado(f"Duplicados en {nombre_col}.{campo_id}", "PASS", "0 duplicados")
            else:
                log_resultado(f"Duplicados en {nombre_col}.{campo_id}", "FAIL", f"{duplicados:,} IDs duplicados encontrados")
        except Exception as e:
            log_resultado(f"Duplicados {nombre_col}", "FAIL", f"Error: {e}")


def validar_nulos_criticos(db) -> None:
    """Verifica ausencia de nulos en campos críticos de la colección reclamos."""
    print("\n[3] VALIDACIÓN DE NULOS EN CAMPOS CRÍTICOS")
    print("-" * 45)

    campos_criticos = ["id_reclamo", "id_cliente", "fecha_apertura", "tipo_reclamo", "canal_ingreso"]

    for campo in campos_criticos:
        try:
            nulos = db["reclamos"].count_documents({campo: None})
            if nulos == 0:
                log_resultado(f"Nulos en reclamos.{campo}", "PASS", "0 nulos")
            else:
                log_resultado(f"Nulos en reclamos.{campo}", "FAIL", f"{nulos:,} registros con valor nulo")
        except Exception as e:
            log_resultado(f"Nulos {campo}", "FAIL", f"Error: {e}")


def validar_rangos_fechas(db) -> None:
    """Verifica que las fechas de apertura estén dentro del rango esperado."""
    print("\n[4] VALIDACIÓN DE RANGOS DE FECHAS")
    print("-" * 45)

    try:
        # Fechas anteriores al mínimo permitido
        anteriores = db["reclamos"].count_documents({
            "fecha_apertura": {"$lt": FECHA_MINIMA}
        })
        if anteriores == 0:
            log_resultado("Fechas anteriores a 2020", "PASS", "0 registros")
        else:
            log_resultado("Fechas anteriores a 2020", "WARN", f"{anteriores:,} registros con fecha anterior a 2020-01-01")

        # Fechas futuras
        futuras = db["reclamos"].count_documents({
            "fecha_apertura": {"$gt": FECHA_MAXIMA}
        })
        if futuras == 0:
            log_resultado("Fechas futuras", "PASS", "0 registros")
        else:
            log_resultado("Fechas futuras", "FAIL", f"{futuras:,} registros con fecha_apertura en el futuro")

        # Fecha_cierre anterior a fecha_apertura (lógicamente imposible)
        invertidas = db["reclamos"].count_documents({
            "$expr": {"$lt": ["$fecha_cierre", "$fecha_apertura"]}
        })
        if invertidas == 0:
            log_resultado("Fechas invertidas (cierre < apertura)", "PASS", "0 registros")
        else:
            log_resultado("Fechas invertidas", "FAIL", f"{invertidas:,} registros con fecha_cierre < fecha_apertura")

    except Exception as e:
        log_resultado("Rangos de fechas", "FAIL", f"Error: {e}")


def validar_rangos_numericos(db) -> None:
    """Verifica que duracion_horas y csat_score estén en rangos válidos."""
    print("\n[5] VALIDACIÓN DE RANGOS NUMÉRICOS")
    print("-" * 45)

    try:
        # duracion_horas: entre 0.1 y 8760 (1 año en horas)
        fuera_rango = db["reclamos"].count_documents({
            "$or": [
                {"duracion_horas": {"$lte": 0}},
                {"duracion_horas": {"$gt": 8760}},
                {"duracion_horas": None}
            ]
        })
        if fuera_rango == 0:
            log_resultado("duracion_horas en rango válido (0.1-8760h)", "PASS", "todos los registros válidos")
        else:
            log_resultado("duracion_horas fuera de rango", "WARN", f"{fuera_rango:,} registros fuera de rango")

        # csat_score: entre 1.0 y 5.0
        csat_invalido = db["reclamos"].count_documents({
            "csat_score": {"$not": {"$gte": 1.0, "$lte": 5.0}}
        })
        if csat_invalido == 0:
            log_resultado("csat_score en rango válido (1-5)", "PASS", "todos los registros válidos")
        else:
            log_resultado("csat_score fuera de rango", "WARN", f"{csat_invalido:,} registros con CSAT fuera de [1,5]")

    except Exception as e:
        log_resultado("Rangos numéricos", "FAIL", f"Error: {e}")


def validar_catalogo_tipos(db) -> None:
    """Verifica que todos los tipos de reclamo pertenecen al catálogo estándar."""
    print("\n[6] VALIDACIÓN DE CATÁLOGO DE TIPOS")
    print("-" * 45)

    try:
        # Obtener valores únicos de tipo_reclamo en la colección
        tipos_encontrados = db["reclamos"].distinct("tipo_reclamo")
        tipos_invalidos = [t for t in tipos_encontrados if t not in TIPOS_RECLAMO_VALIDOS and t is not None]

        if not tipos_invalidos:
            log_resultado("Tipos de reclamo en catálogo", "PASS",
                          f"valores encontrados: {sorted(tipos_encontrados)}")
        else:
            log_resultado("Tipos de reclamo fuera de catálogo", "FAIL",
                          f"valores no reconocidos: {tipos_invalidos}")

        # Verificar canales de ingreso
        canales_encontrados = db["reclamos"].distinct("canal_ingreso")
        canales_invalidos = [c for c in canales_encontrados if c not in CANALES_INGRESO_VALIDOS and c is not None]

        if not canales_invalidos:
            log_resultado("Canales de ingreso en catálogo", "PASS",
                          f"valores encontrados: {sorted([c for c in canales_encontrados if c][:5])}")
        else:
            log_resultado("Canales fuera de catálogo", "WARN", f"valores no reconocidos: {canales_invalidos}")

    except Exception as e:
        log_resultado("Catálogo de tipos", "FAIL", f"Error: {e}")


def validar_consistencia_referencial(db) -> None:
    """
    Verifica consistencia referencial entre reclamos y clientes
    mediante muestreo de 100 registros aleatorios.
    """
    print("\n[7] VALIDACIÓN DE CONSISTENCIA REFERENCIAL (muestreo)")
    print("-" * 45)

    try:
        # Tomar muestra aleatoria de 100 reclamos y verificar que sus clientes existen
        muestra = list(db["reclamos"].aggregate([
            {"$sample": {"size": 100}},
            {"$project": {"_id": 0, "id_cliente": 1}}
        ]))

        ids_muestra = [doc["id_cliente"] for doc in muestra if doc.get("id_cliente")]
        ids_en_clientes = db["clientes"].distinct("id_cliente", {"id_cliente": {"$in": ids_muestra}})

        huerfanos = len(set(ids_muestra) - set(ids_en_clientes))

        if huerfanos == 0:
            log_resultado("Consistencia referencial reclamos→clientes", "PASS",
                          f"100% de la muestra tiene cliente existente")
        elif huerfanos <= 5:
            log_resultado("Consistencia referencial reclamos→clientes", "WARN",
                          f"{huerfanos} reclamos en la muestra sin cliente (aceptable en datos sintéticos)")
        else:
            log_resultado("Consistencia referencial reclamos→clientes", "FAIL",
                          f"{huerfanos} de 100 reclamos en la muestra sin cliente existente")

    except Exception as e:
        log_resultado("Consistencia referencial", "FAIL", f"Error: {e}")


def resumen_kpis_rapidos(db) -> None:
    """Calcula y muestra KPIs básicos como verificación de negocio."""
    print("\n[8] KPIs DE VERIFICACIÓN RÁPIDA")
    print("-" * 45)

    try:
        # TTR promedio
        ttr = list(db["reclamos"].aggregate([
            {"$group": {"_id": None, "ttr": {"$avg": "$duracion_horas"}}}
        ]))
        ttr_val = round(ttr[0]["ttr"], 2) if ttr else 0
        print(f"  → TTR promedio global:          {ttr_val} horas")

        # FCR: porcentaje resuelto en primer contacto
        total      = db["reclamos"].count_documents({})
        primer_c   = db["reclamos"].count_documents({"resuelto_primer_contacto": True})
        fcr_pct    = round((primer_c / total * 100), 2) if total > 0 else 0
        print(f"  → FCR (primer contacto):        {fcr_pct}% ({primer_c:,} de {total:,})")

        # Distribución por canal (top 3)
        top_canales = list(db["reclamos"].aggregate([
            {"$group": {"_id": "$canal_ingreso", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": 3}
        ]))
        print(f"  → Top 3 canales de ingreso:")
        for c in top_canales:
            pct = round(c["total"] / total * 100, 1)
            print(f"      {c['_id']:<20} {c['total']:>8,} ({pct}%)")

        # CSAT promedio
        csat = list(db["reclamos"].aggregate([
            {"$match": {"csat_score": {"$ne": None}}},
            {"$group": {"_id": None, "csat_avg": {"$avg": "$csat_score"}}}
        ]))
        csat_val = round(csat[0]["csat_avg"], 2) if csat else 0
        print(f"  → CSAT promedio:               {csat_val}/5.0")

    except Exception as e:
        print(f"  [ERROR] Error en KPIs rápidos: {e}")


# ─────────────────────────────────────────────────────────────
# Función principal
# ─────────────────────────────────────────────────────────────
def main():
    """
    Función principal que ejecuta el conjunto completo de validaciones
    de calidad y genera el reporte final de resultados.
    """
    print("=" * 55)
    print("VALIDACIÓN DE CALIDAD — ENTEL PERÚ 2026")
    print(f"Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        cliente.server_info()
        db = cliente[MONGO_DB_NAME]
        print(f"[OK] Conectado a MongoDB — Base de datos: {MONGO_DB_NAME}")

        validar_conteos(db)
        validar_duplicados(db)
        validar_nulos_criticos(db)
        validar_rangos_fechas(db)
        validar_rangos_numericos(db)
        validar_catalogo_tipos(db)
        validar_consistencia_referencial(db)
        resumen_kpis_rapidos(db)

        # Reporte final
        print("\n" + "=" * 55)
        print("RESULTADO FINAL DE VALIDACIÓN")
        print("=" * 55)
        total_checks = sum(resultados.values())
        print(f"  ✓ PASS: {resultados.get('PASS', 0):>3} / {total_checks}")
        print(f"  ⚠ WARN: {resultados.get('WARN', 0):>3} / {total_checks}")
        print(f"  ✗ FAIL: {resultados.get('FAIL', 0):>3} / {total_checks}")

        if resultados.get("FAIL", 0) == 0:
            print("\n[✓] VALIDACIÓN EXITOSA — Pipeline ETL listo para Semana 3")
        else:
            print(f"\n[✗] ATENCIÓN — {resultados['FAIL']} validaciones fallaron. Revisar logs.")
        print("=" * 55)

    except ConnectionFailure as e:
        print(f"[ERROR] No se pudo conectar a MongoDB: {e}")
        raise
    except Exception as e:
        print(f"[ERROR CRÍTICO] Validación fallida: {e}")
        raise
    finally:
        if 'cliente' in locals():
            cliente.close()


if __name__ == "__main__":
    main()