"""
01_generate_data.py
===================
Generación de datos sintéticos para el proyecto Big Data Entel Perú 2026.
Simula los registros operativos del CRM de Entel que no son de acceso público.

Datasets generados:
    - clientes.csv         → 50,000 registros
    - reclamos.csv         → 150,000 registros
    - logs_app.json        → 200,000 registros
    - call_center.csv      → 30,000 registros

Autor: Choque Martinez Gabriel / Jacobo Pachas Giancarlo
Curso: Big Data y Analytics — UPSJB VIII Ciclo 2026
"""

import os
import json
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from faker.providers import person, address, phone_number
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# Configuración inicial
# ─────────────────────────────────────────────────────────────
load_dotenv()

# Faker configurado para Perú (español latinoamericano)
fake = Faker("es_MX")
Faker.seed(42)
random.seed(42)

# Rutas de salida desde variables de entorno
DATA_RAW_PATH = os.getenv("DATA_RAW_PATH", "data/raw")
OUTPUT_PATH = os.path.join(DATA_RAW_PATH, "synthetic")
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Catálogos de dominio específicos para Entel Perú
REGIONES_PERU = [
    "Lima", "Arequipa", "La Libertad", "Piura", "Cusco",
    "Junín", "Lambayeque", "Áncash", "Loreto", "Ica",
    "San Martín", "Cajamarca", "Puno", "Huánuco", "Callao"
]

DEPARTAMENTOS_POR_REGION = {
    "Lima": ["Lima", "Lima Norte", "Lima Este", "Lima Sur", "Lima Centro"],
    "Arequipa": ["Arequipa", "Camaná", "Islay"],
    "La Libertad": ["Trujillo", "Pacasmayo", "Ascope"],
    "Piura": ["Piura", "Sullana", "Paita"],
    "Cusco": ["Cusco", "Urubamba", "Canchis"],
    "Junín": ["Huancayo", "Chanchamayo", "Satipo"],
    "Lambayeque": ["Chiclayo", "Ferreñafe", "Lambayeque"],
    "Áncash": ["Huaraz", "Chimbote", "Carhuaz"],
    "Loreto": ["Iquitos", "Requena", "Maynas"],
    "Ica": ["Ica", "Chincha", "Pisco", "Nazca"],
    "San Martín": ["Tarapoto", "Moyobamba", "Rioja"],
    "Cajamarca": ["Cajamarca", "Jaén", "Cutervo"],
    "Puno": ["Puno", "Juliaca", "Azángaro"],
    "Huánuco": ["Huánuco", "Leoncio Prado", "Ambo"],
    "Callao": ["Callao", "Bellavista", "La Punta"]
}

TIPOS_RECLAMO = [
    "facturación", "cobertura", "velocidad",
    "portabilidad", "equipos", "otro"
]

CANALES_INGRESO = [
    "call_center", "app_movil", "portal_web",
    "redes_sociales", "tienda_fisica"
]

TIPOS_PLAN = [
    "prepago", "postpago_basico", "postpago_premium",
    "empresarial", "hogar"
]

ESTADOS_RECLAMO = ["cerrado", "resuelto", "en_proceso", "escalado"]

EVENTOS_APP = [
    "login", "logout", "consulta_saldo", "recarga",
    "reporte_falla", "cambio_plan", "descarga_factura",
    "chat_soporte", "consulta_cobertura", "pago_factura"
]


# ─────────────────────────────────────────────────────────────
# Función 1: Generar clientes
# ─────────────────────────────────────────────────────────────
def generar_clientes(n: int = 50000) -> pd.DataFrame:
    """
    Genera un DataFrame con registros sintéticos de clientes de Entel Perú.

    Args:
        n (int): Número de registros a generar. Por defecto 50,000.

    Returns:
        pd.DataFrame: DataFrame con columnas de clientes.
    """
    print(f"[INFO] Generando {n:,} registros de clientes...")
    registros = []

    for i in range(1, n + 1):
        region = random.choice(REGIONES_PERU)
        departamento = random.choice(DEPARTAMENTOS_POR_REGION[region])
        fecha_alta = fake.date_between(start_date="-5y", end_date="today")

        registros.append({
            "id_cliente": f"CLI{i:06d}",
            "nombre": fake.name(),
            "dni": fake.numerify(text="########"),
            "telefono": fake.numerify(text="9########"),
            "email": fake.email(),
            "region": region,
            "departamento": departamento,
            "tipo_plan": random.choice(TIPOS_PLAN),
            "fecha_alta": fecha_alta.strftime("%Y-%m-%d"),
        })

        # Log de progreso cada 10,000 registros
        if i % 10000 == 0:
            print(f"  → {i:,} clientes generados...")

    df = pd.DataFrame(registros)
    print(f"[OK] Clientes generados: {len(df):,} registros")
    return df


# ─────────────────────────────────────────────────────────────
# Función 2: Generar reclamos
# ─────────────────────────────────────────────────────────────
def generar_reclamos(n: int = 150000, ids_clientes: list = None) -> pd.DataFrame:
    """
    Genera un DataFrame con registros sintéticos de reclamos de Entel Perú.

    Args:
        n (int): Número de reclamos a generar. Por defecto 150,000.
        ids_clientes (list): Lista de IDs de clientes válidos para referenciar.

    Returns:
        pd.DataFrame: DataFrame con columnas de reclamos.
    """
    print(f"[INFO] Generando {n:,} registros de reclamos...")
    registros = []

    for i in range(1, n + 1):
        # Fecha de apertura en los últimos 2 años
        fecha_apertura = fake.date_time_between(
            start_date="-2y", end_date="now"
        )

        # Duración del reclamo en horas (entre 1 y 720 horas = 30 días)
        duracion_horas = round(random.uniform(1.0, 720.0), 2)
        fecha_cierre = fecha_apertura + timedelta(hours=duracion_horas)

        # Número de reescalamientos (0 a 4, con sesgo hacia 0)
        num_reescalamientos = random.choices(
            [0, 1, 2, 3, 4],
            weights=[60, 20, 10, 7, 3]
        )[0]

        # Resolución en primer contacto
        resuelto_primer_contacto = num_reescalamientos == 0

        # Reapertura en 7 días (10% de probabilidad)
        reabierto_7dias = random.random() < 0.10

        # CSAT entre 1 y 5
        csat_score = round(random.uniform(1.0, 5.0), 1)

        # Canal de ingreso (con nulos simulados ~5%)
        canal = random.choice(CANALES_INGRESO)
        if random.random() < 0.05:
            canal = None

        # Región y departamento del reclamo
        region = random.choice(REGIONES_PERU)
        departamento = random.choice(DEPARTAMENTOS_POR_REGION[region])

        registros.append({
            "id_reclamo": f"REC{i:07d}",
            "id_cliente": random.choice(ids_clientes) if ids_clientes else f"CLI{random.randint(1, 50000):06d}",
            "fecha_apertura": fecha_apertura.strftime("%Y-%m-%d %H:%M:%S"),
            "fecha_cierre": fecha_cierre.strftime("%Y-%m-%d %H:%M:%S"),
            "tipo_reclamo": random.choice(TIPOS_RECLAMO),
            "canal_ingreso": canal,
            "region": region,
            "departamento": departamento,
            "estado": random.choice(ESTADOS_RECLAMO),
            "num_reescalamientos": num_reescalamientos,
            "duracion_horas": duracion_horas,
            "resuelto_primer_contacto": resuelto_primer_contacto,
            "reabierto_7dias": reabierto_7dias,
            "csat_score": csat_score,
        })

        if i % 30000 == 0:
            print(f"  → {i:,} reclamos generados...")

    df = pd.DataFrame(registros)
    print(f"[OK] Reclamos generados: {len(df):,} registros")
    return df


# ─────────────────────────────────────────────────────────────
# Función 3: Generar logs de app móvil (JSON)
# ─────────────────────────────────────────────────────────────
def generar_logs_app(n: int = 200000, ids_clientes: list = None) -> list:
    """
    Genera una lista de registros JSON que simulan logs de la app móvil de Entel.

    Args:
        n (int): Número de logs a generar. Por defecto 200,000.
        ids_clientes (list): Lista de IDs de clientes válidos.

    Returns:
        list: Lista de diccionarios con estructura de log.
    """
    print(f"[INFO] Generando {n:,} logs de app móvil...")
    logs = []

    for i in range(1, n + 1):
        timestamp = fake.date_time_between(start_date="-2y", end_date="now")
        evento = random.choice(EVENTOS_APP)

        # Detalle adicional en JSON según el tipo de evento
        detalle = {}
        if evento == "reporte_falla":
            detalle = {
                "tipo_falla": random.choice(["sin_señal", "lentitud", "caída_llamada"]),
                "intensidad_señal": random.randint(-120, -50),
            }
        elif evento == "consulta_saldo":
            detalle = {"saldo_disponible_pen": round(random.uniform(0, 100), 2)}
        elif evento == "pago_factura":
            detalle = {"monto_pagado_pen": round(random.uniform(30, 300), 2)}

        logs.append({
            "id_log": f"LOG{i:08d}",
            "id_cliente": random.choice(ids_clientes) if ids_clientes else f"CLI{random.randint(1, 50000):06d}",
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "evento": evento,
            "detalle_json": detalle,
            "canal": "app_movil",
            "version_app": random.choice(["3.1.0", "3.2.1", "3.3.0", "4.0.0"]),
            "sistema_operativo": random.choice(["Android", "iOS"]),
        })

        if i % 50000 == 0:
            print(f"  → {i:,} logs generados...")

    print(f"[OK] Logs de app generados: {len(logs):,} registros")
    return logs


# ─────────────────────────────────────────────────────────────
# Función 4: Generar registros de call center
# ─────────────────────────────────────────────────────────────
def generar_call_center(n: int = 30000, ids_clientes: list = None) -> pd.DataFrame:
    """
    Genera un DataFrame con registros sintéticos de atenciones del call center.

    Args:
        n (int): Número de atenciones a generar. Por defecto 30,000.
        ids_clientes (list): Lista de IDs de clientes válidos.

    Returns:
        pd.DataFrame: DataFrame con columnas de atenciones de call center.
    """
    print(f"[INFO] Generando {n:,} registros de call center...")
    registros = []

    for i in range(1, n + 1):
        fecha_llamada = fake.date_time_between(start_date="-2y", end_date="now")

        # Tiempo de espera: entre 30 segundos y 20 minutos
        tiempo_espera_seg = random.randint(30, 1200)

        # Tiempo de atención: entre 1 y 30 minutos
        tiempo_atencion_seg = random.randint(60, 1800)

        registros.append({
            "id_atencion": f"CC{i:06d}",
            "id_cliente": random.choice(ids_clientes) if ids_clientes else f"CLI{random.randint(1, 50000):06d}",
            "fecha_llamada": fecha_llamada.strftime("%Y-%m-%d %H:%M:%S"),
            "tiempo_espera_seg": tiempo_espera_seg,
            "tiempo_atencion_seg": tiempo_atencion_seg,
            "agente": f"AGT{random.randint(1, 200):03d}",
            "resultado": random.choice([
                "resuelto", "escalado", "abandonado", "callback"
            ]),
        })

        if i % 10000 == 0:
            print(f"  → {i:,} atenciones generadas...")

    df = pd.DataFrame(registros)
    print(f"[OK] Call center generado: {len(df):,} registros")
    return df


# ─────────────────────────────────────────────────────────────
# Función principal: guardar todos los datasets
# ─────────────────────────────────────────────────────────────
def main():
    """
    Función principal que coordina la generación y guardado de todos los datasets
    sintéticos del proyecto Big Data Entel Perú 2026.
    """
    print("=" * 60)
    print("GENERACIÓN DE DATOS SINTÉTICOS — ENTEL PERÚ 2026")
    print("=" * 60)

    try:
        # 1. Generar clientes
        df_clientes = generar_clientes(n=50000)
        ruta_clientes = os.path.join(OUTPUT_PATH, "clientes.csv")
        df_clientes.to_csv(ruta_clientes, index=False, encoding="utf-8")
        print(f"[GUARDADO] {ruta_clientes}")

        # Extraer lista de IDs para referencias cruzadas
        ids_clientes = df_clientes["id_cliente"].tolist()

        # 2. Generar reclamos
        df_reclamos = generar_reclamos(n=150000, ids_clientes=ids_clientes)
        ruta_reclamos = os.path.join(OUTPUT_PATH, "reclamos.csv")
        df_reclamos.to_csv(ruta_reclamos, index=False, encoding="utf-8")
        print(f"[GUARDADO] {ruta_reclamos}")

        # 3. Generar logs de app móvil
        logs_app = generar_logs_app(n=200000, ids_clientes=ids_clientes)
        ruta_logs = os.path.join(OUTPUT_PATH, "logs_app.json")
        with open(ruta_logs, "w", encoding="utf-8") as f:
            json.dump(logs_app, f, ensure_ascii=False, indent=2)
        print(f"[GUARDADO] {ruta_logs}")

        # 4. Generar registros de call center
        df_callcenter = generar_call_center(n=30000, ids_clientes=ids_clientes)
        ruta_callcenter = os.path.join(OUTPUT_PATH, "call_center.csv")
        df_callcenter.to_csv(ruta_callcenter, index=False, encoding="utf-8")
        print(f"[GUARDADO] {ruta_callcenter}")

        # Resumen final
        print("\n" + "=" * 60)
        print("RESUMEN DE GENERACIÓN:")
        print(f"  Clientes:      {len(df_clientes):>10,} registros → clientes.csv")
        print(f"  Reclamos:      {len(df_reclamos):>10,} registros → reclamos.csv")
        print(f"  Logs App:      {len(logs_app):>10,} registros → logs_app.json")
        print(f"  Call Center:   {len(df_callcenter):>10,} registros → call_center.csv")
        print(f"  Ruta de salida: {OUTPUT_PATH}")
        print("=" * 60)
        print("[COMPLETADO] Generación de datos sintéticos finalizada.")

    except Exception as e:
        print(f"[ERROR] Error durante la generación de datos: {e}")
        raise


if __name__ == "__main__":
    main()