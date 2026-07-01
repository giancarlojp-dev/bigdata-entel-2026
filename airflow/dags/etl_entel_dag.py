"""
etl_entel_dag.py
================
DAG de orquestación del pipeline ETL completo para el proyecto Big Data
Entel Perú 2026. Ejecuta diariamente a las 2:00 AM las 5 fases del pipeline:
extracción, transformación, carga, validación y notificación.

Tareas:
    1. task_extract    → 02_extract.py
    2. task_transform  → 03_transform.py
    3. task_load       → 04_load.py
    4. task_validate   → 05_validate.py
    5. task_notify     → log de resultado final

Autor: Choque Martinez Gabriel / Jacobo Pachas Giancarlo
Curso: Big Data y Analytics — UPSJB VIII Ciclo 2026
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# ─────────────────────────────────────────────────────────────
# Configuración por defecto del DAG-
# ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "entel_bigdata_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Ruta del Python del host (montado vía volumen, no el de la imagen de Airflow)
# Se usa BashOperator para 02 y 03 porque requieren pandas/pyspark con
# dependencias pesadas que no se instalan dentro del contenedor de Airflow
RUTA_SCRIPTS = "/opt/airflow/scripts"


def task_notify_callable(**context):
    """
    Función ejecutada al final del DAG. Registra en el log de Airflow
    un resumen del resultado de la ejecución completa del pipeline.

    Args:
        **context: Contexto de ejecución inyectado automáticamente por Airflow.

    Returns:
        str: Mensaje de resumen registrado en los logs de la tarea.
    """
    fecha_ejecucion = context["ds"]
    mensaje = (
        f"[NOTIFICACION] Pipeline ETL Entel Perú 2026 completado exitosamente. "
        f"Fecha de ejecución: {fecha_ejecucion}. "
        f"Fases completadas: extract -> transform -> load -> validate."
    )
    print(mensaje)
    return mensaje


# ─────────────────────────────────────────────────────────────
# Definición del DAG
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="etl_entel_pipeline",
    description="Pipeline ETL diario: extraccion, transformacion PySpark, carga MongoDB y validacion",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule="0 2 * * *",  # Diario a las 2:00 AM
    catchup=False,
    tags=["entel", "bigdata", "etl", "upsjb"],
) as dag:

    # ── Tarea 1: Extracción ──────────────────────────────────
    task_extract = BashOperator(
        task_id="task_extract",
        bash_command=f"python {RUTA_SCRIPTS}/02_extract.py",
        doc_md="Lee fuentes crudas (OSIPTEL + sintéticos) y genera Parquet crudo.",
    )

    # ── Tarea 2: Transformación con PySpark ──────────────────
    task_transform = BashOperator(
        task_id="task_transform",
        bash_command=f"python {RUTA_SCRIPTS}/03_transform.py",
        doc_md="Aplica limpieza, normalización y enriquecimiento con PySpark.",
    )

    # ── Tarea 3: Carga en MongoDB ─────────────────────────────
    task_load = BashOperator(
        task_id="task_load",
        bash_command=f"python {RUTA_SCRIPTS}/04_load.py",
        doc_md="Carga los Parquet procesados en las 4 colecciones de MongoDB con índices.",
    )

    # ── Tarea 4: Validación de calidad ────────────────────────
    task_validate = BashOperator(
        task_id="task_validate",
        bash_command=f"python {RUTA_SCRIPTS}/05_validate.py",
        doc_md="Ejecuta el reporte de validación de calidad sobre MongoDB.",
    )

    # ── Tarea 5: Notificación final ───────────────────────────
    task_notify = PythonOperator(
        task_id="task_notify",
        python_callable=task_notify_callable,
        doc_md="Registra en los logs de Airflow el resultado final del pipeline.",
    )

    # ── Definición de la secuencia (dependencias) ─────────────
    task_extract >> task_transform >> task_load >> task_validate >> task_notify