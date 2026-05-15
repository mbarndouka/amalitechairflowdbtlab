"""Weekly ML training pipeline — runs every Monday at 02:00 UTC.

Triggers each stage of the Flight Fare Prediction ML pipeline
as an isolated Docker container, sharing host volumes so artifacts
(models, MLflow DB, reports) persist between stages.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

ML_IMAGE = "amalitech-mllab:latest"
ML_LAB_PATH = "/home/mbarndouka/Documents/amalitechmllab"

_COMMON_MOUNTS = [
    Mount(source=f"{ML_LAB_PATH}/data",        target="/app/data",        type="bind"),
    Mount(source=f"{ML_LAB_PATH}/models",       target="/app/models",      type="bind"),
    Mount(source=f"{ML_LAB_PATH}/reports",      target="/app/reports",     type="bind"),
    Mount(source=f"{ML_LAB_PATH}/mlflow.db",    target="/app/mlflow.db",   type="bind"),
    Mount(source=f"{ML_LAB_PATH}/logs",         target="/app/logs",        type="bind"),
    # Mount at same absolute path as host so artifact URIs stored in mlflow.db resolve correctly
    # (experiment artifact_location = file:///home/mbarndouka/Documents/amalitechmllab/mlartifacts)
    Mount(source=f"{ML_LAB_PATH}/mlartifacts",  target=f"{ML_LAB_PATH}/mlartifacts", type="bind"),
]

_COMMON_ENV = {
    "MLFLOW_TRACKING_URI": "sqlite:////app/mlflow.db",
    "MLFLOW_REGISTRY_NAME": "FarePredictor",
    "MODELS_DIR": "/app/models",
    "FEATURES_DIR": "/app/data/features",
}


def _stage(stage_name: str) -> DockerOperator:
    return DockerOperator(
        task_id=f"stage_{stage_name}",
        image=ML_IMAGE,
        command=["python", "main.py", "--stage", stage_name],
        mounts=_COMMON_MOUNTS,
        environment=_COMMON_ENV,
        network_mode="amalitech-net",
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )


@dag(
    dag_id="ml_training_pipeline",
    start_date=datetime(2026, 5, 19),
    schedule="0 2 * * 1",   # Every Monday at 02:00 UTC
    catchup=False,
    tags=["ml", "flight-fare", "training"],
    doc_md="""
## ML Training Pipeline

Retrains the Flight Fare Prediction models every Monday at 02:00 UTC.

### Stages (in order)

| Task | Stage | What it does |
|---|---|---|
| `stage_clean` | clean | Remove leakage columns, fix types, handle missing values |
| `stage_engineer` | engineer | Encode features, log-transform target, train/val/test split |
| `stage_train` | train | Linear Regression baseline |
| `stage_advanced` | advanced | Ridge, Lasso, Tree, RF, GBT, XGBoost, Stacking |
| `stage_tune` | tune | Optuna hyperparameter search for XGBoost (100 trials) |
| `stage_interpret` | interpret | SHAP values, feature importance, business insights report |

### Artifacts written (host paths)

- `amalitechmllab/models/` — trained `.pkl` files
- `amalitechmllab/mlflow.db` — experiment tracking DB
- `amalitechmllab/mlartifacts/` — MLflow model artifacts
- `amalitechmllab/reports/` — metrics JSON, stakeholder report

### Viewing results

- MLflow UI: `docker compose up mlflow-ui` (from `amalitechmllab/`)
- Streamlit: `docker compose up streamlit` (from `amalitechmllab/`)
    """,
)
def ml_training_pipeline():
    clean     = _stage("clean")
    engineer  = _stage("engineer")
    train     = _stage("train")
    advanced  = _stage("advanced")
    tune      = _stage("tune")
    interpret = _stage("interpret")

    clean >> engineer >> train >> advanced >> tune >> interpret


ml_training_pipeline()
