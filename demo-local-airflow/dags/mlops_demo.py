"""Demo DAGs for manually exercising the airflow-mlops plugin (one healthy, two failing)."""

from __future__ import annotations

import random
from datetime import datetime

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["mlops", "demo"])
def demo_train_ok():
    @task
    def extract() -> dict:
        return {"rows": 1200}

    @task
    def quality_check(stats: dict, ti=None) -> dict:  # `ti` is injected by Airflow
        # Data-quality convention read by the report (XCom key "data_quality").
        report = {"rows": stats["rows"], "null_ratio": 0.01, "freshness": "PT1H",
                  "checks_passed": True}
        ti.xcom_push(key="data_quality", value=report)
        return report

    quality_check(extract())


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["mlops", "demo"])
def demo_train_critical():
    @task
    def load() -> dict:
        return {"features": [1, 2, 3]}

    @task
    def train(frame: dict) -> None:
        print(frame["target"])  # KeyError -> critical

    @task(trigger_rule="all_done")
    def notify() -> None:
        print("notify")

    train(load()) >> notify()


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["mlops", "demo"])
def demo_fetch_transient():
    @task(retries=0)
    def fetch() -> None:
        if random.random() < 0.9:
            raise ConnectionResetError("[Errno 104] Connection reset by peer")

    @task
    def publish() -> None:
        print("published")

    fetch() >> publish()


demo_train_ok()
demo_train_critical()
demo_fetch_transient()
