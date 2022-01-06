from abc import ABC
from datetime import datetime, timezone
from typing import List

from airflow.contrib.hooks.datadog_hook import DatadogHook
from airflow.models import TaskInstance
from airflow.utils.state import State

from dag_bakery.callbacks.context_callback import ContextCallback
from dag_bakery.utils.transformers import clean_key

COUNT = "count"
TIMER = "gauge"


def tags_to_list(tags: dict) -> List[str]:
    return [f"{key}:{clean_key(value)}" for key, value in tags.items()]


class DatadogAlertCallback(ContextCallback, ABC):
    def __init__(self, datadog_conn_id: str = "datadog", namespace: str = "airflow", enable_events: bool = False):
        self.datadog_conn_id = datadog_conn_id
        self.namespace = namespace
        self.enable_events = enable_events


class DatadogTaskAlertCallback(DatadogAlertCallback):
    def callback(self, context: dict):
        datadog = DatadogHook(datadog_conn_id=self.datadog_conn_id)
        task_instance: TaskInstance = context["task_instance"]
        duration = datetime.now(timezone.utc) - task_instance.start_date
        duration = duration.total_seconds()
        state = task_instance.state

        tags = {
            "dag": task_instance.dag_id,
            "dag_owner": task_instance.task.dag.owner,
            "task_id": task_instance.task_id,
            "operator": task_instance.operator,
            "state": state,
        }
        tags_list = tags_to_list(tags)

        datadog.send_metric(
            metric_name=f"{self.namespace}.task.{state}",
            datapoint=1,
            tags=tags_list,
            type_=COUNT,
        )
        datadog.send_metric(
            metric_name=f"{self.namespace}.task.execution_time",
            datapoint=duration,
            tags=tags_list,
            type_=TIMER,
        )
        if self.enable_events:
            alert_type = "info" if state in [State.SUCCESS, State.SKIPPED] else "error"
            datadog.post_event(
                title=f"DAG {task_instance.dag_id} | Task {task_instance.task_id} | Status {state}",
                text="",
                aggregation_key=f"{self.namespace}.{task_instance.dag_id}",
                alert_type=alert_type,
                tags=tags_list,
            )


class DatadogDagAlertCallback(DatadogAlertCallback):
    def callback(self, context: dict):
        datadog = DatadogHook(datadog_conn_id=self.datadog_conn_id)
        dag_run = context["dag_run"]
        dag_id = dag_run.dag_id
        state = dag_run.state
        owner = "undefined"

        duration = datetime.now(timezone.utc) - context["execution_date"]
        duration = duration.total_seconds()

        tags = {
            "dag": dag_id,
            "dag_owner": owner,
            "state": state,
        }
        tags_list = tags_to_list(tags)

        datadog.send_metric(
            metric_name=f"{self.namespace}.dag.{state}",
            datapoint=1,
            tags=tags_list,
            type_=COUNT,
        )
        datadog.send_metric(
            metric_name=f"{self.namespace}.dag.execution_time",
            datapoint=duration,
            tags=tags_list,
            type_=TIMER,
        )

        if self.enable_events:
            alert_type = "info" if state in [State.SUCCESS] else "error"
            datadog.post_event(
                title=f"DAG {dag_id} | Status {state}",
                text=context["reason"],
                aggregation_key=f"{self.namespace}.{dag_id}",
                alert_type=alert_type,
                tags=tags_list,
            )
