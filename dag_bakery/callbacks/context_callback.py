from abc import ABC, abstractmethod
from typing import List, TypeVar, Any, Optional, Generic

from airflow import DAG
from pydantic import BaseModel


class ContextCallback(ABC):
    @abstractmethod
    def callback(self, context: dict) -> Any:
        pass

    def __call__(self, context: dict):
        return self.callback(context)


ContextCallbackType = TypeVar("ContextCallbackType", bound=ContextCallback)


def chain_callbacks(ctx_callbacks: List[ContextCallbackType]):
    def execute(context: dict):
        for ctx_callback in ctx_callbacks:
            ctx_callback(context)

    return execute


class CallbacksConfig(BaseModel, Generic[ContextCallbackType]):
    task_success_callbacks: Optional[List[ContextCallbackType]] = None
    task_failure_callbacks: Optional[List[ContextCallbackType]] = None
    dag_success_callbacks: Optional[List[ContextCallbackType]] = None
    dag_failure_callbacks: Optional[List[ContextCallbackType]] = None


def set_task_success_callbacks(dag: DAG, callbacks: List[ContextCallbackType]):
    tasks = dag.tasks
    alerts = chain_callbacks(callbacks)
    for task in tasks:
        task.on_success_callback = alerts


def set_dag_success_callbacks(dag: DAG, callbacks: List[ContextCallbackType]):
    alerts = chain_callbacks(callbacks)
    dag.on_success_callback = alerts


def set_task_failure_callbacks(dag: DAG, callbacks: List[ContextCallbackType]):
    tasks = dag.tasks
    alerts = chain_callbacks(callbacks)
    for task in tasks:
        task.on_failure_callback = alerts


def set_dag_failure_callbacks(dag: DAG, callbacks: List[ContextCallbackType]):
    alerts = chain_callbacks(callbacks)
    dag.on_failure_callback = alerts


def configure_callbacks(dag: DAG, callbacks_config: Optional[CallbacksConfig]):
    if not callbacks_config:
        return
    if callbacks_config.task_success_callbacks:
        set_task_success_callbacks(dag, callbacks_config.task_success_callbacks)
    if callbacks_config.dag_success_callbacks:
        set_dag_success_callbacks(dag, callbacks_config.dag_success_callbacks)
    if callbacks_config.task_failure_callbacks:
        set_task_failure_callbacks(dag, callbacks_config.task_failure_callbacks)
    if callbacks_config.dag_failure_callbacks:
        set_dag_failure_callbacks(dag, callbacks_config.dag_failure_callbacks)
