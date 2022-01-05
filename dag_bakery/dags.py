from datetime import timedelta, datetime
from typing import Optional, Dict, Any, List, Union

from airflow import DAG
from airflow.models.dag import ScheduleInterval
from pydantic import BaseModel

from dag_bakery.callbacks.context_callback import CallbacksConfig, configure_callbacks


class DefaultArgsConfig(BaseModel):
    owner: str
    email: str
    email_on_failure: Optional[bool] = True
    email_on_retry: Optional[bool] = False
    depends_on_past: Optional[bool] = None
    pool: Optional[str] = None
    retries: Optional[int] = None
    retry_delay: Optional[timedelta] = timedelta(seconds=30)
    execution_timeout: timedelta = timedelta(hours=3)


class DagConfig(BaseModel):
    dag_name: str
    description: str
    start_date: datetime
    end_date: Optional[datetime] = None
    interval: ScheduleInterval
    tags: Optional[List[str]] = None
    catchup: bool = True
    concurrency: int = 1
    max_active_runs: int = 1
    dagrun_timeout: Optional[timedelta] = timedelta(hours=6)

    # default args
    default_args: DefaultArgsConfig


class UserDefinedIngredients(BaseModel):
    user_defined_filters: Optional[Dict] = None
    user_defined_macros: Optional[Dict] = None
    template_searchpath: Optional[Union[List[str], str]] = None
    constants: Optional[Dict[str, Any]] = None
    slack: Optional[str] = None


def build_dag(dag_config: DagConfig, callbacks_config: Optional[CallbacksConfig] = None) -> DAG:
    with DAG(
        dag_id=dag_config.dag_name,
        default_args=dag_config.default_args.dict(),
        description=dag_config.description,
        catchup=dag_config.catchup,
        concurrency=dag_config.concurrency,
        max_active_runs=dag_config.max_active_runs,
        dagrun_timeout=dag_config.dagrun_timeout,
        tags=dag_config.tags,
        start_date=dag_config.start_date,
        end_date=dag_config.end_date,
        schedule_interval=dag_config.interval,
        template_searchpath=None,
        user_defined_filters=None,
        user_defined_macros=None,
    ) as dag:
        # builder_fn(dag)
        pass

    configure_callbacks(dag, callbacks_config)
    return dag
