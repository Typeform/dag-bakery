from datetime import datetime
from typing import Callable, Optional, List

from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance
from airflow.operators.slack_operator import SlackAPIPostOperator

from dag_bakery.callbacks.context_callback import ContextCallback


def task_fail_slack_msg(_: dict) -> List[dict]:
    attachments = [
        {
            "mrkdwn_in": ["text"],
            "color": "#ED553B",
            "author_name": "{{ task_instance.dag_id }}",
            "author_link": "{{ task_instance.log_url }}",
            "author_icon": "https://airflow.apache.org/docs/stable/_images/pin_large.png",
            "fields": [
                {
                    "title": "Task ID",
                    "value": "{{ task_instance.task_id }}",
                    "short": False,
                },
                {
                    "title": "Exception",
                    "value": "{{ exception }}",
                    "short": False,
                },
            ],
            "footer": "{{ env_var('ENV', 'local') }} | <{{ task_instance.log_url }}|Check Logs>",
            "footer_icon": "https://image.flaticon.com/icons/png/512/391/391116.png",
            "ts": datetime.now().timestamp(),
        },
    ]
    return attachments


class SlackAlertCallback(ContextCallback):
    def __init__(
        self, slack_conn_id: str = "SLACK", message_gen: Callable = task_fail_slack_msg, channel: Optional[str] = None
    ):
        self.slack_conn_id = slack_conn_id
        self.message_gen = message_gen
        self.channel = channel

    def callback(self, context: dict):
        attachments = self.message_gen(context)

        slack_conn = BaseHook.get_connection(self.slack_conn_id)
        slack_channel = self.channel or slack_conn.login

        slack_alert_operator = SlackAPIPostOperator(
            slack_conn_id=self.slack_conn_id,
            task_id="slack_alert",
            channel=slack_channel,
            text=None,
            attachments=attachments,
            username="Airflow {{ env_var('ENV', 'local') }}",
        )

        task_instance: TaskInstance = context["task_instance"]
        template_env = task_instance.task.get_template_env()
        slack_alert_operator.render_template_fields(
            context=context,
            jinja_env=template_env,
        )
        return slack_alert_operator.execute(context=context)
