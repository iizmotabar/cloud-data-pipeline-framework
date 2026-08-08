"""
Airflow DAG that chains extraction and dbt transformation, with retries and
Slack alerting on failure. Runs once daily, early morning, so the warehouse is
current before business hours.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": None,  # wired to slack_alert_on_failure below in production
}

with DAG(
    dag_id="daily_warehouse_pipeline",
    default_args=default_args,
    schedule_interval="0 5 * * *",  # 5 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["extraction", "dbt", "daily"],
) as dag:

    def extract_crm():
        from python.extract_api_source import APISourceConfig, run

        run(
            APISourceConfig(
                base_url="https://api.example-crm.com/v1/contacts",
                auth_header={"Authorization": "Bearer REPLACE_WITH_TOKEN"},
                destination_table="project.raw.crm_contacts",
            )
        )

    extract_crm_task = PythonOperator(
        task_id="extract_crm_contacts",
        python_callable=extract_crm,
    )

    run_dbt_task = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/dbt_project && dbt run --select staging+ marts+",
    )

    alert_on_failure = SlackWebhookOperator(
        task_id="alert_on_failure",
        slack_webhook_conn_id="slack_webhook",
        message="Daily warehouse pipeline failed, check Airflow logs.",
        trigger_rule="one_failed",
    )

    extract_crm_task >> run_dbt_task >> alert_on_failure
