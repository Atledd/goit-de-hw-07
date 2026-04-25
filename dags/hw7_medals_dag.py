import random
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule


MYSQL_CONN_ID = "mysql_default"
TARGET_TABLE = "olympic_dataset.medal_counts"


def pick_medal(**context):
    medal = random.choice(["Bronze", "Silver", "Gold"])
    context["ti"].xcom_push(key="medal", value=medal)
    print(f"Selected medal: {medal}")


def choose_branch(**context):
    medal = context["ti"].xcom_pull(task_ids="pick_medal", key="medal")
    return f"count_{medal.lower()}_medals"


def generate_delay():
    time.sleep(5)


with DAG(
    dag_id="hw7_airflow_medals",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["goit", "hw7", "airflow"],
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=MYSQL_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS olympic_dataset.medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(20),
            `count` INT,
            created_at DATETIME
        );
        """,
    )

    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_by_medal",
        python_callable=choose_branch,
    )

    count_bronze = MySqlOperator(
        task_id="count_bronze_medals",
        mysql_conn_id=MYSQL_CONN_ID,
        sql="""
        INSERT INTO olympic_dataset.medal_counts (medal_type, `count`, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    count_silver = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=MYSQL_CONN_ID,
        sql="""
        INSERT INTO olympic_dataset.medal_counts (medal_type, `count`, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    count_gold = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=MYSQL_CONN_ID,
        sql="""
        INSERT INTO olympic_dataset.medal_counts (medal_type, `count`, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_recent_record = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        sql="""
        SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
        FROM olympic_dataset.medal_counts;
        """,
        poke_interval=5,
        timeout=30,
        mode="poke",
    )

    create_table >> pick_medal_task >> branch_task

    branch_task >> [count_bronze, count_silver, count_gold]

    [count_bronze, count_silver, count_gold] >> delay_task >> check_recent_record