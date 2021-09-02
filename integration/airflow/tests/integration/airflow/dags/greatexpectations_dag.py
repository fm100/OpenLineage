from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

from openlineage.airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from openlineage.client import set_producer

set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow")

default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}

data_context_dir = "/opt/data/great_expectations"

dag = DAG(
    'great_expectations_validation',
    schedule_interval='@once',
    default_args=default_args,
    description='Validates data.'
)

t1 = GreatExpectationsOperator(
    task_id='ge_sqlite_test',
    run_name="ge_sqlite_run",
    checkpoint_name="sqlite",
    data_context_root_dir=data_context_dir,
    dag=dag
)

t2 = GreatExpectationsOperator(
    task_id='ge_pandas_test',
    run_name="ge_pandas_run",
    checkpoint_name="pandas",
    data_context_root_dir=data_context_dir,
    dag=dag
)

t1 >> t2
