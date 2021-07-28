from airflow import DAG 
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint

# Definir las funciones a utilizar - jobs
def _choose_best_model(ti):
    # get the data from the database (SQLite)
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])

    # escojer el accuracy alto
    best_accuracy = max(accuracies)

    if (best_accuracy > 8):
        variable = 'accurate'
    else:
        variable = 'inaccurate'
    return variable


def _training_model():
    return randint(1, 10)

# Create an instance of DAG - context manager
with DAG("my_dag", start_date=datetime(2021, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

    # Python Operator
    training_model_A = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model,
    )

    training_model_B = PythonOperator(
        task_id="training_model_B",
        python_callable=_training_model,
    )

    training_model_C = PythonOperator(
        task_id="training_model_C",
        python_callable=_training_model,
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_choose_best_model
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

    # Defining the order of executions - dependencies
    [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]