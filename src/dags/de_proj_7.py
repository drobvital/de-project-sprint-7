from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    'geo_marts_dag',
    default_args = {'start_date': datetime(2025,12,16)},
    schedule = '0 2 * * *', # каждый день в 2 часа ночи
    catchup = False,
)
#Задача для первой витрины
user_geo = SparkSubmitOperator(
    task_id = 'calculate_user_geo_mart',
    application = '/home/vitekdy/scripts/mart1_usergeo.py',
    conn_id = 'spark_local',
    conf = {
        'spark.master': 'local',
        'spark.executor.instances': '10',
        'spark.submit.deployMode' : 'client',
    },
    application_args = [
        "/user/master/data/geo/events",
        '/user/vitekdy/data/geo_1.parquet',
        '/user/vitekdy/data/analytics/user_geo',
        "27",
        "yyyy-MM-dd HH:mm:ss"
   ],
    dag=dag,
)
zone_geo = SparkSubmitOperator(
    task_id = 'calculate_zone_geo_mart',
    application = '/home/vitekdy/scripts/mart2_zone_geo.py',
    conn_id = 'spark_local',
    conf = {
        'spark.master': 'local',
        'spark.executor.instances': '10',
        'spark.submit.deployMode' : 'client',
    },
    application_args = [
        "/user/master/data/geo/events",
        '/user/vitekdy/data/geo_1.parquet',
        '/user/vitekdy/data/analytics/zone_geo'
   ],
    dag=dag,
)
recommendations = SparkSubmitOperator(
    task_id = 'calculate_recommendation_mart',
    application = '/home/vitekdy/scripts/mart3_recommend.py',
    conn_id = 'spark_local',
    conf = {
        'spark.master': 'local',
        'spark.executor.instances': '10',
        'spark.submit.deployMode' : 'client',
    },
    application_args = [
        "/user/master/data/geo/events",
        '/user/vitekdy/data/geo_1.parquet',
        '/user/vitekdy/data/analytics/recommendations',
        "404815",
        "1.0",
        "True",
   ],
    dag=dag,
)
[user_geo >> zone_geo >> recommendations]

