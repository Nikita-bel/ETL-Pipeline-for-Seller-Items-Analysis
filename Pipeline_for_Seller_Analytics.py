import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"
S3_BUCKET = "startde-project"
S3_PATH = "nikita-belov-cbx8663/seller_items"


CREATE_ITEMS_QUERY = """
    DROP EXTERNAL TABLE IF EXISTS "nikita-belov-cbx8663".seller_items CASCADE;
    CREATE EXTERNAL TABLE "nikita-belov-cbx8663".seller_items (
    sku_id BIGINT,
    title TEXT,
    category TEXT,
    brand TEXT,
    seller TEXT,
    group_type TEXT,
    country TEXT,
    availability_items_count BIGINT,
    ordered_items_count BIGINT,
    warehouses_count BIGINT,
    item_price BIGINT,	
    goods_sold_count BIGINT,	
    item_rate FLOAT8,	
    days_on_sell BIGINT,
    avg_percent_to_sold	BIGINT,	
    returned_items_count INTEGER,	
    potential_revenue BIGINT,	
    total_revenue BIGINT,	
    avg_daily_sales	FLOAT8,	
    days_to_sold FLOAT8,	
    item_rate_percent FLOAT8
    )
    LOCATION ('pxf://startde-project/nikita-belov-cbx8663/seller_items?PROFILE=s3:parquet&SERVER=default')
    ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
"""

UNRELIABLE_SELLERS_QUERY = """
    CREATE OR REPLACE VIEW "nikita-belov-cbx8663".unreliable_sellers_view AS
    select r.seller,
    r.total_overload_items_count,
    r.total_ordered_items_count > r.total_overload_items_count as is_unreliable
    from
    (select s.seller,
    sum(s.availability_items_count) as total_overload_items_count, 
    sum(s.ordered_items_count) as total_ordered_items_count
    from "nikita-belov-cbx8663".seller_items as s 
    where s.days_on_sell > 100
    group by s.seller) as r
"""


def _build_submit_operator(task_id: str, application_file: str, link_dag):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=link_dag
    )


def _build_sensor(task_id: str, application_name: str, link_dag):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        dag=link_dag
    )


with DAG(
    dag_id="startde-project-nikita-belov-cbx8663-dag",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 9, 10, tz="UTC"),
    tags=["de-project"],
    catchup=False
) as dag:

    submit_task = _build_submit_operator(
        task_id='job_submit',
        application_file='spark_submit.yaml',
        link_dag=dag
    )

    sensor_task = _build_sensor(
        task_id='job_sensor',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='job_submit')['metadata']['name']}}}}",
        link_dag=dag
    )

    create_gp_datamart = SQLExecuteQueryOperator(
        task_id='items_datamart',
        conn_id = GREENPLUM_ID,
        sql = CREATE_ITEMS_QUERY
    )

    create_view = SQLExecuteQueryOperator(
        task_id = "create_unreliable_sellers_report_view",
        conn_id = GREENPLUM_ID,
        sql = UNRELIABLE_SELLERS_QUERY
    )

    submit_task >> sensor_task >> create_gp_datamart
    create_gp_datamart >> create_view