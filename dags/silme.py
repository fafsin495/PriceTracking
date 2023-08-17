from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG('silme',start_date = datetime(2022,1,1),
    schedule_interval='@daily',catchup=False,tags=["cimri"])as dag:
    
    delete_table = PostgresOperator(
        task_id='delete_table',
        postgres_conn_id = 'postgres',
        sql='''
            DELETE FROM product3 WHERE product_title is not null;
        '''
    )
    count_table = PostgresOperator(
        task_id='count_table',
        postgres_conn_id = 'postgres',
        sql='''
            select count(*) from product3;
        '''
    )
delete_table>>count_table