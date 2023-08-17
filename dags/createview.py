from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

dateTime =str(datetime.now().strftime('%Y-%m-%d'))
# dateTime = "2023-04-25"


with DAG('CreateDailyVIEW',start_date = datetime(2022,1,1),
    schedule_interval='30 18 * * *',catchup=False,tags=["cimri"])as dag:
    
    create_product_enfView = PostgresOperator(
        task_id='create_product_enfView',
        postgres_conn_id = 'cimri_db',
        sql=f'''
            CREATE OR REPLACE VIEW ENFLASYONDENEME AS
            select p.product_id ,p.product_title,p.product_category,p.product_price as agustosfiyat,p2.product_price as guncelFiyat,  round(((((p2.product_price-p.product_price)/p.product_price)*100)),2) as enflasyon , p2.product_date 
            from (select * from product3  where product_title in(select product_title  from product3 p group by product_title) and product_date='2022-08-01')as p 
            join 
            (select * from product3  where product_title in (select product_title  from product3 p group by product_title)  and product_date='{dateTime}')as p2 
            on p2.product_title=p.product_title  order by enflasyon desc ;
        '''
    )
    create_category_enfView = PostgresOperator(
        task_id='create_category_enfView',
        postgres_conn_id = 'cimri_db',
        sql=f'''
            CREATE OR REPLACE VIEW KATEGORIENFLASYON AS
            select p.category_id ,c.* from productcategory p 
            join  
            (select product_category as Category_Name ,sum(enflasyon)/count(product_category) as Category_Enf , '{dateTime}' as Category_Date from ENFLASYONDENEME group by product_category)c
            on c.Category_Name=p.category_name ;

        '''
    )
create_product_enfView>>create_category_enfView