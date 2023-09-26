from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import warnings
import requests
warnings.filterwarnings('ignore')
import json
from datetime import datetime,timedelta
import time
import logging
import urllib

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# bu metot içerisinde verilen linklere request sorgusu atılıp sorgu sonucu alınan verileri json formatında kaydeder.
def get_link(**kwargs):
    url =["İlgili web sitesine ait ürünlere ait urllerin tanımlanması"]
    json_result =[]
    for i in url:
        try:
            time.sleep(1)
            
            headers ={
                    # 'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    # 'Accept-Language':'en-GB,en;q=0.9,tr-TR;q=0.8,tr;q=0.7,en-US;q=0.6',
                    # 'referer':'http://localhost:8080/',
            }
            getRequest = requests.get(i,headers=headers)
            html = BeautifulSoup(getRequest.text,'html.parser')
            json_result.append(json.loads(str(html.find('script',{'id':'__NEXT_DATA__'}).text))) 
        except urllib.error.HTTPError as e:
            print("ERROR ===> ",e)
            logging.info(i+" --HATA ALDI-- " + str(e))
            time.sleep(1)
            continue
    return json_result

    
# bu metot içerisinde ilgili json verilerin içerisinde ihtiyacımıza yönelik olan kısımları alınıp bir dataframe'e kaydedilir
def parse_json(**kwargs):
    
    ti = kwargs['ti']
    v1= ti.xcom_pull(key=None, task_ids='get_link')
    logging.info("V! DENEME => " + str(v1))

    minPrice=[]
    date = []
    p_title = []
    p_category =[]
    p_id = []
    p_url =[]
    current_time = str(datetime.now().strftime("%Y-%m-%d")) 

    for den1 in v1:
        try:
            # minPrice.append(float([i['minPrice'] for i in den1['props']['pageProps']['data']['priceHistory'] if i['date'].split('T')[0] == current_time ][0]))
            p_title.append(den1['query']['data']['data']['product']["product"]['title'])
            p_category.append(den1['query']['data']['data']['product']["product"]["categorySummary"]['name'])
            p_id.append(den1['query']['data']['data']['product']["product"]['id'])
            p_url.append("https://www.cimri.com/"+den1['query']['data']['data']['product']["product"]["path"])
            strdate = str(current_time.strftime("%Y-%m-%d"))
            date.append(strdate)
            if(len(den1['query']['data']['data']['priceHistory']["success"])!= 0):
                for i in  den1['query']['data']['data']['priceHistory']["success"]:
                    if i['date'].split('T')[0] == strdate:
                        minPrice.append(i['minPrice'])
                        break
                    else:
                        minPrice.append(den1['query']['data']['data']['priceHistory']["success"][0]['minPrice'])
                        break
            else:
                minPrice.append(0)
        except Exception as e:
            print(e)
            logging.info(" --HATA ALDI-- " + str(e.__class__) )
        
    df = pd.DataFrame(
        {
            'product_title':p_title,
            'product_category':p_category,
            'product_price': minPrice,
            'product_date':date,
            'product_id':p_id,
            'product_url':p_url
        }
    )
    result = df.to_json(orient='columns',force_ascii=False)
    return result

# kaydedilen dataframedeki verileri ilgili sql scriptine yazdırılma işlemini gerçekleştirir
def prepare_query(**kwargs):
    try:
        ti = kwargs['ti']
        v1= ti.xcom_pull(key=None, task_ids='parse_json')
        logging.info("XCOM dan veriler alındı " )
        df = pd.read_json(v1) 
        forex_currencies2= open('/opt/airflow/dags/sql/product_schema.sql',"w",encoding='utf-8')
        forex_currencies2.seek(0,0)

        for index,row in df.iterrows():
            title ="%s" %str(row['product_title'])
            title = title.replace("'"," ")
            date= datetime.strptime(str(row['product_date'])[:10], '%Y-%m-%d').date()
            category = str(row['product_category'])
            price= float(row['product_price'])
            product_id = str(row['product_id'])
            product_link = str(row['product_url'])

            with open('/opt/airflow/dags/sql/product_schema.sql', 'a', encoding='utf-8') as forex_currencies:
                forex_currencies.write("insert into product3 (product_title,product_category,product_price,product_date,product_id,product_url) values ('%s' , '%s' ,%s,'%s','%s','%s');\n" % (title, category, price, date, product_id, product_link))

            logging.info("INSERTLER yazıldı")
            forex_currencies.close()
            logging.info("SQL KAPATILDI")
    except Exception  as e :
        logging.info(" --HATA ALDI-- " + str(e.__class__) )
        logging.info(" --HATA ALDI-- " + str(e.args) )
        


with DAG('son' ,start_date = datetime(2022,9,24),
    schedule_interval='0 18 * * *',
    catchup=False,tags=["cimri"]) as dag:
    opp_request = PythonOperator(task_id ='get_link',python_callable=get_link,provide_context=True)
    opp_parse_json = PythonOperator(task_id = 'parse_json',python_callable=parse_json,provide_context=True)
    opp_prepare_query = PythonOperator(task_id='prepare_query',python_callable=prepare_query,provide_context=True)

    # yukarıdaki dag sonucunda kaydedilen scriptleri toplu bir şekilde postgresql dbsine insert işlemi gerçekleştirilir.
    opp_insert_db = PostgresOperator(
        task_id='opp_insert_db',
        postgres_conn_id = 'cimri_db',
        sql='product_schema.sql'
    )

opp_request >> opp_parse_json>>opp_prepare_query>>opp_insert_db
