from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import warnings
import requests
warnings.filterwarnings('ignore')
import json
from datetime import datetime
import time
import logging
import urllib

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.operators.email import EmailOperator


# bu metot içerisinde verilen linklere request sorgusu atılıp sorgu sonucu alınan verileri json formatında kaydeder.
def mopas_get_transform_insert_data(**kwargs):
    url =[
    "https://mopas.com.tr/uno-buyuk-tost-ekmek-550-gr/p/857707",
    "https://mopas.com.tr/sutas-suzme-peynir-500-gr/p/102551",
    "https://mopas.com.tr/sutas-kaymaksiz-yogurt-2000-gr/p/704590",
    "https://mopas.com.tr/kumbasar-yumurta-medium-boy-30lu/p/78915",
    "https://mopas.com.tr/sut-kuzu-pirzola/p/1410",
    "https://mopas.com.tr/dana-bonfile/p/1364",
    "https://mopas.com.tr/dana-kusbasi/p/1368",
    "https://mopas.com.tr/bizim-margarin-paket-250-gr/p/2037",
    "https://mopas.com.tr/beypilic-posetli-butun-pilic/p/1437",
    "https://mopas.com.tr/pastavilla-spaghetti-500-gr/p/3008",
    "https://mopas.com.tr/senpilic-baget/p/858577",
    "https://mopas.com.tr/koroplast-buzdolabi-poseti-20x30-4-al-3-ode/p/126819",
    "https://mopas.com.tr/sutas-tam-yagli-beyaz-peynir-500-gr/p/47790",
    "https://mopas.com.tr/senpilic-derili-pirzola/p/858576",
    "https://mopas.com.tr/senpilic-izgara-kanat/p/858579",
    "https://mopas.com.tr/reis-dermason-fasulye-1-kg/p/61567",
    "https://mopas.com.tr/patlican/p/33017",
    "https://mopas.com.tr/hunkar-nohut-1-kg/p/97542",
    "https://mopas.com.tr/sutas-ayran-1-l/p/428"
    
    ]
    minPrice=[]
    date = []
    p_title = []
    # current_time = str(datetime.now().strftime("%Y-%m-%d")) 
    # current_time = "2023-08-15"
    




    p_id = []
    p_url =[]
    product_ids=["52148",
                "16445",
                "16819",
                "53224",
                "551599",
                "551602",
                "551615",
                "711133",
                "944708",
                "43344",
                "66460",
                "584735",
                "16442",
                "66469",
                "66441",
                "20798",
                "120667",
                "20408",
                "63384"
                ]
    
    for i in url:
            
            time.sleep(1)
            getRequest = urlopen(i)
            html = BeautifulSoup(getRequest,'html.parser')


            minPrice.append(float(html.find('div',{'class':'price-container'}).find('span',{'class','sale-price'}).text[1:].replace(',','.')))
            p_title.append(str(i.split('/')[3]))
            date.append(current_time)
            p_url.append(str(i))
            p_id.append(i.split('/')[5])

    print(minPrice[9])
    print("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[9], url[9],product_ids[9],date[0]))
    queryFile= open('/opt/airflow/dags/sql/mopas_to_product_schema.sql',"w",encoding='utf-8')
    queryFile.seek(0,0)
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[0], url[0],product_ids[0],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[1], url[1],product_ids[1],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[2], url[2],product_ids[2],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[3], url[3],product_ids[3],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[4], url[4],product_ids[4],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[5], url[5],product_ids[5],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[6], url[6],product_ids[6],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[7], url[7],product_ids[7],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[8], url[8],product_ids[8],date[0]))
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[9], url[9],product_ids[9],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[10], url[10],product_ids[10],date[0]))      
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[12], url[12],product_ids[12],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[13], url[13],product_ids[13],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[14], url[14],product_ids[14],date[0]))
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[15], url[15],product_ids[15],date[0]))  
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[16]*2, url[16],product_ids[16],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[17], url[17],product_ids[17],date[0]))       
    queryFile.write("UPDATE product3 SET product_price  = %s ,product_url='%s' WHERE product_id='%s' and product_date ='%s'  ;\n"  %(minPrice[18], url[18],product_ids[18],date[0]))       

      
    queryFile.write("INSERT INTO product3 (product_title,product_category,product_price,product_date,product_id,product_url)VALUES ('Koroplast Buzdolabı Poşeti Orta Boy 4 Al 3 Öde', 'Buzdolabı Poşeti', %s, '%s','87825','https://mopas.com.tr/koroplast-buzdolabi-poseti-20x30-4-al-3-ode/p/126819');"%(minPrice[11],date[0]))
    



    queryFile.close()


    forex_currencies= open('/opt/airflow/dags/sql/mopas_product_schema.sql',"w",encoding='utf-8')
    forex_currencies.seek(0,0)

    array_lenght=len(p_title)
    for j in range(array_lenght):
        dates= datetime.strptime(str(date[j])[:10], '%Y-%m-%d').date()
        minPrices = float(minPrice[j])
        forex_currencies.write("insert into productMopas (product_title_m,product_price_m,product_date_m,product_id_m,product_url_m) values ('%s' ,%s,'%s','%s','%s');\n" %(p_title[j],minPrices,dates,p_url[j],p_id[j]))
    forex_currencies.close()
    
def trendyol_get_transform_insert_data(**kwargs):

    url =["https://www.trendyol.com/sr?q=Komili%20Riviera%20Zeytinya%C4%9F%C4%B1%205%20lt&qt=Komili%20Riviera%20Zeytinya%C4%9F%C4%B1%205%20lt&st=Komili%20Riviera%20Zeytinya%C4%9F%C4%B1%205%20lt&os=1"]
    minPrice=[]
    date = []
    p_title = []
    # current_time = str(datetime.now().strftime("%Y-%m-%d")) 
    # current_time = "2023-08-15"




    p_id = []
    p_url =[]
    product_ids =["573"]
    for index,i in enumerate(url):
            time.sleep(1)
            getRequest = requests.get(i)
            html = BeautifulSoup(getRequest.text,'html.parser')


            minPrice.append(float(html.find_all('div',{'class','prc-box-dscntd'})[0].text.split(' ')[0].replace(',','.')))
            p_title.append(str(html.find_all('span',{'class','prdct-desc-cntnr-name hasRatings'})[0].text))
            date.append(current_time)
            p_url.append(str(i))
            p_id.append(int(product_ids[index]))
    
    queryFile= open('/opt/airflow/dags/sql/mopas_to_product_schema.sql',"w",encoding='utf-8')
    # queryFile.seek(0,0)
    queryFile.write("UPDATE product3 SET product_price  = %s  , product_url='%s' WHERE product_id='%s' and product_date ='%s' ;\n"  %(minPrice[0],p_url[0],product_ids[0],date[0]))   
    queryFile.close()


    forex_currencies= open('/opt/airflow/dags/sql/mopas_product_schema.sql',"w",encoding='utf-8')
    forex_currencies.seek(0,0)
    array_lenght=len(p_title)
    for j in range(array_lenght):
        dates= datetime.strptime(str(date[j])[:10], '%Y-%m-%d').date()
        minPrices = float(minPrice[j])
        forex_currencies.write("insert into productMopas (product_title_m,product_price_m,product_date_m,product_id_m,product_url_m) values ('%s' ,%s,'%s','%s','%s');\n" %(p_title[j],minPrices,dates,p_url[j],p_id[j]))
    forex_currencies.close()

with DAG('mopas_Urun' ,start_date = datetime(2022,9,24),
    schedule_interval='15 18 * * *',
    # schedule_interval='@daily',
    catchup=False,tags=["cimri"]) as dag:
    opp_mopas_all = PythonOperator(task_id ='mopas_get_transform_insert_data',python_callable=mopas_get_transform_insert_data,provide_context=True)
    opp_trendyol_all = PythonOperator(task_id ='trendyol_get_transform_insert_data',python_callable=trendyol_get_transform_insert_data,provide_context=True)
    # yukarıdaki dag sonucunda kaydedilen scriptleri toplu bir şekilde postgresql dbsine insert işlemi gerçekleştirilir.
    opp_insert_db = PostgresOperator(
        task_id='opp_insert_db',
        postgres_conn_id = 'cimri_db',
        sql='mopas_product_schema.sql'
    )
    opp_update_product3_db = PostgresOperator(
        task_id='opp_update_product3_db',
        postgres_conn_id = 'cimri_db',
        sql='mopas_to_product_schema.sql'
    )
    opp_trendyol = PostgresOperator(
        task_id = 'opp_trendyol',
        postgres_conn_id = 'cimri_db',
        sql='mopas_to_product_schema.sql'
    )

    opp_update_db = PostgresOperator(
        task_id='opp_update_db',
        postgres_conn_id = 'cimri_db',
        sql='updateQuery.sql'
    )

    # send_email = EmailOperator( 
    #     task_id='emailoperator_demo', 
    #     to='afsinfatih21@gmail.com', 
    #     subject='Mopas Web Scraping Succesful', 
    #     html_content="Date: OLDu", 
    # )

opp_mopas_all >> opp_insert_db>>opp_update_product3_db>>opp_trendyol_all>>opp_trendyol>>opp_update_db