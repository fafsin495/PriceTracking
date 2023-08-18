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
    url =[
    "https://www.cimri.com/market/konserve/en-ucuz-tukas-830-gr-domates-salcasi-fiyatlari,40673",
    "https://www.cimri.com/market/sivi-yag-ve-zeytinyagi/en-ucuz-yudum-5-lt-teneke-aycicek-yagi-fiyatlari,7",
    "https://www.cimri.com/market/toz-ve-kup-seker/en-ucuz-balkupu-5-kg-toz-seker-fiyatlari,51432",
    "https://www.cimri.com/market/tuz/en-ucuz-billur-750-gr-iyotlu-tuz-fiyatlari,50989",
    "https://www.cimri.com/market/bakliyat/en-ucuz-reis-baldo-2-5-kg-pirinc-fiyatlari,20079",
    "https://www.cimri.com/market/bakliyat/en-ucuz-duru-2-5-kg-pilavlik-bulgur-fiyatlari,20509",
    "https://www.cimri.com/market/bakliyat/en-ucuz-yayla-1-kg-yayla-kirmizi-mercimek-fiyatlari,20393",
    "https://www.cimri.com/market/bakliyat/en-ucuz-duru-502-8-mm-1000-gr-kocbasi-nohut-fiyatlari,20408",
    "https://www.cimri.com/market/bakliyat/en-ucuz-migros-1000-gr-dermason-kuru-fasulye-fiyatlari,20798",
    "https://www.cimri.com/market/bakliyat/en-ucuz-yayla-1-kg-yesil-mercimek-fiyatlari,20402",
    "https://www.cimri.com/market/bakliyat/en-ucuz-yayla-1-kg-barbunya-fiyatlari,20378",
    "https://www.cimri.com/market/un-ve-irmik/en-ucuz-soke-5-kg-geleneksel-un-fiyatlari,36625",
    "https://www.cimri.com/market/un-ve-irmik/en-ucuz-filiz-500-gr-irmik-fiyatlari,35199",
    "https://www.cimri.com/market/makarnalar/en-ucuz-ankara-burgu-500-gr-makarna-fiyatlari,42483",
    "https://www.cimri.com/market/makarnalar/en-ucuz-filiz-yassi-spagetti-500-gr-makarna-fiyatlari,43344",
    "https://www.cimri.com/market/sivi-yag-ve-zeytinyagi/en-ucuz-komili-riviera-sicak-lezzetler-5-lt-zeytinyagi-fiyatlari,573",
    "https://www.cimri.com/market/unlu-mamuller/en-ucuz-uno-670-gr-buyuk-tost-ekmegi-fiyatlari,52148",
    "https://www.cimri.com/market/peynir/en-ucuz-sutas-500-gr-suzme-peynir-fiyatlari,16445",
    "https://www.cimri.com/market/peynir/en-ucuz-sutas-600-gr-kasar-peyniri-fiyatlari,16718",
    "https://www.cimri.com/market/peynir/en-ucuz-sutas-labne-400-gr-peynir-fiyatlari,16441",
    "https://www.cimri.com/market/peynir/en-ucuz-sutas-tam-yagli-500-gr-beyaz-peynir-fiyatlari,16442",
    "https://www.cimri.com/market/bal-ve-recel/en-ucuz-balparmak-850-gr-suzme-cicek-bali-fiyatlari,39705",
    "https://www.cimri.com/market/tahin-pekmez-ve-helva/en-ucuz-koska-550-gr-bidon-tahin-fiyatlari,52418",
    "https://www.cimri.com/market/tahin-pekmez-ve-helva/en-ucuz-koska-700-gr-uzum-pekmezi-fiyatlari,44252",
    "https://www.cimri.com/market/tahin-pekmez-ve-helva/en-ucuz-koska-kakaolu-500-gr-tahin-helvasi-fiyatlari,66735",
    "https://www.cimri.com/market/zeytin/en-ucuz-marmarabirlik-kuru-sele-xs-321-350-400-gr-siyah-zeytin-fiyatlari,50133",
    "https://www.cimri.com/market/surulebilir/en-ucuz-nutella-750-gr-kakaolu-findik-krem-cikolata-fiyatlari,19381",
    "https://www.cimri.com/market/tereyag-ve-margarin/en-ucuz-icim-1-kg-tereyagi-fiyatlari,18396",
    "https://www.cimri.com/market/tereyag-ve-margarin/en-ucuz-bizim-ulker-250-gr-margarin-fiyatlari,711133",
    "https://www.cimri.com/market/sut/en-ucuz-pinar-1-lt-yarim-yagli-sut-fiyatlari,16750",
    "https://www.cimri.com/market/yumurta/en-ucuz-cp-53-62-gr-30-lu-yumurta-fiyatlari,53224",
    "https://www.cimri.com/market/yogurt/en-ucuz-sutas-2000-gr-kaymaksiz-yogurt-fiyatlari,16819",
    "https://www.cimri.com/market/kaymak/en-ucuz-eker-200-gr-kaymak-fiyatlari,16495",
    "https://www.cimri.com/market/sebze/en-ucuz-domates-fiyatlari,120718",
    "https://www.cimri.com/market/sebze/en-ucuz-patates-fiyatlari,53318",
    "https://www.cimri.com/market/meyve/en-ucuz-yerli-muz-fiyatlari,54126",
    "https://www.cimri.com/market/sebze/en-ucuz-kuru-sogan-fiyatlari,53345",
    "https://www.cimri.com/market/meyve/en-ucuz-limon-fiyatlari,116833",
    "https://www.cimri.com/market/sebze/en-ucuz-sarimsak-fiyatlari,135625",
    "https://www.cimri.com/market/sebze/en-ucuz-sivri-biber-fiyatlari,120681",
    "https://www.cimri.com/market/sebze/en-ucuz-salatalik-fiyatlari,135605",
    "https://www.cimri.com/market/sebze/en-ucuz-maydanoz-bag--fiyatlari,135654",
    "https://www.cimri.com/market/sebze/en-ucuz-kemer-patlican-fiyatlari,120667",
    "https://www.cimri.com/market/cay/en-ucuz-caykur-tiryaki-1-kg-cay-fiyatlari,54363",
    "https://www.cimri.com/market/kahve/en-ucuz-kuru-kahveci-mehmet-efendi-100-gr-turk-kahvesi-fiyatlari,59916",
    "https://www.cimri.com/market/su/en-ucuz-erikli-5-lt-su-fiyatlari,63285",
    "https://www.cimri.com/market/ayran/en-ucuz-sutas-1-lt-ayran-fiyatlari,63384",
    "https://www.cimri.com/market/sarkuteri/en-ucuz-cumhuriyet-fermente-300-gr-dana-kangal-sucuk-fiyatlari,65627",
    "https://www.cimri.com/market/kirmizi-et/en-ucuz-mopas-500-gr-dana-kusbasi-fiyatlari,551615",
    "https://www.cimri.com/market/kirmizi-et/en-ucuz-mopas-500-gr-dana-bonfile-fiyatlari,551602",
    "https://www.cimri.com/market/kirmizi-et/en-ucuz-mopas-500-gr-sut-kuzu-pirzola-fiyatlari,551599",
    "https://www.cimri.com/market/beyaz-et/en-ucuz-banvit-1-kg-pilic-baget-fiyatlari,66460",
    "https://www.cimri.com/market/beyaz-et/en-ucuz-banvit-1-kg-pilic-pirzola-fiyatlari,66469",
    "https://www.cimri.com/market/beyaz-et/en-ucuz-banvit-1-kg-pilic-kanat-fiyatlari,66441",
    "https://www.cimri.com/market/beyaz-et/en-ucuz-banvit-1850-gr-butun-pilic-fiyatlari,944708",
    "https://www.cimri.com/market/tuvalet-kagidi/en-ucuz-familia-32-li-tuvalet-kagidi-fiyatlari,85180",
    "https://www.cimri.com/market/bulasik-deterjani/en-ucuz-fairy-650-ml-limon-elde-yikama-bulasik-deterjani-fiyatlari,83990",
    "https://www.cimri.com/market/camasir-deterjani/en-ucuz-bingo-matik-mutlu-yuvam-10-kg-renkliler-ve-beyazlar-toz-camasir-deterjani-fiyatlari,699672",
    "https://www.cimri.com/market/cop-posetleri/en-ucuz-koroplast-15-li-yasemin-buzgulu-cop-torbasi-fiyatlari,88636",
    ]
    json_result =[]
    for i in url:
        try:
            time.sleep(1)
            
            headers ={
                    # 'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    # 'Accept-Language':'en-GB,en;q=0.9,tr-TR;q=0.8,tr;q=0.7,en-US;q=0.6',
                    # 'referer':'http://localhost:8080/',
                    # 'cookie': 'cimri_location=; cimriSlot=SLOT-CONTROL; OfferMiner_ID=LAEZCLMXCHBYLONU20220916133539; scarab.visitor=%22468F6942B1E42576%22; _hjSessionUser_195245=eyJpZCI6Ijc5ODMyYWU5LWJkMmItNWM2Zi04OTgzLTk0MGNmMTg4YTcyNCIsImNyZWF0ZWQiOjE2NjMzMjQ1NDAzODEsImV4aXN0aW5nIjp0cnVlfQ==; _fbp=fb.1.1664438772298.623455218; _gcl_aw=GCL.1667152885.Cj0KCQjwwfiaBhC7ARIsAGvcPe7d5RJnz_Ui60yJHLk3x-TWviVCYjSShxyX0rq0CBkmni9UieH8zbEaArkoEALw_wcB; _gcl_dc=GCL.1667152885.Cj0KCQjwwfiaBhC7ARIsAGvcPe7d5RJnz_Ui60yJHLk3x-TWviVCYjSShxyX0rq0CBkmni9UieH8zbEaArkoEALw_wcB; _gac_UA-4786420-1=1.1667152887.Cj0KCQjwwfiaBhC7ARIsAGvcPe7d5RJnz_Ui60yJHLk3x-TWviVCYjSShxyX0rq0CBkmni9UieH8zbEaArkoEALw_wcB; cimri_device_id=37c35cf0-0c76-40ab-b244-a8d934d41507; scarab.profile=%22101726024%7C1667154537%22; _ga=GA1.1.2058295970.1663324540; _gac_UA-4786420-1=1.1667152887.Cj0KCQjwwfiaBhC7ARIsAGvcPe7d5RJnz_Ui60yJHLk3x-TWviVCYjSShxyX0rq0CBkmni9UieH8zbEaArkoEALw_wcB; cto_bundle=nfTgwl9YSEZ1cTZYMERnMEgwMlRaS3hBYlBsdGZHTERKY1o4VFRNeFMzemJKOWRmZ2paZGdtb0ZWajN2UEJPeTBCJTJGYjZqQyUyRlczUkhHUHlGUTBSQUV5V1dXelFNZzROYVpLZWpXSUhRbm15JTJGa2JVczdEUkpVVmoyNXl3Y3VoWSUyQmF5R1l2NHV5MTRXSmYlMkJrNSUyRkNYMyUyRjc0THNRUSUzRCUzRA; _gcl_au=1.1.1942603377.1671199649; _gid=GA1.2.1451042573.1671199650; _ga_0W7MRKJ2XP=GS1.1.1671199650.39.0.1671199650.0.0.0; _dc_gtm_UA-4786420-1=1; _ga=GA1.2.2058295970.1663324540; __cf_bm=KBqSlKtkP0fsCPY44AMhB4G6nk_RYkhPh.TNbfT0XnM-1671199650-0-AWr6+YG7Y/IR6OidOPabtOsZ/bv+pha8WyxBkxvQYyvxBOg9UQOoiGpcqjKu3eF54oAlhHl3aUk0KhtC56+g/4VXoAfqIftQhOPySbWeMLoQZvnJqAwfOzQ/AFfy0ickT1r5GXrPLGcYkH5YKUijhbc=; _hjIncludedInSessionSample=0; _hjSession_195245=eyJpZCI6IjIwYjQ3YTk4LTk4ZWItNGNiMi1hZmY5LWEyNDkzYTRkYWEzOCIsImNyZWF0ZWQiOjE2NzExOTk2NjkxOTUsImluU2FtcGxlIjpmYWxzZX0=; _hjIncludedInPageviewSample=1; _hjAbsoluteSessionInProgress=0'
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
