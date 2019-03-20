from datetime import datetime
import elasticsearch
import csv
import unicodedata
from urllib.parse import urlparse
from hdfs import InsecureClient
import os
import json
from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

'''
сообщения помещаются в каталог hdfs://34.76.18.152:8020/tmp/yyyymmdd/yyyymmdd.json
где
yyyymmdd - это временная метка для файлов с данными, полученными за указанный день (тогда, когда был запущен скрипт).

а также сами данные загружаются в таблицу lab1db.lab1_messages в ClickHouse с использованием поставляемого Python-драйвера для работы с данной СУБД.


запуск самого Apache Airflow 1.9.0 в качестве демона выполняется следующим образом:

cd /home/airflow/workspace
export AIRFLOW_HOME=/home/airflow/workspace/airflow_home
source venv/bin/activate

airflow webserver --daemon
airflow scheduler --daemon

остановить процессы:

sudo kill $(ps -ef | grep "airflow webserver" | awk '{print $2}')
sudo kill $(ps -ef | grep "airflow scheduler" | awk '{print $2}')

подключение к ClickHouse

clickhouse-client --port 9011

use lab1db;
select count(1) from lab1_messages;


git clone https://github.com/dmitriy-voronko-2019/data_engineering_2019.git D:\lab1

'''

def my_dag_function():
  # хост со статически определённым IP
  hostname = '34.76.18.152'
  # порт ElasticSearch
  elk_port = 9200
  # порт HDFS
  hdfs_port = 50070

  # открываем соединение к ElasticSearch и получаем данные из индекса
  es = elasticsearch.Elasticsearch([hostname+":"+str(elk_port)])
  res = es.search(index="dmitriy.voronko", body = {"query" : {"range" : {"@timestamp": {"gte" : "now-15m", "lt" : "now"}}}}, size = 500)

  # получаем название директории текущего дня обработки данных.
  curr_dir_name = (datetime.now()).strftime("%Y%m%d")
  print("Directory for current date: " + curr_dir_name)

  # Открываем соединение с HDFS и проверяем есть ли там файл в той директории, куда мы будем писать данные. Если его нету, то создаём файл, чтобы запись в режиме append работала
  # без ошибок.
  client_hdfs = InsecureClient("http://"+hostname+":"+str(hdfs_port), user="hdfs")
  try:
    status = client_hdfs.status("/tmp/"+curr_dir_name+"/"+curr_dir_name+".json")
  except:
    client_hdfs.write("/tmp/"+curr_dir_name+"/"+curr_dir_name+".json", append=False, encoding="utf-8", data="")

  for doc in res['hits']['hits']:
    client_hdfs.write("/tmp/"+curr_dir_name+"/"+curr_dir_name+".json", encoding="utf-8", append=True, data=json.dumps(doc['_source']))

  # пишем данные в ClickHouse.
  # возможно не самый оптимальный вариант, связанный с тем, что каждое сообщение из ElasticSearch индекса раскладывается на переменные, из которых потом формируется
  # tuple, вставляемый в таблицу в ClickHouse.
  client = Client('localhost', port=9011)

  for doc in res['hits']['hits']:
      data = json.loads(doc['_source']['message'])
      timestamp_v = data['timestamp']
      referer_v = data['referer']
      location_v = data['location']
      remoteHost_v = data['remoteHost']
      partyId_v = data['partyId']
      sessionId_v = data['sessionId']
      pageViewId_v = data['pageViewId']
      eventType_v = data['eventType']
      item_id_v = data['item_id']
      item_price_v = int(data['item_price'])
      item_url_v = data['item_url']
      basket_price_v = None
      if data['basket_price'] != '':
          basket_price_v = data['basket_price']
      detectedDuplicate_ = 0
      if data['detectedDuplicate'] == 'true':
          detectedDuplicate_v = 1
      else:
        detectedDuplicate_v = 0
      detectedCorruption_v = 0
      if data['detectedCorruption'] == 'true':
          detectedCorruption_v = 1
      else:
        detectedCorruption_v = 0
      firstInSession_v = 0
      if data['firstInSession'] == 'true':
          firstInSession_v = 1
      else:
      else:
        firstInSession_v = 0
      userAgentName_v = data['userAgentName']
      client.execute('INSERT INTO lab1db.lab1_messages (timestamp_, referer_, location_, remoteHost_, partyId_, sessionId_, pageViewId_, eventType_, item_id_, item_price_, item_url_, basket_price_, detectedDuplicate_, detectedCorruption_,  firstInSession_, userAgentName_) VALUES', [(timestamp_v, referer_v, location_v, remoteHost_v, partyId_v, sessionId_v, pageViewId_v, eventType_v, item_id_v, item_price_v, item_url_v,  basket_price_v, detectedDuplicate_v, detectedCorruption_v, firstInSession_v, userAgentName_v)])
  return True

# запускаемый DAG работает каждые 15 минут.
dag = DAG('lab1_dag', description='Simple tutorial DAG',
          schedule_interval='*/15 * * * *',
          start_date=datetime(2019, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

lab1_dag_operator = PythonOperator(task_id='lab1_dag_task', python_callable=my_dag_function, dag=dag)

dummy_operator >> lab1_dag_operator	  