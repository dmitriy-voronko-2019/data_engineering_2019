import warnings
import pprint
import json
import os
import time
import random
from kafka import KafkaProducer
from time import sleep


def rows_to_dict_list(cursor):
    columns = [str(i[0]).lower() for i in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]

def get_message_json():
    value_ = {"timestamp":1540817494016, "referer":"https://b24-d2wt09.bitrix24.shop/katalog/item/dress-spring-ease/", "location":"https://b24-d2wt09.bitrix24.shop/", "remoteHost":"test0", "partyId":"0:jmz8xo3z:BvkQdVYMXSAUTS42oNOW8Yg7kSdkkdHl", "sessionId":"0:jnu6ph44:kxtaa4hPKImjpoyQSgSYCuzoFOMrHA4f", "pageViewId":"0:PvFIq25Zl1esub4LGUQ75xAuoYH0XAlj", "eventType":"itemViewEvent", "item_id":"bx_40480796_7_52eccb44ded0bb34f72b273e9a62ef02", "item_price":"2331", "item_url":"https://b24-d2wt09.bitrix24.shop/katalog/item/t-shirt-mens-purity/", "basket_price":"", "detectedDuplicate":False, "detectedCorruption":False, "firstInSession":True, "userAgentName":"TEST_UserAgentName" }
    return value_;


warnings.filterwarnings("ignore")
os.environ['NLS_LANG'] = 'American_America.AL32UTF8'

producer = KafkaProducer(bootstrap_servers=['34.76.18.152:6667'], value_serializer = lambda v: json.dumps(v).encode('utf-8'))

for x in range(10):
    producer.send('dmitriy.voronko', value=get_message_json())
    sleep(1)
    print("successfull send message to kafka broker!")