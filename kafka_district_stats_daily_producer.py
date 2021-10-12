import requests,json
from kafka import KafkaProducer
import time
from json import dumps
import csv 
KAFKA_TOPIC_NAME_CONS="district_stats_daily"
KAFKA_BOOTSTRAP_SERVERS_CONS = '172.19.0.5:9093'
kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,value_serializer=lambda x:dumps(x).encode('utf-8'))

district_state = "https://api.covid19india.org/csv/latest/districts.csv"
india_district_daily_stats={}

response = requests.request("POST", district_state)
data =response.text.splitlines()
count =0
print("Kafka producer started")
if __name__ == '__main__':
    for i in range(1,len(data)):
        date, state, district, confirmed_total, recovered_total, deceased_total, other_total, tested_total = data[i].split(',')
        india_district_daily_stats['date'] = date
        india_district_daily_stats['state'] = state
        india_district_daily_stats['district'] = district

        if confirmed_total == '':
            india_district_daily_stats['confirmed_total'] = '0'
        else:
            india_district_daily_stats['confirmed_total'] = confirmed_total

        if recovered_total == '':
            india_district_daily_stats['recovered_total'] = '0'
        else:
            india_district_daily_stats['recovered_total'] = recovered_total

        if deceased_total == '':
            india_district_daily_stats['deceased_total'] = '0'
        else:
            india_district_daily_stats['deceased_total'] = deceased_total

        if other_total == '':
            india_district_daily_stats['other_total'] = '0'
        else:
            india_district_daily_stats['other_total'] = other_total
            
        if tested_total == '':
            india_district_daily_stats['tested_total'] = '0'
        else:
            india_district_daily_stats['tested_total'] = tested_total

        print(india_district_daily_stats)
        count+=1
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS,india_district_daily_stats)
        time.sleep(0.00001)
    print("Total Records Streamed - ",count)
