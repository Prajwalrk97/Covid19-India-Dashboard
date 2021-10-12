import requests,json
from kafka import KafkaProducer
import time
from json import dumps

time_series = "https://api.covid19india.org/v4/min/timeseries.min.json"	#Daily numbers across C,R,D and Tested per state (historical data).
district_state = "https://api.covid19india.org/v4/min/data.min.json"	#Current day numbers across districts and states.
states = "https://api.covid19india.org/csv/latest/states.csv"#Statewise timeseries of Confirmed, Recovered and Deceased numbers.
districts = "https://api.covid19india.org/csv/latest/districts.csv" #Districtwise timeseries of Confirmed, Recovered and Deceased numbers.
state_wise_daily="https://api.covid19india.org/csv/latest/state_wise_daily.csv" #Statewise per day delta of Confirmed, Recovered and Deceased numbers.
state_wise="https://api.covid19india.org/csv/latest/state_wise.csv" #Statewise cumulative numbers till date.
district_wise="https://api.covid19india.org/csv/latest/district_wise.csv"	#Districtwise Cumulative numbers till date.
vaccine_doses_administered_statewise="http://api.covid19india.org/csv/latest/vaccine_doses_statewise_v2.csv"#Number of vaccine doses administered statewise - Collected from MOHFW daily bulletin
cowin_vaccine_data_statewise="http://api.covid19india.org/csv/latest/cowin_vaccine_data_statewise.csv"	#Key data points from CoWin database at a state level
cowin_vaccine_data_districtwise="http://api.covid19india.org/csv/latest/cowin_vaccine_data_districtwise.csv"	#Key data points from CoWin database at a district level

response = requests.request("POST", time_series)
fields = ["confirmed","deceased","recovered","tested","vaccinated1","vaccinated2"]
KAFKA_TOPIC_NAME_CONS="kar_stats_daily"
KAFKA_BOOTSTRAP_SERVERS_CONS = '172.19.0.5:9093'
kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,value_serializer=lambda x:dumps(x).encode('utf-8'))

states={'AN': 'Andaman and Nicobar Islands', 'AP': 'Andhra Pradesh', 'AR': 'Arunachal Pradesh', 'AS': 'Assam', 'BR': 'Bihar',\
        'CH': 'Chandigarh', 'CT': 'Chhattisgarh', 'DN': 'Dadra and Nagar Haveli and Daman and Diu', 'DL': 'Delhi', 'GA': 'Goa',\
        'GJ': 'Gujarat', 'HR': 'Haryana', 'HP': 'Himachal Pradesh', 'JK': 'Jammu and Kashmir', 'JH': 'Jharkhand', 'KA': 'Karnataka',\
        'KL': 'Kerala', 'LA': 'Ladakh', 'LD': 'Lakshadweep', 'MP': 'Madhya Pradesh', 'MH': 'Maharashtra', 'MN': 'Manipur', 'ML': 'Meghalaya',\
        'MZ': 'Mizoram', 'NL': 'Nagaland', 'OR': 'Odisha', 'PY': 'Puducherry', 'PB': 'Punjab', 'RJ': 'Rajasthan', 'SK': 'Sikkim', 'TN': 'Tamil Nadu',\
        'TG': 'Telangana', 'TR': 'Tripura', 'UP': 'Uttar Pradesh', 'UT': 'Uttarakhand', 'WB': 'West Bengal'} 

india_daily_stats={}
json_data = json.loads(response.text)
count=0
print("Kafka producer started")
if __name__ == '__main__':
    for state in states.keys():
        india_daily_stats['state'] = states[state]
        for date in json_data[state]['dates']:
            india_daily_stats['date'] = date
            for field in fields:
                #Daily
                try:
                    india_daily_stats[field] = json_data[state]['dates'][date]['delta'][field]
                except:
                    india_daily_stats[field] = 0
            
                #7 Day rolling window
                try:
                    india_daily_stats[field+"_week_window"] = json_data[state]['dates'][date]['delta7'][field]
                except:
                    india_daily_stats[field+"_week_window"] = 0
                
                #Total
                try:
                    india_daily_stats[field+"_total"] = json_data[state]['dates'][date]['total'][field]
                except:
                    india_daily_stats[field+"_total"] = 0

            try:
                india_daily_stats['tpr'] = round(india_daily_stats["confirmed"]*100/india_daily_stats['tested'],2)
            except:
                    india_daily_stats["tpr"] = 0
            try:
                india_daily_stats['cfr'] = round(india_daily_stats["deceased"]*100/india_daily_stats['tested'],2)
            except:
                    india_daily_stats["cfr"] = 0

            try:
                india_daily_stats['tpr_week_window'] = round(india_daily_stats["confirmed_week_window"]*100/india_daily_stats['tested_week_window'],2)
            except:
                    india_daily_stats["tpr_week_window"] = 0
            try:
                india_daily_stats['cfr_week_window'] = round(india_daily_stats["deceased_week_window"]*100/india_daily_stats['tested_week_window'],2)
            except:
                    india_daily_stats["cfr_week_window"] = 0

            # try:
            #     india_daily_stats['tpr_total'] = round(india_daily_stats["confirmed_total"]*100/india_daily_stats['tested_total'],2)
            # except:
            #         india_daily_stats["tpr_total"] = 0
            # try:
            #     india_daily_stats['cfr_total'] = round(india_daily_stats["deceased_total"]*100/india_daily_stats['tested_total'],2)
            # except:
            #         india_daily_stats["cfr_total"] = 0

            print(india_daily_stats)
            count+=1
            kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS,india_daily_stats)
            time.sleep(0.001)
    print("Total Records Streamed - ",count)






# state_codes=['AN', 'AP', 'AR', 'AS', 'BR', 'CH', 'CT', 'DN', 'DL', 'GA', 'GJ', 'HR',\
#                  'HP', 'JK', 'JH', 'KA', 'KL', 'LA', 'LD', 'MP', 'MH', 'MN', 'ML', 'MZ',\
#                 'NL', 'OR', 'PY', 'PB', 'RJ', 'SK', 'TN', 'TG', 'TR', 'UP', 'UT', 'WB']

# state_names = ['Andaman and Nicobar Islands', 'Andhra Pradesh',\
#        'Arunachal Pradesh', 'Assam', 'Bihar', 'Chandigarh',\
#        'Chhattisgarh', 'Dadra and Nagar Haveli and Daman and Diu',\
#        'Delhi', 'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh',\
#        'Jammu and Kashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh',\
#        'Lakshadweep', 'Madhya Pradesh', 'Maharashtra', 'Manipur',\
#        'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Puducherry',\
#        'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana',\
#        'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal']