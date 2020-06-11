import json 
import os
import pandas as pd 
print(pd.__version__)

from pandas.io.json import json_normalize #package for flattening json in pandas df
import db.timescaledb as db

param_dic = {
    "host"      : "127.0.0.1",
    "database"  : "keycloak",
    "user"      : "keycloak",
    "password"  : "password"
}

conn = db.connect(param_dic)

def bulk_load_to_timescaledb(file_path, conn, device_id):
	# load json object
	with open(file_path) as f:
	    d = json.load(f)

	# put the data into a pandas df
	data = json_normalize(d["5-minute"], sep='_')
	df = pd.DataFrame(data)
	df = df.drop(columns=['end'])
	df['device_id'] = device_id
	df = df.drop_duplicates(subset=['start'], keep='first')

	# bulk write to timescaledb
	db.execute_values(conn, df, "pwrview")

	# write dataframe to csv
	#df.to_csv('csv/5min_24hr_output.csv', index=False)

if __name__ == "__main__":  	

	path='json/beacon/'
	for beacon_dir in os.listdir(path):
		beacon_path = path + beacon_dir + '/'
		for time_dir in os.listdir(beacon_path):
			file_path = beacon_path+time_dir+'/5-minute-24-hr.json'
			print(file_path)
			bulk_load_to_timescaledb(file_path, conn, beacon_dir)
