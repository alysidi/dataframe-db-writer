import json 
import os
import pandas as pd 
import random
print(pd.__version__)

from pandas.io.json import json_normalize #package for flattening json in pandas df
import db.timescaledb as db

'''
param_dic = {
    "host"      : "tsdb-164cde20-generac-38fe.a.timescaledb.io",
    "database"  : "defaultdb",
    "user"      : "tsdbadmin",
    "password"  : "sakeocmytuetzamt",
    "port"			: 12949
}
'''
param_dic = {
    "host"      : "127.0.0.1",
    "database"  : "keycloak",
    "user"      : "keycloak",
    "password"  : "password",
    "port"			: 5432
}

#yoyo apply --database postgresql://tsdbadmin:sakeocmytuetzamt@tsdb-164cde20-generac-38fe.a.timescaledb.io:12949/defaultdb ./migrations/legacy-status-updates
#yoyo rollback --database postgresql://tsdbadmin:sakeocmytuetzamt@tsdb-164cde20-generac-38fe.a.timescaledb.io:12949/defaultdb ./migrations/legacy-status-updates

conn = db.connect(param_dic)

def bulk_load_to_timescaledb(conn, dataframe):
	
	# bulk write to timescaledb
	db.execute_values(conn, df, "status.legacy_status")

	# write dataframe to csv
	#df.to_csv('csv/5min_24hr_output.csv', index=False)

if __name__ == "__main__":
	data = pd.read_csv('csv/legacy-status-one-day.csv')
	for x in range(1000):
		rand = int(random.random()*10000)
		df = pd.DataFrame(data)
		df['device_id'] = df['device_id'] + rand
		#bulk_load_to_timescaledb(conn, df)
		db.copy_from_stringio(conn, df, "status.legacy_status")
		print(x)
	
	
