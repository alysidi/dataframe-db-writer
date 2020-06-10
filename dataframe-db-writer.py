import json 
import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df
import db.timescaledb as db

param_dic = {
    "host"      : "127.0.0.1",
    "database"  : "keycloak",
    "user"      : "keycloak",
    "password"  : "password"
}


#load json object
with open('json/5-minute-24-hr.json') as f:
    d = json.load(f)

#put the data into a pandas df
data = json_normalize(d["5-minute"], sep='_')
df = pd.DataFrame(data)
df = df.drop(columns=['end'])
df['device_id'] = 'device1'

conn = db.connect(param_dic)
db.execute_values(conn, df, "pwrview")

df.to_csv('csv/5min_24hr_output.csv', index=False)