import json 
import pandas as pd 
print(pd.__version__)
from pandas.io.json import json_normalize #package for flattening json in pandas df

#load json object
with open('json/5-minute-24-hr.json') as f:
    d = json.load(f)

#lets put the data into a pandas df
#clicking on raw_nyc_phil.json under "Input Files"
#tells us parent node is 'programs'
data = json_normalize(d["5-minute"])
df = pd.DataFrame(data)
df2 = pd.DataFrame()
for x in range(1):
	print(x)
	df2 = df2.append(df)


tuples = [tuple(x) for x in df.to_numpy()]
# Comma-separated dataframe columns
cols = ','.join(list(df.columns))
# SQL quert to execute
query  = "INSERT INTO %s(%s) VALUES %%s" % ('table', cols)

for x in tuples:
	print query % str(x)

df2.to_csv('csv/5min_24hr_output.csv', index=False)