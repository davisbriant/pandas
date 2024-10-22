from pyathena import connect
import pandas as pd
import numpy as np
import os, sys
import yaml
import operator
from pprint import pprint

config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config/', 'pandas.yaml')
with open(config_file_path) as config_file:
    config = yaml.safe_load(config_file)

def filter_dataframe(df, column, threshold, operator):
	return df.query(f'{column} {operator} {threshold}').index.values.tolist()

def style_dataframe_ctr(x):
	if ((x['impressions'] > 100) & (x['ctr'] >= x['ctr benchmark'])):
		color = '#7dcea0'
	elif ((x['impressions'] > 100) & (x['ctr'] >= (x['ctr benchmark'] * 0.9))):
		color = '#f4d03f'
	elif (x['impressions'] > 100):
		color = '#e74c3c'
	else:
		color = ''
	background = ['background-color: {}'.format(color) for _ in x]
	return background

def getCtrBenchmarkPerDevice(df, device):
	df = df.loc[df['device'] == device].groupby(['date','device'])[['impressions', 'clicks']].sum()
	df['ctr'] = (df['clicks']/df['impressions']*100).fillna(0)
	df['ctr benchmark'] = df['ctr'].rolling(window=7).mean().fillna(0)
	df = df.iloc[6:]
	return df

def getAthenaData(query):
    try: 
        conn = connect(s3_staging_dir=config['athena']['s3_staging_dir'], aws_access_key_id=config['aws']['keyId'], aws_secret_access_key=config['aws']['secretAccessKey'], region_name=config['aws']['region'])
        response = pd.read_sql(query, conn)
        return response
        
    except Exception as e:
        print("Exception: {}".format(e))

query = "select date(date) as date, adid, device, sum(impressions) as impressions, sum(clicks) as clicks from dbt.mart_googleads_performancereport_ads where date(date) >= current_date - interval '30' day GROUP BY 1,2,3 order by date asc, adid asc, device asc"

#get ad performance data for the last 30 days
data = getAthenaData(query)
print(data.head())
#exclude rows where an adid did not have a minumum 0f 100 ad impressions per day or where the device type is not eligible for click activity
df = data.loc[(data['impressions'] >= 100) & (data['device'] != 'CONNECTED_TV')]
#define a list including the unique device values in the remaining rows
devices = df['device'].unique()
#for each value in the list, use a function to return the rolling seven day average click-thru rate per device type
dfs = []
for device in devices:
	ctrBenchmark = getCtrBenchmarkPerDevice(df, device)
	dfs.append(ctrBenchmark)
#consolidate the list of dfs created per device type
df = pd.concat(dfs)
#drop the duplicate column names before joining df to orignal query result dataframe
df = df.drop(columns=['impressions','clicks','ctr'])
#create a column for actual ctr metric per date, device, and adid
data = data.assign(ctr = data['clicks']/data['impressions']*100).fillna(value=0)
#join the ctr benchmarks by device df to the ad performance data df
df = data.merge(df, how='left', on=['date','device']).dropna().reset_index(drop=True)
df.index = df.index.set_names(['index'])
#export results to csv
df.to_csv(sep='\t', header=True, index=False, path_or_buf='ctrvsbenchmark.tsv')
#create a Styler object version of the df in preparation for export to html including a function that color codes rows with a minimum of 100 impressions based on how the actual ctr compares with the ctr benchmark by device type
df_styled = df.style.apply(style_dataframe_ctr, axis=1).format(thousands=',', hyperlinks={'html'}, precision=2).format({'ctr': '{:.2f}%','ctr benchmark': '{:.2f}%'})
#assign table attributes to leverage bootstrap 5 css classes
df_styled = df_styled.set_table_attributes('class="table table-bordered"').hide(axis=0)
#export the styled df to the webroot location of localhost for import via xhr into a parent html document in the same folder location
f = open('/var/www/html/ctrvsbenchmark.html', 'w')
f.write(df_styled.to_html(table_uuid='ctrvsbenchmark'))
f.close()



