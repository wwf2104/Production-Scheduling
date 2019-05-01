# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 13:30:32 2019

@author: Winnie
"""

# Imports
import pandas as pd
import os
import datetime as dt

# Assuming JSON files are in the same dir as this file
# and there are no other .json files

files = os.listdir('data files')
files = ['data files/'+fname for fname in files if '.json' in fname]
results = pd.DataFrame()
for f in files:
    results = results.append(pd.read_json(f))

get_col = ['cmplnt_fr_tm','cmplnt_to_dt','cmplnt_to_tm',
                         'longitude','latitude','ofns_desc','law_cat_cd']
results = results[get_col].dropna()
filter_cols = {'cmplnt_to_dt': 'rj_date','cmplnt_fr_tm':'rj_time',
               'cmplnt_to_tm':'Cj','longitude':'lat','latitude':'long',
               'ofns_desc':'ofns_desc','law_cat_cd':'law_cat_cd'}
results.rename(filter_cols, axis=1, inplace=True)
results['Cj'].replace('24:00:00','00:00:00',inplace=True)

results['Cj'] = pd.to_datetime(results['Cj']).apply(lambda x: x.time())
results['rj_date'] = pd.to_datetime(results['rj_date']).apply(lambda x: x.date())
results['rj_time'] = pd.to_datetime(results['rj_time']).apply(lambda x: x.time())
tups1 = list(zip(results['rj_date'].tolist(), results['rj_time'].tolist()))
tups2 = list(zip(results['rj_date'].tolist(), results['Cj'].tolist()))
results['rj'] = [pd.datetime.combine(*i) for i in tups1]
results['Cj'] = [pd.datetime.combine(*i) for i in tups2]

results['pj'] = results['Cj'] - results['rj']
results = results[results['pj'].apply(lambda x: '-' not in str(x))]
results.drop(['rj_date','rj_time'], axis=1, inplace=True)

results.index = list(range(len(results)))
results['pj'] = results['pj'].apply(lambda x: str(x))
results['rj'] = results['rj'].apply(lambda x: str(x))
results['Cj'] = results['Cj'].apply(lambda x: str(x))
results.to_json('data files/all_data.json', orient='records')

