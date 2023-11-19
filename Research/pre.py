"""
Module Docstring
python pre.py
"""

__author__ = "msb.net.in@gmail.com"
__version__ = "0.1.0"
__license__ = "MIT" 



#import configparser
import os
import re
import datetime
import sys, traceback
import logging
import logzero
from logzero import logger
import json
import pandas as pd
import json
import requests
import appLevel as appLevel


#config check
with open('config.json', 'r') as f:
    appLevel.appConfig = json.load(f)
    f.close()

appLevel.baseDateTimeStampString = datetime.datetime.now().strftime("%y%m%d_%H_%M_%S")
appLevel.currentDateTimeStampString = appLevel.baseDateTimeStampString
appLevel.log_file_name = 'msb_log_' + appLevel.baseDateTimeStampString + '.log'
# Setup rotating logfile with 10 rotations, each with a maximum filesize of 3MB:
logzero.logfile(appLevel.log_file_name, maxBytes=3e6, backupCount=10)
# Set a minimum log level
logzero.loglevel(logging.INFO)
logger.info("App initiated!")

pb_col_dict = appLevel.appConfig['AddressColumnMap']
yesterday_file = appLevel.appConfig['DEFAULT']['FILE_PREVIOUS']
today_file = appLevel.appConfig['DEFAULT']['FILE_CURRENT']

logger.info("Reading yesterday's and today's files..")
with open(yesterday_file, 'r') as y_file, open(today_file, 'r') as t_file:
    y_file_obj = y_file.readlines()
    t_file_obj = t_file.readlines()

logger.info("Creating delta file msb_1.csv..")
with open('msb_1.csv', 'w') as outFile:
    if t_file_obj[0] == y_file_obj[0]:
        outFile.write(t_file_obj[0])
        for line in t_file_obj:
            if line not in y_file_obj:
                outFile.write(line)
    else:
        logger.info('Input files did not match! Exiting App!')
        os._exit(0)


logger.info("Created delta file msb_1.csv..")
#PitneyBowes
#Username: SanTch2018	
#Password: #0072ST$ak

# api-endpoint 
URL = "https://spectrum.precisely.com/rest/ValidateAddress/results.json?​Data.AddressLine1=​1825+Kramer+Ln&Data.PostalCode=78758"
URL = "https://spectrum.precisely.com/rest/ValidateAddress/results.json"
payload = {}
headers = {'Authorization': 'Basic SanTch2018:#0072ST$ak'}
params = {'Data.AddressLine1': '​1825 Kramer Ln', 'Data.PostalCode':78758}
#response = requests.request("GET", url=URL, auth=('SanTch2018', '#0072ST$ak'),  params = PARAMS, data = payload)    
#print(response.text.encode('utf8'))
#json_data = response.json() if response and response.status_code == 200 else None
##json_data['output_port'][0]['AddressLine1']


df = pd.read_csv('msb_1.csv')

addr_keys = {}
change_tracker = {}

logger.info("Processing addresses with PB..")
def process_response_json(json_data = None):
    if json_data != None:
        if int(json_data['output_port'][0]['Confidence']) > -1:
            for pb_col, csv_col in pb_col_dict.items():
                #print(df.loc[i, "Name"], df.loc[i, "Age"])
                if csv_col != None:
                    if pb_col.startswith('Data.'): 
                        trimmed_pb_col = re.sub('Data.', '', pb_col)
                        if json_data['output_port'][0].get(trimmed_pb_col) != None:
                            if(df.loc[i, csv_col] != json_data['output_port'][0][trimmed_pb_col]):
                                change_tracker[df.loc[i, csv_col]] = json_data['output_port'][0][trimmed_pb_col]
                                df.loc[i, csv_col] = json_data['output_port'][0][trimmed_pb_col]

for i in range(len(df)): 
    #print(df.loc[i, "Name"], df.loc[i, "Age"])
    params = dict() 
    k = ''

    ## Prepare Address key combination key and param object
    for pb_col, csv_col in pb_col_dict.items():
        if csv_col != None:
            #Key creation
            k += str(df.loc[i, csv_col]).strip()
            #Param creation
            params[pb_col] = df.loc[i, csv_col]

    #After pb_col_dict for loop, add to dict
    if k in addr_keys.keys():
        if addr_keys[k] != None:
            json_data = addr_keys[k]
            process_response_json(json_data)
    else:
        logger.info("Initiating new PB Api call..")
        response = requests.request("GET", url=URL, auth=('SanTch2018', '#0072ST$ak'),  params = params, data = payload)
        json_data = response.json() if response and response.status_code == 200 else None
        logger.info("Api call completed.")
        addr_keys[k] = json_data
        process_response_json(json_data)

        
# After loop completition
logger.info('Writing msb_final.csv with updated addresses...')
df.to_csv('msb_final.csv')

print('Printing all delta columns:')
for oldval, newval in change_tracker.items():
    logger.info("Old: {0} - New: {1}".format(oldval,newval))
    

