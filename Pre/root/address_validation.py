#!/usr/bin/env python3.7
"""
address_validation.py

Module Docstring

"""

__author__ = "msb.net.in@gmail.com"
__version__ = "0.1.0"
__license__ = "MIT"

import json
import pandas as pd
import json
import requests
from logzero import logger
import root.app_level as app_level

class address_validation(object):
    """description of class"""

    # Class level attribute
    codername = "Manish"
    

    # Constructor
    def __init__(self):
        self.codername = "Manish"


    def execute(self):
        logger.info("Address validation starting..")

        # api-endpoint
        # PitneyBowes
        # Username: SanTch2018	
        # Password: #0072ST$ak 
        URL = "https://spectrum.precisely.com/rest/ValidateAddress/results.json?​Data.AddressLine1=​1825+Kramer+Ln&Data.PostalCode=78758"
        URL = "https://spectrum.precisely.com/rest/ValidateAddress/results.json"
        payload = {}
        headers = {'Authorization': 'Basic SanTch2018:#0072ST$ak'}
        params = {'Data.AddressLine1': '​1825 Kramer Ln', 'Data.PostalCode':78758}
        pb_col_dict = app_level.appConfig['AddressColumnMap']

        df = pd.read_csv('msb_1.csv')

        addr_keys = {}
        change_tracker = {}
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
                    self.__process_response_json(json_data)
            else:
                logger.info("Initiating new PB Api call..")
                response = requests.request("GET", url=URL, auth=('SanTch2018', '#0072ST$ak'),  params = params, data = payload)
                json_data = response.json() if response and response.status_code == 200 else None
                logger.info("Api call completed.")
                addr_keys[k] = json_data
                self.__process_response_json(json_data)

        # After loop completition
        logger.info('Writing msb_final.csv with updated addresses...')
        df.to_csv('msb_final.csv')

        print('Printing all delta columns:')
        for oldval, newval in change_tracker.items():
            logger.info("Old: {0} - New: {1}".format(oldval,newval))



    def __process_response_json(json_data = None):
        if json_data != None:
            if int(json_data['output_port'][0]['Confidence']) > int(app_level.appConfig['PB_API_CONFIG']['MIN_CONFIDENCE_LEVEL']):
                for pb_col, csv_col in pb_col_dict.items():
                    #print(df.loc[i, "Name"], df.loc[i, "Age"])
                    if csv_col != None:
                        if pb_col.startswith('Data.'): 
                            trimmed_pb_col = re.sub('Data.', '', pb_col)
                            if json_data['output_port'][0].get(trimmed_pb_col) != None:
                                if(df.loc[i, csv_col] != json_data['output_port'][0][trimmed_pb_col]):
                                    change_tracker[df.loc[i, csv_col]] = json_data['output_port'][0][trimmed_pb_col]
                                    df.loc[i, csv_col] = json_data['output_port'][0][trimmed_pb_col]
