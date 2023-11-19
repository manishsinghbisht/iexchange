#!/usr/bin/env python3
"""
Module Docstring
__author__ = "msb.net.in@gmail.com"
__version__ = "0.1.0"
__license__ = "MIT"

"""
import copy
import xml
import os
import shutil
import datetime
import sys
import traceback
import logging
import json
import root.app_level as app_level
import argparse
import logzero
from logzero import logger
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import json
from bson import json_util 
import io
import uuid
import pandas as pd
import numpy as np
import bson.json_util
from pandas.io.json import json_normalize
import csv


def write_extract_fwf_file(batch_count = None, file_output_location = None, file_name = None, file_extension="txt", col_names = None, delimiter = ',', shall_quote=False, quote_char="", col_width=None):
    try:
        if col_width == None:
            col_width = {}
            col_width["sys_default"] = 50

        logger.info("Initiating writing FWF extract file!")
        start_time = datetime.datetime.utcnow()
        app_level.currentDateTimeStampString = datetime.datetime.now().strftime("%y%m%d_%H_%M_%S")
        outputextractFileName = "../" + app_level.appConfig['DEFAULT']['DATAFOLDER'] + '/extractOutput' + '_' + app_level.currentDateTimeStampString + '_' + str(batch_count) + '.' + file_extension
        logger.info("Writing FWF extract file name: {x}".format(x=outputextractFileName))

        documentsCount = len(app_level.massagedJsonData)
        app_level.extract_output_file_object = []
        col_names_list = col_names.split(",")

        for item in app_level.massagedJsonData:
            if not "Extract_Sorted_Columns" in item.keys():
                item['Extract_Sorted_Columns'] = {}
            for col in col_names_list:
                col = col.strip()
                req_len = col_width['sys_default']
                if col in item['Extract'].keys():
                    item['Extract_Sorted_Columns'][col] = item['Extract'][col]
                    data_len = len(item['Extract_Sorted_Columns'][col])
                    if col in col_width: 
                        req_len = col_width[col]
                    if data_len < req_len:
                        spaces_len = req_len - data_len
                        for x in range(0, spaces_len):
                            item['Extract_Sorted_Columns'][col] += ' '
                else:
                    for x in range(0, spaces_len):
                        item['Extract_Sorted_Columns'][col] = ' '

                    logger.error("Layout column missing in massaged data. Verify rule defined for it. Column: {x}".format(x=col))

            app_level.extract_output_file_object.append(item['Extract_Sorted_Columns'])

        #Write df
        df = pd.DataFrame(app_level.extract_output_file_object) 

        df.to_csv(outputextractFileName, header=False, index=False, sep='\t', columns=col_names_list) 
        #df.to_csv(outputextractFileName, header=False, index=False, sep = '\t', quoting = csv.QUOTE_NONE, escapechar = ' ', columns=col_names_list) 
        
        #df.to_string(columns=col_names_list)

        logger.info("Completed writing FWF extract file name: {x}".format(x=outputextractFileName))
        end_time = datetime.datetime.utcnow()
        time_taken = end_time - start_time
        datetime.timedelta(0, 8, 562000)
        logger.info('FWF writing time for {0} records : {1}'.format(len(app_level.massagedJsonData), str(divmod(time_taken.days * 86400 + time_taken.seconds, 60))))

        cust_file = file_output_location + "/" + file_name + '_' + app_level.currentDateTimeStampString + '_' + str(batch_count) + '.' + file_extension
        if file_output_location != None and file_name != None:
            logger.info("Now moving file to " + cust_file)
            # File movement
            shutil.move(outputextractFileName, cust_file)
            # Add to output_batch_files
            app_level.output_batch_files.append(cust_file)
        
    except Exception as e:
        logger.error("Exception: {0}. \n{1} \n{2} \n".format('write_extract_fwf_file', str(e), e.args))
        pass
    
       