#import configparser
import os
import re
import datetime
import sys
import traceback
import logging
import logzero
from logzero import logger
import json
import pandas as pd
import json
import requests
import root.app_level as app_level
import root.csv_manager as csv_manager
import root.fwf_manager as fwf_manager
from root.csv_manager import csv_manager as CsvManager
from root.fwf_manager import fwf_manager as FwfManager



def remove_duplicates():
    pb_col_dict = app_level.appConfig['AddressColumnMap']
    yesterday_file = app_level.yesterday_file
    today_file = app_level.today_file
    delta_file = app_level.delta_file

    logger.info("Reading yesterday's and today's files..")
    with open(yesterday_file, 'r') as y_file, open(today_file, 'r') as t_file:
        y_file_obj = y_file.readlines()
        t_file_obj = t_file.readlines()

    logger.info("Creating delta file from yesterday and today's file ..")
    with open(delta_file, 'w') as out_file:
        if t_file_obj[0] == y_file_obj[0]:
            out_file.write(t_file_obj[0])
            for line in t_file_obj:
                if line not in y_file_obj:
                    out_file.write(line)
        else:
            logger.info('Input files did not match! Exiting app!')
            os._exit(0)



def create_json_objects_of_files(file_name=app_level.yesterday_file, 
                                 input_filetype="CSV", 
                                 input_file_has_header=True, 
                                 input_delimiter=",", 
                           col_Names="Id,Provider,OfficeName,First Name,Last Name",
                           col_Widths="9,10,19,26,20"):
        try:
            # After cache init, start processing
            logger.info('Initiating file reading...')
            output_dict = {}
            chunk_counter = 1
            chunksize = int(app_level.appConfig['DEFAULT']['CHUNK_SIZE']) # 10 ** 2
            

            # File Header handling
            if input_file_has_header:
                file_header = 0
            else:
                file_header = None
            
            if input_filetype.upper() == "CSV" or input_filetype.upper() == "TXT":
                csvManagerObject = CsvManager()
                # Default initialization of delimiter
                delimiter = ","
                if input_delimiter == None or input_delimiter == "":
                    delimiter = ","
                else:
                    delimiter = input_delimiter

                # Setup columns
                if col_Names != None:
                    my_input_colNames = col_Names.split(",") 
                else:
                    df_for_columns = pandas.DataFrame(pandas.read_csv(file_name, chunksize=chunksize, sep = delimiter))
                    my_input_colNames = [1] * len(df_for_columns.columns)
                    
                
                for chunk in pd.read_csv(file_name, chunksize=chunksize, sep = delimiter, header = file_header, index_col = False, encoding = 'unicode_escape', names = my_input_colNames, 
                                            converters={i: str for i in range(0, 10000)}):
                    logger.info('Processing chunk number {0} of {1} records...'.format(str(chunk_counter), str(app_level.appConfig['DEFAULT']['CHUNK_SIZE'])))
                    # Convert CSV to JSON string and then JSON string to JSON
                    # Object
                    if chunk_counter == 1:
                        output_dict = json.loads(csvManagerObject.df_ToJson(chunk))

                    chunk_counter = chunk_counter + 1
            elif self.input_filetype.upper() == "FWF":
                fwfManagerObject = FwfManager()
                my_input_colNames = col_Names.split(",") 
                my_fwidth = map(int, col_Widths.split(","))
                for chunk in pd.read_fwf(file_name, chunksize=chunksize, widths=my_fwidth, encoding = 'unicode_escape', names=my_input_colNames, 
                                            converters={i: str for i in range(0, len(my_input_colNames))}):
                    logger.info('Processing chunk number {0} of {1} records...'.format(str(chunk_counter), str(app_level.appConfig['DEFAULT']['CHUNK_SIZE'])))
                    # Convert FWF to JSON string and then JSON string to JSON
                    # Object
                    if chunk_counter == 1:
                        output_dict = json.loads(fwfManagerObject.fwf_ToJson(chunk))

                    chunk_counter = chunk_counter + 1  
            
            return output_dict
        except Exception as e:
            logger.error("[REF#1112] Exception: process_file_in_chunks {0}.\n {1}\n".format(e, e.args))
            logger.exception(e)

