#!/usr/bin/env python3
"""
sql_works Module Docstring
"""

__author__ = "msb.net.in@gmail.com"
__version__ = "0.1.0"
__license__ = "MIT"

import pyodbc
import os
import datetime
import time
import root.app_level as app_level
from logzero import logger
import root.mongo_works as mongo_works
import uuid
from logzero import logger
import json
import decimal

def de():
    logger.info("Sql works started..")


def init_extract(mongo_extract_request_id=None, obj_file = None):
        if mongo_extract_request_id == None or obj_file == None:
            logger.error('None extract request or file passed!')
            return None

        sqlConnectionServer = app_level.appConfig['SqlServerConnections']['Server'] 
        sqlConnectionDatabase = app_level.appConfig['SqlServerConnections']['Database'] 
        sqlConnectionUser = app_level.appConfig['SqlServerConnections']['User'] 
        sqlConnectionPassword = app_level.appConfig['SqlServerConnections']['Password'] 
        Trusted_Connection = app_level.appConfig['SqlServerConnections']['Trusted_Connection']
        connection = pyodbc.connect(Driver='{SQL Server}',Server= sqlConnectionServer, Database=sqlConnectionDatabase,uid=sqlConnectionUser,pwd=sqlConnectionPassword,Trusted_Connection=Trusted_Connection,autocommit="True")
        #Truncate extract_base in mongoDB..
        mongo_works.connect_and_clean_base_extract()
        # Set cursor
        cursor = connection.cursor()

        try:
            if app_level.appConfig['DEFAULT']['EXECUTE_SP'] == "TRUE":
                logger.info("Executing stored procedure") 
                file_sql_name = obj_file["sql_name"]
                file_sql_param = obj_file["sql_param"]
                logger.info("Initiating Executing SP..")
                cursor.execute(file_sql_name, file_sql_param)
                logger.info("Finished executing SP..")

            logger.info("Executing select query")
            app_level.inputJsonData = []
            logger.info("Initiate fetch for {0}".format(app_level.extract_snapshot_table))
            cursor.execute("SELECT {0} FROM {1}".format(app_level.extract_snapshot_table_cols, app_level.extract_snapshot_table))
        except Exception as e:
            logger.exception(e)
            logger.error("===== WAITING ===== EXECUTE ERROR =====")
            time.sleep(15)
            cursor.execute("SELECT {0} FROM {1}".format(app_level.extract_snapshot_table_cols, app_level.extract_snapshot_table))

        try:
            fetch_rows = 1000
            rows_set_counter = 0
            while True:
                rows = cursor.fetchmany(fetch_rows)
                if not rows:
                    logger.info("Completed processing {0} set of {1} from {2}".format(rows_set_counter, fetch_rows, app_level.extract_snapshot_table))
                    break
                else:
                    rows_set_counter += 1
                    logger.info("Processing {0} set of {1} from {2}".format(rows_set_counter, fetch_rows, app_level.extract_snapshot_table))

                for row in rows:
                    process_row(row, mongo_extract_request_id)                
        except Exception as e:
            logger.exception(e)
            logger.error("===== WAITING ===== FETCH ERROR =====")
    
        #with open(output_file, 'w', newline='', encoding='utf-8') as f:
        #    writer = csv.writer(f, delimiter=delimiter)
        #    writer.writerow([x[0] for x in cursor.description])  # column headers
        #    for row in data:
        #        writer.writerow(row)
        cursor.close()
        return rows_set_counter


def convert_sql_row_2_dict(row):
    EXTRACT_DOCUMENT = {}
    for index, value in enumerate(row):
        col_name = str(row.cursor_description[index][0])
        if col_name.lower().endswith('__json'):
            if(str(value)) != '':
                EXTRACT_DOCUMENT[col_name] = json.loads(value) if value != None else None
            else:
                EXTRACT_DOCUMENT[col_name] = ''
        else:
            EXTRACT_DOCUMENT[col_name] = str(value)
            
    return EXTRACT_DOCUMENT



def process_row(row, mongo_extract_request_id):
    #logger.info(row)
    document = convert_sql_row_2_dict(row)
    document['uuid'] = str(uuid.uuid4())
    document['extract_request_id'] = str(mongo_extract_request_id)
    app_level.inputJsonData.append(document)
