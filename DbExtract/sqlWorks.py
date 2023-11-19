#!/usr/bin/env python3
"""
sqlWorksSp Module Docstring
"""

__author__ = "msb.net.in@gmail.com"
__version__ = "0.1.0"
__license__ = "MIT"

import pyodbc
import os
import datetime
import time
import appLevel
from logzero import logger
import mongoWorks
import uuid
from logzero import logger

def de():
    logger.info("Sql works sp started..")

def init_extract(mongo_extract_request_id=None):
        if mongo_extract_request_id == None:
            logger.error('None extract request id is passed!')
            return None

        sqlConnectionServer = appLevel.appConfig['SqlServerConnections']['Server'] 
        sqlConnectionDatabase = appLevel.appConfig['SqlServerConnections']['Database'] 
        sqlConnectionUser = appLevel.appConfig['SqlServerConnections']['User'] 
        sqlConnectionPassword = appLevel.appConfig['SqlServerConnections']['Password'] 
        Trusted_Connection = appLevel.appConfig['SqlServerConnections']['Trusted_Connection']
        connection = pyodbc.connect(Driver='{SQL Server}',Server= sqlConnectionServer, Database=sqlConnectionDatabase,uid=sqlConnectionUser,pwd=sqlConnectionPassword,Trusted_Connection=Trusted_Connection)
        logger.info("Executing dbo.GetSnapshotExtract")
        connection.execute('dbo.GetSnapshotExtract')
        #Truncate extract_base in mongoDB..
        mongoWorks.connect_and_clean_base_extract()
        cursor = connection.cursor()
        provider_table = appLevel.provider_table
        provider_colstr = ','.join(appLevel.db_columns)
        logger.info("Initiate fetch for {0}".format(provider_table))
        param_network_id = '2'
        try:
            #cursor.execute("execute dbo.GetSnapshotExtract @NetworkId = '%s'" % param_network_id)
            logger.info("Executing select query")
            #cursor.execute("execute dbo.GetSnapshotExtract")
            cursor.execute("SELECT {0} FROM {1}".format(provider_colstr, provider_table))
        except Exception as e:
            logger.exception(e)
            logger.error("===== WAITING ===== EXECUTE ERROR =====")
            time.sleep(15)
            #cursor.execute("execute dbo.GetSnapshotExtract @NetworkId = '%s'" % param_network_id)
            #cursor.execute("execute dbo.GetSnapshotExtract")
            cursor.execute("SELECT {0} FROM {1}".format(provider_colstr, provider_table))

        try:
            fetch_rows = 1000
            rows_set_counter = 0
            #logger.info("Processing {0} set of {1} for {2}".format(rows_set_counter, fetch_rows, provider_table))
            #rows = cursor.fetchmany(fetch_rows)
            while True:
                rows = cursor.fetchmany(fetch_rows)
                if not rows:
                    logger.info("Completed processing {0} set of {1} for {2}".format(rows_set_counter, fetch_rows, provider_table))
                    break
                else:
                    rows_set_counter += 1
                    logger.info("Processing {0} set of {1} for {2}".format(rows_set_counter, fetch_rows, provider_table))

                for row in rows:
                    do_something(row, mongo_extract_request_id)
                
            #while rows is not None:
            #    for row in rows:
            #        do_something(row)
            #    rows_set_counter += 1
            #    logger.info("Processing {0} set of {1} for {2}".format(rows_set_counter, fetch_rows, provider_table))
            #    rows = cursor.fetchmany(fetch_rows)
        except Exception as e:
            logger.exception(e)
            logger.error("===== WAITING ===== FETCH ERROR =====")
    

        #with open(output_file, 'w', newline='', encoding='utf-8') as f:
        #    writer = csv.writer(f, delimiter=delimiter)
        #    writer.writerow([x[0] for x in cursor.description])  # column headers
        #    for row in data:
        #        writer.writerow(row)
        cursor.close()

def do_something(row, mongo_extract_request_id):
    #logger.info(row)
    mongoWorks.insert_extract_base(row, mongo_extract_request_id)

