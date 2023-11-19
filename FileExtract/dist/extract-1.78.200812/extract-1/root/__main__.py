#!/usr/bin/env python3
"""
File_Extract
Module Docstring
"""

__author__ = "msb.net.in@gmail.com"
__version__ = "0.1.0"
__license__ = "MIT"

import os
import datetime
import sys, traceback
import logging
import json
import pandas
import root.app_level as app_level
import argparse
import logzero
from logzero import logger
from root.db_modules.mongo_db import mongo_db
import uuid
import root.extract_request as extract_request
import root.process_extract_file as process_extract_file
import root.sql_works as sql_works
import root.mongo_works as mongo_works
from jsonmerge import merge, Merger


def main(args=None, version=None):
    """ Main entry point of the app """
    #config check
    with open('config.json', 'r') as f:
        app_level.appConfig = json.load(f)
        f.close()
    app_level.baseDateTimeStampString = datetime.datetime.now().strftime("%y%m%d_%H_%M_%S")
    app_level.currentDateTimeStampString = app_level.baseDateTimeStampString
    app_level.log_file_name = "../" + app_level.appConfig['DEFAULT']['DATAFOLDER'] + '/extract_logfile_' + app_level.baseDateTimeStampString + '.log'
    # Setup rotating logfile with 10 rotations, each with a maximum filesize of 3MB:
    logzero.logfile(app_level.log_file_name, maxBytes=3e6, backupCount=10)
    # Set a minimum log level
    logzero.loglevel(logging.INFO)
    if(version == None):
        logger.info("Application version not found. Exiting application!")
        return
    else:
        logger.info("Application version : " + version)

    try:
        logger.info("Extract process initiated!")
        logger.info(args)
        logger.debug("debug check!")
        logger.info("info check!")
        logger.warning("warn check!")
        logger.error("error check!")
        mongoConnectionClient = app_level.appConfig['MongoConnections']['Client'] 
        mongoConnectionDatabase = app_level.appConfig['MongoConnections']['Database'] 
        sqlConnectionServer = app_level.appConfig['SqlServerConnections']['Server'] 
        sqlConnectionDatabase = app_level.appConfig['SqlServerConnections']['Database'] 
        sqlConnectionUser = app_level.appConfig['SqlServerConnections']['User'] 
        sqlConnectionPassword = app_level.appConfig['SqlServerConnections']['Password'] 
        logger.info("Mongo client:%s",mongoConnectionClient)
        logger.info("Mongo database:%s",mongoConnectionDatabase)
        logger.info("Sql server:%s",sqlConnectionServer)
        logger.info("Sql database:%s",sqlConnectionDatabase)
        logger.info("Sql user:%s",sqlConnectionUser)
        logger.info("Sql pass:%s",sqlConnectionPassword)
        
        obj_mongo_db = mongo_db()
        extract_requests = extract_request.get_extract_requests()
        if(extract_requests == None):
            logger.info("No 'Queued' requests found!")
            logger.info("Exiting!")
            os._exit(1)
        for obj_extract_request in extract_requests:
            mongo_extract_request_id = obj_extract_request["_id"]
            extractRequestStatus = obj_extract_request["ExtractRequestStatus"]
            obj_files = obj_extract_request["Files"]
            # Update extract request to InProcess    
            obj_mongo_db.update_extract_request(mongo_extract_request_id, "InProcess", desc = None)

            # Files Started
            for obj_file in obj_files:
                file_id = obj_file["Id"]
                file_data_scope = obj_file["data_scope"].upper()
                file_sql_name = obj_file["sql_name"]
                file_sql_param = obj_file["sql_param"]
                extract_type_id = obj_file["extract_type_id"]
                col_names = obj_file["col_names"]
                layout_id = obj_file["layout_id"]
                file_type = obj_file["file_type"]
                file_name = obj_file["file_name"]
                file_output_location = obj_file["output_file_location"]
                file_start_date = obj_file["StartDate"]
                file_end_date = obj_file["EndDate"]
                file_request_status = obj_file["FileRequestStatus"]
                file_request_status_log = obj_file["FileRequestStatusLog"]
                if(file_output_location != None and file_output_location != ''):
                    app_level.appConfig['OutputFileLocation'] = file_output_location
                
                #Update File status
                obj_mongo_db.update_extract_request_file(mongo_extract_request_id, file_id, "InProcess", desc = None)
                row_set_counter = sql_works.init_extract(mongo_extract_request_id, obj_file)
                if row_set_counter > 0:
                    output_bulk_insert_extract_base = mongo_works.bulk_insert_extract_base(app_level.inputJsonData, mongo_extract_request_id)
                    if output_bulk_insert_extract_base != None:
                        # Write extract file
                        try:
                            process_extract_file.process_file(obj_extract_request, obj_file)
                        except Exception as e:
                            logger.exception(e)
                            obj_mongo_db.update_extract_request_file(mongo_extract_request_id, file_id, "Error", desc = e)
                            raise e
               
            # Update extract request to Completed
            obj_mongo_db.update_extract_request(mongo_extract_request_id, "Completed", desc = None)

        #raise Exception("Exception check!")
    except Exception as e:
        logger.exception(e)
    

    
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    ## Required positional argument
    #parser.add_argument("arg", help="Required positional argument")

    ## Optional argument flag which defaults to False
    #parser.add_argument("-f", "--flag", action="store_true", default=False)

    ## Optional argument which requires a parameter (eg. -d test)
    #parser.add_argument("-n", "--name", action="store", dest="name")

    ## Optional verbosity counter (eg. -v, -vv, -vvv, etc.)
    #parser.add_argument(
    #    "-v",
    #    "--verbose",
    #    action="count",
    #    default=0,
    #    help="Verbosity (-v, -vv, etc)")

    ## Specify output of "--version"
    #parser.add_argument(
    #    "--version",
    #    action="version",
    #    version="%(prog)s (version {version})".format(version=__version__))

    args = parser.parse_args()
    main(args)
