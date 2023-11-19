#!/usr/bin/env python3
"""
Db Extract
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
import appLevel
import argparse
import logzero
from logzero import logger
import uuid
import dbextractRequest
import sqlWorks
import mongoWorks

def main(args):
    """ Main entry point of the app """
    #config check
    with open('config.json', 'r') as f:
        appLevel.appConfig = json.load(f)
        f.close()
    appLevel.baseDateTimeStampString = datetime.datetime.now().strftime("%y%m%d_%H_%M_%S")
    appLevel.log_file_name = "../" + appLevel.appConfig['DEFAULT']['DATAFOLDER'] + '/extract_logfile_' + appLevel.baseDateTimeStampString + '.log'
    # Setup rotating logfile with 10 rotations, each with a maximum filesize of 1MB:
    logzero.logfile(appLevel.log_file_name, maxBytes=2e6, backupCount=10)
    # Set a minimum log level
    logzero.loglevel(logging.INFO)

    try:
        logger.info("DbExtract process initiated!")
        logger.info(args)
        logger.debug("debug check!")
        logger.info("info check!")
        logger.warning("warn check!")
        logger.error("error check!")
        mongoConnectionClient = appLevel.appConfig['MongoConnections']['Client'] 
        mongoConnectionDatabase = appLevel.appConfig['MongoConnections']['Database'] 
        sqlConnectionServer = appLevel.appConfig['SqlServerConnections']['Server'] 
        sqlConnectionDatabase = appLevel.appConfig['SqlServerConnections']['Database'] 
        sqlConnectionUser = appLevel.appConfig['SqlServerConnections']['User'] 
        sqlConnectionPassword = appLevel.appConfig['SqlServerConnections']['Password'] 
        logger.info("Mongo client:%s",mongoConnectionClient)
        logger.info("Mongo database:%s",mongoConnectionDatabase)
        logger.info("Sql server:%s",sqlConnectionServer)
        logger.info("Sql database:%s",sqlConnectionDatabase)
        logger.info("Sql user:%s",sqlConnectionUser)
        logger.info("Sql pass:%s",sqlConnectionPassword)

        extractRequests = dbextractRequest.get_extract_requests()
        if(extractRequests == None):
            logger.info("No 'Queued' requests found!")
            logger.info("Exiting!")
            os._exit(1)
        for x in extractRequests:
            mongo_extract_request_id = x["_id"]
            extractRequestStatus = x["ExtractRequestStatus"]
            dataScope = x["DataScope"]
            db_extract_type_id = x["db_extract_type_id"]
            dataStartDate = x["DataStartDate"]
            dataEndDate = x["DataEndDate"]
            appLevel.db_columns = dbextractRequest.get_extract_request_columns(db_extract_type_id)
            dbextractRequest.update_request_to_InProcess(mongo_extract_request_id)
            if(dataScope == 'Full'):
                sqlWorks.init_extract(mongo_extract_request_id)
            elif(dataScope == 'Increment'):
                sqlWorks.init_extract(mongo_extract_request_id)

            # Update request
            dbextractRequest.update_request_to_Completed(mongo_extract_request_id)
        
        #Outside loop
        #Prefer not to use os.system, use import
        #os.system("python script2.py myParameter")
        # or
        #import script1
        #for i in range(whatever):
        #    script1.some_function(i)

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
