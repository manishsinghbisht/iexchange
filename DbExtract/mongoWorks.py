#!/usr/bin/env python3
"""
Module Docstring
"""

__author__ = "msb.net.in@gmail.com"
__version__ = "0.1.0"
__license__ = "MIT"

import os
import datetime
import sys
import traceback
import logging
import json
import appLevel
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


def connect_and_clean_base_extract():
    logger.info("Mongo works started..")
    logger.info("Cleaning BaseExtract..")
    mongoConnectionClient = appLevel.appConfig['MongoConnections']['Client'] 
    mongoConnectionDatabase = appLevel.appConfig['MongoConnections']['Database'] 
    try:
        client = MongoClient(mongoConnectionClient)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        # Get the database
        appLevel.mongo_db = client[mongoConnectionDatabase]
        appLevel.mongo_db.extract_base.remove({})
    except ConnectionFailure as cf:
        logger.error("Mongo connection faliure.")
        logger.exception(cf)
    except Exception as e:
         logger.exception(e)


def insert_extract_base(row=None, mongo_extract_request_id = None):
    if row == None or mongo_extract_request_id == None:
        logger.error('None row/extract request id is passed!')
        return None
    #mongoConnectionClient = appLevel.appConfig['MongoConnections']['Client'] 
    #mongoConnectionDatabase = appLevel.appConfig['MongoConnections']['Database'] 
    try:
        #client = MongoClient(mongoConnectionClient)
        ## The ismaster command is cheap and does not require auth.
        #client.admin.command('ismaster')
        # Get the database
        #db = client[mongoConnectionDatabase]
        
        EXTRACT_DOCUMENT = {}
        EXTRACT_DOCUMENT['uuid'] = str(uuid.uuid4())
        EXTRACT_DOCUMENT['extract_request_id'] = str(mongo_extract_request_id)
        
        for index, col_name in enumerate(appLevel.db_columns):
            EXTRACT_DOCUMENT[col_name] = row[index]
            
        result = appLevel.mongo_db.extract_base.insert_one(EXTRACT_DOCUMENT)
        #logger.info(result)
    except ConnectionFailure as cf:
        logger.error("Mongo connection faliure.")
        logger.exception(cf)
    except Exception as e:
         logger.exception(e)


