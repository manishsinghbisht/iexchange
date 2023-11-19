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
import pandas
import bson.json_util
from pandas.io.json import json_normalize
import csv

def get_extract_requests():
    logger.info("Getting extract request..")
    mongoConnectionClient = appLevel.appConfig['MongoConnections']['Client'] 
    mongoConnectionDatabase = appLevel.appConfig['MongoConnections']['Database'] 
    try:
        client = MongoClient(mongoConnectionClient)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        # Get the database
        appLevel.mongo_db = client[mongoConnectionDatabase]
        if appLevel.mongo_db.db_extract_request.count_documents({'ExtractRequestStatus': 'Queued'}) > 0:
            # 'RequestStatus': 'Queued'
            queuedImportRequests = appLevel.mongo_db.db_extract_request.find({'ExtractRequestStatus': 'Queued'})
            return queuedImportRequests;
        else:
            return None
    except ConnectionFailure as cf:
        logger.error("Mongo connection faliure.")
        logger.exception(cf)
    except Exception as e:
            logger.exception(e)


def get_extract_request_columns(db_extract_type_id):
    logger.info("Getting extract columns for request..")
    try:
        cols_cursor = appLevel.mongo_db.db_extract_request_columns.find({'db_extract_type_id' : db_extract_type_id})
        for document in cols_cursor:
            return document['columns']
    except Exception as e:
        logger.exception(e)


    
# Updates a request status to InProcess
def update_request_to_InProcess(_id):
    originalDocument = appLevel.mongo_db.db_extract_request.find_one({'_id' : _id, 'ExtractRequestStatus': 'Queued'})
    if originalDocument != None:
        result = appLevel.mongo_db.db_extract_request.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'ExtractRequestStatus': 'InProcess'}},  upsert=False)
        #print('Number of documents modified : ' + str(result.modified_count))
        return result.modified_count;


    
# Updates a request status to Completed
def update_request_to_Completed(_id):
    originalDocument = appLevel.mongo_db.db_extract_request.find_one({'_id' : _id, 'ExtractRequestStatus': 'InProcess'})
    if originalDocument != None:
        result = appLevel.mongo_db.db_extract_request.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'ExtractRequestStatus': 'Completed'}},  upsert=False)
        #print('Number of documents modified : ' + str(result.modified_count))
        return result.modified_count;
