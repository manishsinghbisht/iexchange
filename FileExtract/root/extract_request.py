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
import pandas
import bson.json_util
from pandas.io.json import json_normalize
import csv
import dicttoxml
import xml.etree.ElementTree as ET

def get_extract_requests():
    logger.info("Getting extract request..")
    mongoConnectionClient = app_level.appConfig['MongoConnections']['Client'] 
    mongoConnectionDatabase = app_level.appConfig['MongoConnections']['Database'] 
    try:
        client = MongoClient(mongoConnectionClient)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        # Get the database
        app_level.mongo_db = client[mongoConnectionDatabase]
        if app_level.mongo_db.extract_request.count_documents({'ExtractRequestStatus': 'Queued'}) > 0:
            # 'RequestStatus': 'Queued'
            queuedImportRequests = app_level.mongo_db.extract_request.find({'ExtractRequestStatus': 'Queued'})
            return queuedImportRequests;
        else:
            return None
    except ConnectionFailure as cf:
        logger.error("Mongo connection faliure.")
        logger.exception(cf)
    except Exception as e:
            logger.exception(e)


    
# Updates a request status to InProcess
def update_request_to_InProcess(_id):
    originalDocument = app_level.mongo_db.extract_request.find_one({'_id' : _id, 'ExtractRequestStatus': 'Queued'})
    if originalDocument != None:
        result = app_level.mongo_db.extract_request.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'ExtractRequestStatus': 'InProcess'}},  upsert=False)
        #print('Number of documents modified : ' + str(result.modified_count))
        return result.modified_count;


    
# Updates a request status to Completed
def update_request_to_Completed(_id):
    originalDocument = app_level.mongo_db.extract_request.find_one({'_id' : _id, 'ExtractRequestStatus': 'InProcess'})
    if originalDocument != None:
        result = app_level.mongo_db.extract_request.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'ExtractRequestStatus': 'Completed'}},  upsert=False)
        #print('Number of documents modified : ' + str(result.modified_count))
        return result.modified_count;


def get_extract_request_columns(extract_type_id):
    logger.info("Getting extract columns for request..")
    try:
        cols_cursor = app_level.mongo_db.extract_request_columns.find({'extract_type_id' : extract_type_id})
        for document in cols_cursor:
            return document['columns']
    except Exception as e:
        logger.exception(e)