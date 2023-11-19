#!/usr/bin/env python3
"""
Module Docstring
__author__ = "msb.net.in@gmail.com"
__version__ = "0.1.0"
__license__ = "MIT"


"""
import copy
from root.main_execute_rules import main_execute_rules
import xml
import os
import shutil
import datetime
import sys
import traceback
import logging
import json
from bson.json_util import dumps
import root.app_level as app_level
import argparse
import logzero
from logzero import logger
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import json
from jsonschema import validate
from bson import json_util 
import io
import uuid
import pandas as pd
import bson.json_util
from pandas.io.json import json_normalize
import root.db_modules as db_modules
from jsonmerge import merge, Merger
import root.file_modules.xml_writer as xml_writer
import root.file_modules.csv_writer as csv_writer
import root.file_modules.fwf_writer as fwf_writer
from zipfile import ZipFile
from root.db_modules.mongo_db import mongo_db


def json_schema_management(obj_extract_request=None, obj_file=None):
    logger.info("Preparing exact schema json...")  

    extractRequestStatus = obj_extract_request["ExtractRequestStatus"]
    dataScope = obj_file["data_scope"].upper()
    file_type = obj_file["file_type"].upper()
    file_name = obj_file["file_name"]
    file_output_location = obj_file["output_file_location"]
    layout_id = obj_file["layout_id"]
    file_id = obj_file["Id"]
    dataStartDate = obj_file["StartDate"]
    dataEndDate = obj_file["EndDate"]

    schema= None
    
    # Process to keep only those fields which are defined in template
    with open('dataTemplate_schema.json', 'r') as f:
        schema = json.load(f)
        f.close()
    
    #Validate dataTemplate.json with dataTemplate_schema.json
    try:
        validate(instance=app_level.templateJsonData, schema=schema)
    except Exception as e:
        logger.error("Schema Error: Template schema mismatch!! 'dataTemplate.json' not in compliance with 'dataTemplate_schema.json'!! " + str(e))
        raise Exception("Schema Error: Template schema mismatch!! 'dataTemplate.json' not in compliance with 'dataTemplate_schema.json'!! " + str(e))
    
    # Create new JSON for output as per template
    for index, record in enumerate(app_level.massagedJsonData):
        new_filtered_json = copy.deepcopy(app_level.templateJsonData) 
        for field in record:
            if field in app_level.templateJsonData:
                new_filtered_json[field] = record[field]
        app_level.massagedJsonData[index] = new_filtered_json
    logger.info("Completed preparing exact schema json...")  
    
    # If no exception is raised by validate(), the instance is valid.
    logger.info("Validating each json object with json schema...")  
    for index, record in enumerate(app_level.massagedJsonData):
        try:
            validate(instance=record, schema=schema)
        except Exception as e:
            logger.error("Schema Error: Json Schema Validation error in record {0}.\n{1}\n{2} \n".format(index, str(e), e.args))
            raise Exception("Schema Error: Json Schema Validation error in record {0}.\n{1}\n{2} \n".format(index, str(e), e.args))
    
    logger.info("Completed process of validating each json object with json schema...")


def json_extract(batch_count, file_name, file_output_location, file_type, obj_extract_request, obj_file):
    if file_type ==  'JSON' or app_level.appConfig['EXTRACT_FILES']['WRITE_JSON'] == True:
        if(app_level.appConfig['DEFAULT']['MATCH_JSON_SCHEMA'] == "TRUE"):
            json_schema_management(obj_extract_request, obj_file)  
        
        #Write JSON
        if(app_level.appConfig['EXTRACT_FILES']['WRITE_JSON'] == True):
            logger.info("Writing final data to output file")
            finaloutputdata_file_name = "../" + app_level.appConfig['DEFAULT']['DATAFOLDER'] + '/FOutput_' + app_level.currentDateTimeStampString + '_' + str(batch_count) + '.json'
            with open(finaloutputdata_file_name, 'w') as outfile:
                json.dump(app_level.massagedJsonData, outfile, default=str)

            # File movement
            if(app_level.appConfig['EXTRACT_FILES']['WRITE_JSON'] == True):
                cust_file = file_output_location + "/" + file_name + '_' + app_level.currentDateTimeStampString + '_' + str(batch_count) + '.json'
                # Move to location given in request
                shutil.move(finaloutputdata_file_name, cust_file)
                # Add to output_batch_files
                app_level.output_batch_files.append(cust_file)

        logger.info("Finished writing final data to default output file")
        
        

def process_file(obj_extract_request=None, obj_file=None):
    if obj_extract_request == None:
        logger.error('None extract request object is passed!')
        return None

    mongo_extract_request_id = obj_extract_request["_id"]
    if mongo_extract_request_id == None:
        logger.error('None extract request _id is passed!')
        return None
   
    app_level.output_batch_files = []
    extractRequestStatus = obj_extract_request["ExtractRequestStatus"]
    dataScope = obj_file["data_scope"].upper()
    file_type = obj_file["file_type"].upper()
    file_delimiter = None
    if "file_delimiter" in obj_file.keys():
        file_delimiter = obj_file["file_delimiter"]

    if "quote_all" in obj_file.keys():
        quote_all = obj_file["quote_all"]
        quote_char = obj_file["quote_char"] 
    else:
        quote_all = False
        quote_char = ""

    col_width = {}
    if "col_width" in obj_file.keys():
        col_width = obj_file["col_width"]
    else:
        col_width["sys_default"] = 4

    file_name = obj_file["file_name"]
    file_output_location = obj_file["output_file_location"]
    layout_id = obj_file["layout_id"]
    col_names = obj_file["col_names"]
    file_id = obj_file["Id"]
    dataStartDate = obj_file["StartDate"]
    dataEndDate = obj_file["EndDate"]
    file_data_chunk_size = int(app_level.appConfig['DEFAULT']['CHUNK_SIZE'])

    # Load output templates to app level var
    with open('dataTemplate.json', 'r') as t:
        logger.info("Reading json data template..")
        app_level.templateJsonData = json.load(t)
        t.close()

    logger.info("Getting data for extract_request_id from mongo database..")
    total_documents_count = app_level.mongo_db.extract_base.find({'extract_request_id': str(mongo_extract_request_id)}).count()
    logger.info('There are total {0} documents'.format(total_documents_count))
    if total_documents_count <= 0:
        return None

    docs=[]
    batch_count = 0
    cursor_document_count = 0
    logger.info('Configured batch size is {0} '.format(file_data_chunk_size))

    cursor = app_level.mongo_db.extract_base.find({'extract_request_id': str(mongo_extract_request_id)}, no_cursor_timeout=True).sort([ ("_id", 1 )]).skip(cursor_document_count).batch_size(file_data_chunk_size)
    
    while(True):
        start_time_mongo = datetime.datetime.utcnow()
        if cursor == None:
            logger.info("Reinitiating cursor!!")
            cursor = app_level.mongo_db.extract_base.find({'extract_request_id': str(mongo_extract_request_id)}, no_cursor_timeout=True).sort([ ("_id", 1 )]).skip(cursor_document_count).batch_size(file_data_chunk_size)
        try:
            document = next(cursor, None)
            if(document):
                #document = cursor.next()
                cursor_document_count = cursor_document_count + 1
                docs.append(document)
                
                #Check if batch size reached or all documents added in docs[] list
                # True: only if batch size is met
                if(cursor_document_count % file_data_chunk_size == 0 or cursor_document_count >= total_documents_count):
                    logger.info('Total documents processed + in process {0} '.format(cursor_document_count))
                    batch_count += 1
                    logger.info('Current batch: {0} '.format(batch_count))
                    #jsonString = json_util.dumps(docs, default=str)
                    jsonString = json_util.dumps(docs, default=json_util.default)
                    json_docs = json.loads(jsonString, object_hook=json_util.object_hook)
                    app_level.massagedJsonData = copy.deepcopy(json_docs) 
                    docs=None
                    docs=[]
                    end_time_mongo = datetime.datetime.utcnow()
                    time_taken_mongo = end_time_mongo - start_time_mongo
                    datetime.timedelta(0, 8, 562000)
                    logger.info('DB fetch time for batch {0} of {1} records: {2}'.format(batch_count, file_data_chunk_size, str(divmod(time_taken_mongo.days * 86400 + time_taken_mongo.seconds, 60))))
            
                    ###############################################################################
                    logger.info('Template json merging started!')
                    start_time_template_merging = datetime.datetime.utcnow()
                    # Merge masagedJsonData at app_level var by merging template json and input json
                    for index, record in enumerate(app_level.massagedJsonData):
                        base = copy.deepcopy(app_level.templateJsonData) 
                        record = merge(base, record)
                        #record.update(base)
                        app_level.massagedJsonData[index] = record

                    end_time_template_merging = datetime.datetime.utcnow()
                    time_taken_template_merging = end_time_template_merging - start_time_template_merging
                    datetime.timedelta(0, 8, 562000)
                    logger.info('Template json merging time for batch {0} of {1} records: {2}'.format(batch_count, file_data_chunk_size, str(divmod(time_taken_template_merging.days * 86400 + time_taken_template_merging.seconds, 60))))
                    logger.info('Template json merging completed!')
                    logger.info('Documents massaged {0} records'.format(len(app_level.massagedJsonData)))
                    ###############################################################################

                    # Send Json data to execute rules
                    execute_rules = main_execute_rules(importRequestId = mongo_extract_request_id, sourceId = '', fileId = file_id, fileName = '', fileType = file_type, dataScope = dataScope, layout_id = layout_id)
                    logger.info("Executing rules for {0} records".format(len(app_level.massagedJsonData)))             
                    execute_rules.execute()
                    logger.info("Completed rules for {0} records".format(len(app_level.massagedJsonData)))   
                    execute_rules = None
                    ###############################################################################

                    # File writings
                    json_extract(batch_count, file_name, file_output_location, file_type, obj_extract_request, obj_file)
                    
                    if(app_level.appConfig['EXTRACT_FILES']['WRITE_FWF'] == True):
                        fwf_writer.write_extract_fwf_file(batch_count = batch_count, file_output_location=file_output_location,file_name=file_name, file_extension=file_type,col_names = col_names, delimiter=file_delimiter, shall_quote=quote_all, quote_char=quote_char, col_width = col_width)
                    if(app_level.appConfig['EXTRACT_FILES']['WRITE_CSV'] == True):
                        logger.info("Starting to writing csv")   
                        csv_writer.write_extract_csv_file(batch_count = batch_count, file_output_location=file_output_location,file_name=file_name, file_extension=file_type,col_names = col_names, delimiter=file_delimiter, shall_quote=quote_all, quote_char=quote_char)
                        logger.info("Completed writing csv") 
                    #if(app_level.appConfig['EXTRACT_FILES']['WRITE_XML'] == True):
                    #    logger.info("Starting to writing xml")   
                    #    xml_writer.write_extract_xml_file()
                    #    logger.info("Completed writing xml")   


                    ###############################################################################

                    app_level.massagedJsonData = [] # massaged data after writing file for the current batch
                    logger.info("===== !! Completed batch number {0} !!=====".format(batch_count))
                    logger.info("==================================")
                    if cursor:
                        cursor.close()
                        cursor = None
            else:
                if(len(app_level.output_batch_files) > 0):
                    # Send file to execute Post Process rules
                    execute_rules = main_execute_rules(importRequestId = mongo_extract_request_id, sourceId = '', fileId = file_id, fileName = '', fileType = file_type, dataScope = dataScope, layout_id = layout_id)
                    
                    for outfilename in app_level.output_batch_files:
                        if not outfilename.lower().endswith(('.json', '.xml')):
                            logger.info("Executing after completion text rules for file {0}".format(outfilename))
                            execute_rules.execute_post_process(file=outfilename)
                            logger.info("Completed after completion text rules for file {0}".format(outfilename))

                    execute_rules = None
                    # Create a ZipFile Object
                    zip_output_file_name = file_output_location + "/" + file_name + datetime.datetime.now().strftime("%y%m%d_%H_%M_%S") + ".zip"
                    with ZipFile(zip_output_file_name, 'w') as zipObj:
                        for outfilename in app_level.output_batch_files: 
                           zipObj.write(outfilename)
                    #Update File status
                    obj_mongo_db = mongo_db()
                    obj_mongo_db.update_extract_request_file(mongo_extract_request_id, file_id, "Completed", desc = zip_output_file_name)
                    obj_mongo_db = None

                break # Done processing all batches, exit outer loop
        except Exception as e:
            logger.exception(e)
            #logger.error("===== WAITING ===== EXECUTE ERROR =====")
            raise e
    # After completion of loop
    if cursor:
        cursor.close()
        cursor = None
    