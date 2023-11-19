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

def write_extract_xml_file(template_file=None):
    try:
        logger.info("Initiating writing XML extract file!")
        start_time = datetime.datetime.utcnow()
        template_file = app_level.appConfig['EXTRACT_FILES']['XML_TEMPLATE']

        if template_file == None:
            logger.error('No xml template is provided!')
            return None

        app_level.currentDateTimeStampString = datetime.datetime.now().strftime("%y%m%d_%H_%M_%S")
        outputextractFileName = "../" + app_level.appConfig['DEFAULT']['DATAFOLDER'] + '/extractOutput_' + app_level.currentDateTimeStampString + '.xml'
        logger.info("Writing XML extract file name: {x}".format(x=outputextractFileName))

        tree = ET.parse("../" + app_level.appConfig['DEFAULT']['DATAFOLDER'] + '/' + template_file)  
        root = tree.getroot()
        template_provider_node_original = root.find(".//*[@isTemplateBaseProviderNode='Yes']")
    
        if template_provider_node_original == None:
            logger.error("No node with attribute isTemplateBaseProviderNode='Yes' found in template xml: {0}!".format(template_file))
            return None

        template_provider_node = copy.deepcopy(template_provider_node_original)
        root.remove(template_provider_node_original)
        documentsCount = len(app_level.massagedJsonData)
        # START: Loop for app_level.massagedJsonData
        for index, record in enumerate(app_level.massagedJsonData):
            provider = copy.deepcopy(template_provider_node)
            provider.attrib.pop("isTemplateBaseProviderNode", None)  # None is to not raise an exception if xyz does not exist
            my_xml_str = ET.tostring(provider, encoding='unicode')
        
            #While Loop Starts
            while(my_xml_str.find('{{') != -1):
                start_index = my_xml_str.find('{{')
                end_index = my_xml_str.find('}}')
                str_to_replace = my_xml_str[start_index:end_index+2]
                code_str = "app_level.massagedJsonData[index]" + my_xml_str[start_index+2:end_index]
                try:
                    val1 = eval(code_str)
                except Exception as e:
                    if (app_level.appConfig['EXTRACT_FILES']['LOG_MAP_ERRORS'] ==  True): 
                        logger.error("Invalid JSON path: {0} for {1}".format(code_str, record['IX_REC_UID']))
                    val1 = ''

                my_xml_str = my_xml_str.replace(str_to_replace, str(val1))
                # While Loop ENDS
            # END: Loop for app_level.massagedJsonData

            my_xml_str = my_xml_str.replace('&','&amp;')
            provider = ET.fromstring(my_xml_str)            
            root.append(provider)
        
        ## After cursor loop completition, write file
        ## create a new XML file with the results
        tree.write(outputextractFileName)
        logger.info("Completed writing XML extract file name: {x}".format(x=outputextractFileName))
        end_time = datetime.datetime.utcnow()
        time_taken = end_time - start_time
        datetime.timedelta(0, 8, 562000)
        logger.info('XML writing time for {0} records : {1}'.format(len(app_level.massagedJsonData), str(divmod(time_taken.days * 86400 + time_taken.seconds, 60))))
    except Exception as e:
        logger.error("Exception: {0}. \n{1} \n{2} \n".format('write_extract_xml_file', str(e), e.args))
        pass    