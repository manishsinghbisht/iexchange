#!/usr/bin/env python3
"""
Pre
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
import argparse
import logzero
from logzero import logger
import uuid
from jsonmerge import merge, Merger
import root.app_level as app_level
import root.filetojson as filetojson
import root.address_validation as address_validation

def main(args=None, version=None):
    """ Main entry point of the app """
    #config check
    with open('config.json', 'r') as f:
        app_level.appConfig = json.load(f)
        f.close()
    app_level.baseDateTimeStampString = datetime.datetime.now().strftime("%y%m%d_%H_%M_%S")
    app_level.currentDateTimeStampString = app_level.baseDateTimeStampString
    app_level.log_file_name = app_level.appConfig['DEFAULT']['DATAFOLDER'] + '/pre_logfile_' + app_level.baseDateTimeStampString + '.log'
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
        logger.info("Pre process initiated!")
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
        app_level.yesterday_file = app_level.appConfig['DEFAULT']['DATAFOLDER'] + '/' + app_level.appConfig['DEFAULT']['FILE_YESTERDAY']
        app_level.today_file = app_level.appConfig['DEFAULT']['DATAFOLDER'] + '/' + app_level.appConfig['DEFAULT']['FILE_TODAY']
        app_level.delta_file = app_level.appConfig['DEFAULT']['DATAFOLDER'] + '/' + app_level.appConfig['DEFAULT']['FILE_DELTA']
        filetojson.remove_duplicates()
        app_level.delta_json = filetojson.create_json_objects_of_files(
            file_name=app_level.delta_file, 
            input_filetype = "CSV", 
            input_file_has_header = True, 
            input_delimiter = ",", 
            col_Names = "ProviderCategory,ProviderLastName,ProviderFirstName,ProviderSSN,ProviderDOB,Gender,Specialty,BoardStatus,IndividualNPI,LicenseNumber,LicenseType,LicenseState,LicenseCountry,LicEffDate,LicExpDate,MiddleName,Degree,Language,TaxID,TaxType,PracticeName,Address1,Address2,City,State,Zip,County,CountryCode,CountryName,Mon_Start,Mon_End,Tue_Start,Tue_End,Wed_Start,Wed_End,Thu_Start,Thu_End,Fri_Start,Fri_End,Sat_Start,Sat_End,Sun_Start,Sun_End,BillingAddress1,BillingAddress2,BillingCity,BillingState,BillingZip,BillingPhone,BillingFax,BillingCountryCode,BillingCountryName,Network,SubNetwork,AcceptingNewPatient,ProviderNetworkEffDate,ProviderNetworkTermDate,ProviderNetworkBillingEffDate,ProviderNetworkBillingTermDate,FeeScheduleName,PrvFeeScheduleEffDate,PrvFeeScheduleTermDate,AffiliationLineOtherID,ProviderPracticeLocationStartDate,ProviderPracticeLocationEndDate,ContractID",
            col_Widths = "9,10,19,26,20")

        app_level.today_json = filetojson.create_json_objects_of_files(
            file_name=app_level.today_file,
            input_filetype = "CSV", 
            input_file_has_header = True, 
            input_delimiter = ",", 
            col_Names = "ProviderCategory,ProviderLastName,ProviderFirstName,ProviderSSN,ProviderDOB,Gender,Specialty,BoardStatus,IndividualNPI,LicenseNumber,LicenseType,LicenseState,LicenseCountry,LicEffDate,LicExpDate,MiddleName,Degree,Language,TaxID,TaxType,PracticeName,Address1,Address2,City,State,Zip,County,CountryCode,CountryName,Mon_Start,Mon_End,Tue_Start,Tue_End,Wed_Start,Wed_End,Thu_Start,Thu_End,Fri_Start,Fri_End,Sat_Start,Sat_End,Sun_Start,Sun_End,BillingAddress1,BillingAddress2,BillingCity,BillingState,BillingZip,BillingPhone,BillingFax,BillingCountryCode,BillingCountryName,Network,SubNetwork,AcceptingNewPatient,ProviderNetworkEffDate,ProviderNetworkTermDate,ProviderNetworkBillingEffDate,ProviderNetworkBillingTermDate,FeeScheduleName,PrvFeeScheduleEffDate,PrvFeeScheduleTermDate,AffiliationLineOtherID,ProviderPracticeLocationStartDate,ProviderPracticeLocationEndDate,ContractID",
            col_Widths = "9,10,19,26,20")

        app_level.yesterday_json = filetojson.create_json_objects_of_files(
            file_name=app_level.yesterday_file,
            input_filetype = "CSV", 
            input_file_has_header = True, 
            input_delimiter = ",", 
            col_Names = "ProviderCategory,ProviderLastName,ProviderFirstName,ProviderSSN,ProviderDOB,Gender,Specialty,BoardStatus,IndividualNPI,LicenseNumber,LicenseType,LicenseState,LicenseCountry,LicEffDate,LicExpDate,MiddleName,Degree,Language,TaxID,TaxType,PracticeName,Address1,Address2,City,State,Zip,County,CountryCode,CountryName,Mon_Start,Mon_End,Tue_Start,Tue_End,Wed_Start,Wed_End,Thu_Start,Thu_End,Fri_Start,Fri_End,Sat_Start,Sat_End,Sun_Start,Sun_End,BillingAddress1,BillingAddress2,BillingCity,BillingState,BillingZip,BillingPhone,BillingFax,BillingCountryCode,BillingCountryName,Network,SubNetwork,AcceptingNewPatient,ProviderNetworkEffDate,ProviderNetworkTermDate,ProviderNetworkBillingEffDate,ProviderNetworkBillingTermDate,FeeScheduleName,PrvFeeScheduleEffDate,PrvFeeScheduleTermDate,AffiliationLineOtherID,ProviderPracticeLocationStartDate,ProviderPracticeLocationEndDate,ContractID",
            col_Widths = "9,10,19,26,20")

        av = address_validation.address_validation()
        av.execute()


        logger.info("Pre processing completed successfully!")
        #raise Exception("Exception check!")
    except Exception as e:
        logger.exception(e)
    

    
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    main(args)
