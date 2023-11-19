#!/usr/bin/env python3.7
"""
Module Docstring
python main.py
"""

__author__ = "msb.net.in@gmail.com"
__version__ = "0.1.0"
__license__ = "MIT"


#import configparser
import os
import datetime
import sys, traceback
import logging
import logzero
from logzero import logger

import pandas
import json
from jsonmerge import merge
import copy
from root.fileModules.CsvManager import CsvManager
from root.fileModules.FwfManager import FwfManager
import root.main_execute as main_execute
import root.main_file_process as main_file_process
import root.appLevel as appLevel
from root.dbModules.mongoDbCon import mongoDbCon
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping
from root.apiCalls.insertApiCalls import insertApiCalls
import argparse


def Process_Queued_Import_Requests(input_filename, input_filetype, input_import_request_id, inputSourceSetUpId, input_fileId, input_delimiter, input_colNames, input_colWidths, mongoObj):
    #Read IX_IMPORT_REQUEST
    importRequests = mongoObj.get_QueuedImportRequest()
    if(importRequests == None):
        logger.info("No 'Queued' requests found!")
        logger.info("Exiting!")
        os._exit(1)
    
    try:
        # current working dir
        cwd = os.getcwd()
        for x in importRequests:
            input_import_request_id = x["_id"]
            inputSourceSetUpId = x["SourceSetupId"]
            
            if "InputFileLocation" in x.keys():
                appLevel.appConfig['InputFileLocation'] = x["InputFileLocation"]
            else:
                appLevel.appConfig['InputFileLocation'] = "../" + appLevel.appConfig['DEFAULT']['DATAFOLDER']

            if "ErrorFileLocation" in x.keys():
                appLevel.appConfig['ErrorFileLocation'] = x["ErrorFileLocation"]
            else:
                appLevel.appConfig['ErrorFileLocation'] = "../" + appLevel.appConfig['DEFAULT']['DATAFOLDER'] + "/error"

            if "ArchiveFileLocation" in x.keys():
                appLevel.appConfig['ArchiveFileLocation'] = x["ArchiveFileLocation"]
            else:
                appLevel.appConfig['ArchiveFileLocation'] = "../" + appLevel.appConfig['DEFAULT']['DATAFOLDER'] + "/archive" 
    
            mongoObj.update_IX_Import_SourceRequest(input_import_request_id, "InProcess")
            logger.info("SourceSetupId:")
            logger.info(x["SourceSetupId"])
            logger.info("Request _id:")
            logger.info(x["_id"])
            logger.info("SourceSetupId: {0} Request _id: {1} ".format(x["SourceSetupId"], x["_id"]))
            for f in x["Files"]:
                if f["FileRequestStatus"] == "Queued":              
                    #for file in os.listdir("/mydir"):
                    #for file in os.listdir("../" + appLevel.appConfig['DEFAULT']['DATAFOLDER']):   #  os.chdir("../nodes") to go one folder up
                    #for file in os.listdir(appLevel.appConfig['InputFileLocation']):
                    
                    file_path_name = "{0}{1}{2}".format(appLevel.appConfig['InputFileLocation'], '\\',f["Name"])
                    if os.path.isfile(file_path_name):
                        #if file.endswith(".txt"):
                        #if file.startswith(f["Name"]):
                        # File processing loop starts
                        #logger.info(os.path.join("/", file))
                        logger.info(file_path_name)
                        input_filename = f["Name"] # 'Input file name'
                        input_filetype = f["TYPE"] # 'Input file type FWF or CSV'
                        input_fileId = f["Id"] # 'Input file id'
                        input_colNames = f["COLNAMES"]
                        if "DELIMITER" in f.keys():
                            input_delimiter = f["DELIMITER"]
                        else:
                            input_delimiter = ","

                        logger.info("FileName: {0}, FileType: {1}, FileId: {2} FileRequestStatus: {3} ".format(f["Name"], f["TYPE"], f["Id"], f["FileRequestStatus"]))
                        # Append File Info to Time stamp
                        appLevel.currentDateTimeStampString = ""
                        appLevel.currentDateTimeStampString = appLevel.baseDateTimeStampString + "_" + input_fileId
                        input_colWidths = None
                        if input_filetype == "FWF":
                            input_colWidths = f["COLWIDTHS"]

                        # To get rules from DB
                        if(appLevel.appConfig['DEFAULT']['USE_RULECONFIG_FOR_MAPPING'] == "TRUE"):
                            with open('ruleConfig.json', 'r') as rc:
                                appLevel.ruleConfig = json.load(rc)
                        else:
                            objFileMapperInserter = mongoDbFileMapping()
                            if(appLevel.appConfig['DEFAULT']['RESET_MAPPINGS_DB'] == "TRUE"):
                                with open('ruleConfig.json', 'r') as rc:
                                    appLevel.ruleConfig = json.load(rc)
                                objFileMapperInserter.remove_and_insert()
                            # Get all mappings
                            objFileMapperInserter.get_all_ColumnMappings(input_fileId, input_filename)
    
                        # Start File Processing
                        mainFileProcessObj = main_file_process.mainFileProcess(importRequestId = input_import_request_id, sourceId=inputSourceSetUpId, input_filename = input_filename, input_filetype = input_filetype, input_fileId = input_fileId, input_delimiter = input_delimiter, col_Names = input_colNames, col_Widths = input_colWidths)
                        mainFileProcessObj.process_inputfile_in_chunks()
                    else:
                        logger.error("Exception: Import Request_id {0}. \n File Id and name: {1} ({2}) \n Error: {3}".format(input_import_request_id, f["Id"], f["Name"], "File not found!"))
                        mongoObj.update_IX_Import_FileRequest(input_import_request_id, f["Id"], "Error", "File Not found!!")

            # Update 'SourceRequestStatus' in mongoDB 'IX_IMPORT_REQUEST' collection after all files are completed
            mongoObj.update_IX_Import_SourceRequest_InProcess2Completed(input_import_request_id) 
    except Exception as e:
        logger.error("Exception Main: ")
        logger.exception(e)
        # Update 'SourceRequestStatus' in mongoDB 'IX_IMPORT_REQUEST' collection after all files are completed
        mongoObj.update_IX_Import_SourceRequest(input_import_request_id, "Error", str(e))
        pass
    return importRequests, input_colNames, input_colWidths, input_import_request_id


def Process_ConfigDefined_Import_Files(input_filename, input_filetype, input_import_request_id, inputSourceSetUpId, input_fileId, input_delimiter, input_colNames, input_colWidths, mongoObj):
    input_file_list = input_filename.split(",")
    input_filetype_list = input_filetype.split(",")
    input_colWidths = None
    appLevel.appConfig['InputFileLocation'] = "../" + appLevel.appConfig['DEFAULT']['DATAFOLDER']
    appLevel.appConfig['ErrorFileLocation'] = "../" + appLevel.appConfig['DEFAULT']['DATAFOLDER'] + "/error"
    appLevel.appConfig['ArchiveFileLocation'] = "../" + appLevel.appConfig['DEFAULT']['DATAFOLDER'] + "/archive" 

    for input_file, input_filetype in zip(input_file_list, input_filetype_list):
    #for input_file in input_file_list:
         try:
            logger.info("SourceSetupId: {0} Request _id: {1} ".format("CONFIG", "CONFIG"))
            if input_file == "inputSampleFWF.txt":
               input_colNames = "FName,LName,Speciality1,Speciality2,CertNo1,CertNo2"
               input_colWidths = "11,11,11,11,11,11"
               with open('ruleConfig_inputSampleFWF.json', 'r') as rc:
                    appLevel.ruleConfig = json.load(rc)
            elif input_file == "inputUCDFWF1.txt":
               input_colNames = "HP_ID_1,HP_ADDR_TKN_1,PRV_GRP_PRA_IND,ANW_TSP_GRP_1,PFN_PRV_LA_NAME,PFN_PRV_FST_NAME,  PRV_SSN_ID,PFN_PRV_BTH_DATE,SEX_SPEC_CODE,ANW_TSP_GRP_2,EDC_CRD_DEG_CODE,ANW_TSP_GRP_3,  HP_TXP_ID,TXP_ID_TYP_CODE,HP_BUS_NAME,ANW_TSP_GRP_4,SPL_TYP_CODE,PRV_BRD_STA_CODE,PRV_SPL_EFF_DATE,  ANW_TSP_GRP_5,HP_NPI_ID,HP_NPI_ID_TYP_CODE,ANW_TSP_GRP_6,HPBA_DIRY_URL_AD,ANW_TSP_GRP_7,HP_BUS_SITE_ID,  ADDR_TYP_CODE_1,HP_ADDR_STR_ADDR_1,HP_ADD_INF_ADDR_1,ZIP_LOC_CY_ADDR_1,POL_RGN_CODE_1,MN_ZIP_ADDR_1,  ZIP_SUF_ADDR,COY_CODE_1,COY_NAME_1,ANW_TSP_GRP_8,HP_ADDR_EFF_DATE_1,HP_ADDR_TKN_2,ADDR_TYP_CODE_2,  HP_ADDR_STR_ADDR_2,HP_ADD_INF_ADDR_2,ZIP_LOC_CY_ADDR_2,POL_RGN_CODE_2,MN_ZIP_ADDR_2,ZIP_SUF_ADDR,  COY_CODE_2,COY_NAME_2,ANW_TSP_GRP_9,HP_ADDR_EFF_DATE_2,HP_ID_2,HP_ADDR_TKN_3,HPBA_DIRY_VERI_IN,HPBA_DIRY_VERI_DT,  ANW_TSP_GRP_10,HP_ID_3,HP_ADDR_TKN_4,HP_ADDR_LTTD_ID,HP_ADDR_LGTD_ID,ANW_TSP_GRP_11,HP_ID_4,HP_ADDR_TKN_5,  HP_ADDR_CTY_COR_CODE,HP_ADDR_CTY_COR_DS,ANW_TSP_GRP_12,HP_ID_5,AAE_CODE,PRV_DOC_EFF_DATE,PRV_DOC_CNL_DATE,  PRV_DOC_EMC_IND,INF_INCL_CODE,ANW_TSP_GRP_13"
               input_colWidths = "9,10,1,26,20,15,9,10,1,26,5,26,9,1,53,26,3,1,10,26,10,1,26,53,26,3,1,36,36,22,2,5,4,3,27,26,10,10,1,36,36,22,2,5,4,3,27,26,10,9,10,1,10,26,9,10,12,12,26,9,10,3,50,26,9,1,10,10,1,1,26"
               with open('ruleConfig_inputUCDFWF1.json', 'r') as rc:
                    appLevel.ruleConfig = json.load(rc)
            elif input_file == "inputSampleCSV.txt":
               input_colNames = "Id,Provider Id,Office Name,Provider First Name,Provider Last Name,Provider NPI,Provider Specialty1,CertNo1,Provider Specialty2,CertNo2,Provider Specialty3,CertNo3,LicenseNumber1,LicenseNumber2,LicenseType1,LicenseType2,BILLINGSTREET,BILLINGSTREETAddress2,BillingCity,BillingState,BillingPostalCode,BillingCountry,SHIPPINGSTREET,SHIPPINGSTREETAddress2,ShippingCity,ShippingState,ShippingPostalCode,SHIPPINGCounty,Phone,Fax,Group_Name_Formula__c,Accepts_New_Patients__c,Handicap_Accessible__c,Fee_Schedule_Formula__c,Weekend_Hours__c,Evening_Hours__c,PATIENT_MINIMUM_AGE__C,PATIENT_MAXIMUM_AGE__C,Facets_Location_Id__c,MAILING_STREET__C,MAILING_STREET__Address2,Mailing_City__c,Mailing_State__c,Mailing_Zip__c,MAILINGCounty,BILLING_PHONE__C,Billing_Fax__c,Include_In_Directory__c,Monday_From__c,Monday_To__c,Tuesday_From__c,Tuesday_To__c,Wednesday_From__c,Wednesday_To__c,Thursday_From__c,Thursday_To__c,Friday_From__c,Friday_To__c,Saturday_From__c,Saturday_To__c,Sunday_From__c,Sunday_To__c,Full_Time__c,Fee_Schedule_Effective_Date__c,Fee_Schedule_Term_Date__c,Tax_Id_Effective_Date__c,Network_ID_c,Contract_Type,Tax_Name__c,Tax_Address__c,TAX_ADDRESS2,Tax_City__c,Tax_State__c,Tax_Zip__c,TAX_County,Location_TIN__c,TIN_Type__c,Location_Email__c,NPI_2_Organization__c,Mailing_Address_Effective_Date__c,Billing_Address_Effective_Date__c,Languages_Spoken_At_Location__c,Location_Verified_Date__c,IsAccepting,Handicap"
               with open('ruleConfig_Csv.json', 'r') as rc:
                    appLevel.ruleConfig = json.load(rc)
            elif input_file == "inputSamplePIPE.csv":
               input_colNames = "ProviderID,SantechProviderID,ProviderCategory,ProviderLastName,ProviderFirstName,ProviderSSN,ProviderDOB,Gender,Specialty,BoardStatus,IndividualNPI,LicenseNumber,LicenseType,LicenseState,LicenseCountry,LicEffDate,LicExpDate,CAQHNumber,CAQHNumberStartDate,CAQHNumberEndDate,MiddleName,Suffix,Prefix,Degree,Language,SantechPracticeID,TaxID,TaxType,PracticeName,Additional NPI,OfficeName,LocationID,Address1,Address2,City,State,Zip,County,CountryCode,CountryName,LocationURL,DBA Name,DBANamePurpose,OfficeLocationPhone,OfficeLocationPhoneExt,OfficeLocationFax,OfficeLocationExt,OfficeLocationEmail,PublicTransportationAcceptable,EveningHours,HandicapAccessible,WeekendHours,AfterHoursCare,Mon_Start,Mon_End,Tue_Start,Tue_End,Wed_Start,Wed_End,Thu_Start,Thu_End,Fri_Start,Fri_End,Sat_Start,Sat_End,Sun_Start,Sun_End,BillingAddress1,BillingAddress2,BillingCity,BillingState,BillingZip,BillingPhone,BillingFax,BillingCountryCode,BillingCountryName,Network,Sub-Network,DirectoryIndicator,AcceptingNewPatient,AcceptingNewPatientEffDate,AcceptingNewPatientTermDate,ProviderNetworkEffDate,ProviderNetworkTermDate,ProviderNetworkBillingEffDate,ProviderNetworkBillingTermDate,DirectoryVerificationDate,FeeScheduleName,PrvFeeScheduleEffDate,PrvFeeScheduleTermDate,Affiliation line Other ID,Provider Practice Location Start Date,Provider Practice Location End Date,ContractID,EntityRelationStartDate,EntityRelationEndDate,PractitionerID"
               with open('ruleConfig_Pipe.json', 'r') as rc:                   
                    appLevel.ruleConfig = json.load(rc)
            
            logger.info("Input file given in config: {0} of type {1} ".format(input_file, input_filetype))
            appLevel.currentDateTimeStampString = ""
            appLevel.currentDateTimeStampString = appLevel.baseDateTimeStampString + "_" + input_file
            
            # Start File Processing
            mainFileProcessObj = main_file_process.mainFileProcess(importRequestId = "", sourceId="", input_filename = input_file, input_filetype = input_filetype, input_fileId = input_file + "_" + input_filetype, input_delimiter = input_delimiter, col_Names = input_colNames, col_Widths = input_colWidths)
            mainFileProcessObj.process_inputfile_in_chunks()
         except Exception as e:
            logger.error("Exception Main 2:")
            logger.exception(e)
            # Update 'SourceRequestStatus' in mongoDB 'IX_IMPORT_REQUEST' collection after all files are completed
            mongoObj.update_IX_Import_SourceRequest(input_import_request_id, "Error")
            counter = counter + 1;
            pass
    return input_colNames, input_colWidths, input_file, input_file_list, input_filetype_list


def main(args=None):
    """ Main entry point of the app """
    try:
        #config check
        with open('config.json', 'r') as f:
            appLevel.appConfig = json.load(f)
            f.close()
        #system rule config check
        with open('systemRuleConfig.json', 'r') as f:
            appLevel.systemRuleConfig = json.load(f)
            f.close()

        #logging configuration
        appLevel.baseDateTimeStampString = datetime.datetime.now().strftime("%y%m%d_%H_%M_%S")
        appLevel.currentDateTimeStampString = appLevel.baseDateTimeStampString
        appLevel.log_file_name = "../" + appLevel.appConfig['DEFAULT']['DATAFOLDER'] + '/ix_logfile_' + appLevel.currentDateTimeStampString + '.log'
        #logging.basicConfig(filename=appLevel.log_file_name, level=logging.INFO, 
        #                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
        #logger = logging.getLogger(appLevel.log_file_name)

        # Setup rotating logfile with 10 rotations, each with a maximum filesize of 1MB:
        logzero.logfile(appLevel.log_file_name, maxBytes=2e6, backupCount=10)
        # Set a minimum log level
        logzero.loglevel(logging.INFO)
        logger.info("I-Exchange process started..")
        logger.debug("Log File Initated : " + appLevel.log_file_name)
        logger.info("Log File Initated : " + appLevel.log_file_name)
        logger.debug("main Initiated..")
        logger.info("main Initiated..")
        secret_key = appLevel.appConfig['DEFAULT']['SECRET_KEY'] # 'secret-key-of-myapp'
        ci_hook_url = appLevel.appConfig['CI']['HOOK_URL'] # 'web-hooking-url-from-ci-service'
        mongoConnectionClient = appLevel.appConfig['MongoConnections']['Client'] 
        mongoConnectionDatabase = appLevel.appConfig['MongoConnections']['Database'] 
        appLevel.appConfig['InputFileLocation'] = "../" + appLevel.appConfig['DEFAULT']['DATAFOLDER']
        input_filename = appLevel.appConfig['DEFAULT']['INPUTFILENAME'] # 'Input file name'
        input_filetype = appLevel.appConfig['DEFAULT']['INPUTFILETYPE'] # 'Input file type FWF or CSV'
        input_delimiter = appLevel.appConfig['DEFAULT']['DELIMITER']
        input_import_request_id = None
        inputSourceSetUpId = None
        input_fileId = None
        input_colNames = None
        input_colWidths = None
        mongoObj = mongoDbFileMapping()
        #config check for dev runs
        if input_filename == "" :
            #IX_IMPORT_REQUEST
            importRequests, input_colNames, input_colWidths, input_import_request_id = Process_Queued_Import_Requests(input_filename, input_filetype, input_import_request_id, inputSourceSetUpId, input_fileId, input_delimiter, input_colNames, input_colWidths, mongoObj)
        elif input_filename != "" :
            input_colNames, input_colWidths, input_file, input_file_list, input_filetype_list = Process_ConfigDefined_Import_Files(input_filename, input_filetype, input_import_request_id, inputSourceSetUpId, input_fileId, input_delimiter, input_colNames, input_colWidths, mongoObj)
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
