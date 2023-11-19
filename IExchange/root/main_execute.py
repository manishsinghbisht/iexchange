#!/usr/bin/env python3.7
"""
mainExecute.py

Module Docstring

"""
__author__ = "msb.net.in@gmail.com"
__version__ = "0.1.0"
__license__ = "MIT"

import logging
from logzero import logger
import json
import io
import root.appLevel as appLevel
import datetime
from root.fileModules.CsvManager import CsvManager
from root.fileModules.FwfManager import FwfManager
from root.dbModules.mongoDbCon import mongoDbCon
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping
import root.ruleModules.columnMapManager as columnMapManagerModule
import root.ruleModules.removeColumnManager as removeColumnManagerModule
import root.ruleModules.splitToJsonManager as splitToJsonManagerModule
import root.ruleModules.splitRuleManager as splitRuleManagerModule
import root.ruleModules.mergeRuleManager as mergeRuleManagerModule
import root.ruleModules.ruleNpiManager as ruleNpiManagerModule
import root.ruleModules.ruleSsnManager as ruleSsnManagerModule
import root.ruleModules.ruleIXTimeStampManager as ruleIXTimeStampManagerModule
import root.ruleModules.stringRemoveManager as stringRemoveManagerModule
import root.ruleModules.stringReplaceManager as stringReplaceManagerModule
import root.ruleModules.ruleConditionalManager as ruleConditionalManagerModule
import root.ruleModules.stringAppendManager as stringAppendManagerModule
import root.ruleModules.stringPrependManager as stringPrependManagerModule
import root.ruleModules.stringLowerUpperManager as stringLowerUpperManagerModule
import root.ruleModules.pickCharManager as pickCharManagerModule

class mainExecuteModule(object):
    """description of class"""

    # Class level attribute
    codername = "Manish"
    ruleTypeList = ["IXTimeStamp", "ColumnMap", "Rule_NPI", "Rule_SSN", "Merge", "Split", "SplitToJson", "StringRemove", "Conditional", "StringAppend", "StringPrepend", "StringLowerUpper", "StringReplace", "PickChar", "RemoveColumn"]
    ruleNameList = ["IXTimeStamp", "Rule_NPI", "Rule_SSN", "Merge", "Split"]
    mongoDbFileMappingObj = None

    # Constructor
    def __init__(self, importRequestId = None, sourceId = None, fileId = None, fileName = None, fileType = None, created_by_id = None, tenant_id = None, sub_tenant_id = None):
        #self.logger = logging.getLogger()
        self.mongoDbFileMappingObj = mongoDbFileMapping()
        logger.info("Initiated class mainExecuteModule(object)")
        self.importRequestId = importRequestId
        self.sourceId = sourceId 
        self.fileId = fileId 
        self.fileName = fileName 
        self.fileType = fileType
        self.created_by_id = created_by_id 
        self.tenant_id = tenant_id, 
        self.sub_tenant_id = sub_tenant_id

    def get_coder_name(self):
        return self.codername


    def execute(self):
        try:
            logger.info("Rules execution started")
            executeRuleStartTime = datetime.datetime.utcnow()
            # Run Sys rules
            logger.info("Total system rules: " + str(len(appLevel.systemRuleConfig)))
            for r in appLevel.systemRuleConfig:
                try:
                    rule = r #appLevel.ruleConfig[r]
                    if((r in self.ruleNameList or rule['RuleType'] in self.ruleTypeList or rule['RuleKey'] in self.ruleNameList) and  rule['IsActive'] == True): #str(rule['IsActive']).upper() == "TRUE"
                        self.executeRule(r)
                except :
                    pass

            #sort json. do not hanlde key error as it is must to have RuleSection and RuleSeqNo
            logger.info("Total user defined rules: " + str(0 if appLevel.ruleConfig == None else len(appLevel.ruleConfig)))
            if appLevel.ruleConfig != None:
                appLevel.ruleConfig  = sorted(appLevel.ruleConfig, key=lambda k: (int(k['RuleSection']), float(k['RuleSeqNo'])), reverse=False)
                for r in appLevel.ruleConfig:
                    try:
                        rule = r #appLevel.ruleConfig[r]
                        if((r in self.ruleNameList or rule['RuleType'] in self.ruleTypeList or rule['RuleKey'] in self.ruleNameList) and  rule['IsActive'] == True):
                            self.executeRule(r)
                    except :
                        pass
            # clennUp and Write the data buffer back to file after all rules are exceuted
            self.cleanUp()
            logger.info("Rules execution completed")
            executeRuleEndTime = datetime.datetime.utcnow()
            ruleExecutionTime = executeRuleEndTime - executeRuleStartTime
            datetime.timedelta(0, 8, 562000)
            logger.info('Rules execution time : {0}'.format(str(divmod(ruleExecutionTime.days * 86400 + ruleExecutionTime.seconds, 60))))

            if (appLevel.appConfig['DEFAULT']['WRITE_FOUTPUT_JSON'] == "TRUE"):
                logger.info("Writing final data to output file")
                finaloutputdata_file_name = "../" + appLevel.appConfig['DEFAULT']['DATAFOLDER'] + '/FOutput_' + appLevel.currentDateTimeStampString + '.json'
                with open(finaloutputdata_file_name, 'w') as outfile:
                    #json.dump(appLevel.massagedJsonData, outfile, sort_keys=True, indent=4) 
                    json.dump(appLevel.massagedJsonData, outfile) 
                logger.info("Finished writing final data to output file")        
        except Exception as e:
            logger.error("Exception: Main execute error {0}. \n {1}\n".format(str(e), e.args))
            logger.exception(e)



    def executeRule(self, r):
        rule = r #appLevel.ruleConfig[r]
        try:
            if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                logger.info("Rule listed:- Key/Type: " + rule['RuleKey'] + "/" + rule['RuleType']  + "  ||  Sec/Seq: " + rule['RuleSection'] + "/" + rule['RuleSeqNo'])

            if( rule['RuleType'] == "ColumnMap"):
                columnMapManagerObj = columnMapManagerModule.columnMapManager(rule)
                columnMapManagerObj.executeRule()
                columnMapManagerObj = None
            elif rule['RuleType'] == "SplitToJson": 
                splitToJsonManagerObj = splitToJsonManagerModule.splitToJsonManager(rule)
                splitToJsonManagerObj.executeRule()  
                splitToJsonManagerObj = None
            elif rule['RuleType'] == "Split" : 
                splitRuleManagerObj = splitRuleManagerModule.splitRuleManager(rule)
                splitRuleManagerObj.executeRule()  
                splitRuleManagerObj = None
            elif rule['RuleType'] == "Merge": 
                mergeRuleManagerObj = mergeRuleManagerModule.mergeRuleManager(rule)
                mergeRuleManagerObj.executeRule()  
                mergeRuleManagerObj = None
            elif (rule['RuleType'] == "NPI"):
                ruleNpiManagerObj = ruleNpiManagerModule.ruleNpiManager(rule)
                ruleNpiManagerObj.executeRule()
                ruleNpiManagerObj = None
            elif (rule['RuleType'] == "SSN"):
                ruleSsnManagerObj = ruleSsnManagerModule.ruleSsnManager(rule)
                ruleSsnManagerObj.executeRule()
                ruleSsnManagerObj = None
            elif (rule['RuleType'] == "IXTimeStamp"):
                ruleIXTimeStampManagerObj = ruleIXTimeStampManagerModule.ruleIXTimeStampManager(rule, self.importRequestId, self.sourceId, self.fileId, self.fileName, self.fileType)
                ruleIXTimeStampManagerObj.executeRule()
                ruleIXTimeStampManagerObj = None
            elif( rule['RuleType'] == "StringRemove"):
                stringRemoveManagerObj = stringRemoveManagerModule.stringRemoveManager(rule)
                stringRemoveManagerObj.executeRule()
                stringRemoveManagerObj = None
            elif( rule['RuleType'] == "StringReplace"):
                stringReplaceManagerObj = stringReplaceManagerModule.stringReplaceManager(rule)
                stringReplaceManagerObj.executeRule()
                stringReplaceManagerObj = None
            elif( rule['RuleType'] == "Conditional"):
                ruleConditionalManagerObj = ruleConditionalManagerModule.ruleConditionalManager(rule)
                ruleConditionalManagerObj.executeRule()
                ruleConditionalManagerObj = None
            elif( rule['RuleType'] == "StringAppend"):
                stringAppendManagerObj = stringAppendManagerModule.stringAppendManager(rule)
                stringAppendManagerObj.executeRule()
                stringAppendManagerObj = None
            elif( rule['RuleType'] == "StringPrepend"):
                stringPrependManagerObj = stringPrependManagerModule.stringPrependManager(rule)
                stringPrependManagerObj.executeRule()
                stringPrependManagerObj = None
            elif( rule['RuleType'] == "StringLowerUpper"):
                stringLowerUpperManagerObj = stringLowerUpperManagerModule.stringLowerUpperManager(rule)
                stringLowerUpperManagerObj.executeRule()
                stringLowerUpperManagerObj = None
            elif( rule['RuleType'] == "PickChar"):
                pickCharManagerObj = pickCharManagerModule.pickCharManager(rule)
                pickCharManagerObj.executeRule()
                pickCharManagerObj = None
            elif( rule['RuleType'] == "RemoveColumn"):
                removeColumnManagerObj = removeColumnManagerModule.removeColumnManager(rule)
                removeColumnManagerObj.executeRule()
                removeColumnManagerObj = None
            else:
                logger.info("No rule executed")

        except Exception as e:
            logger.debug("Could not execute in mainExecute.executeRule class")
            logger.error("Could not execute in mainExecute.executeRule class")
            logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(rule['RuleKey'], str(e), e.args))


    
    def cleanUp(self):
        try:
            logger.info("CleanUp initiated...")
            # After deep Copy remove the original template
            for r in appLevel.massagedJsonData:
                record = appLevel.massagedJsonData[r] 
                for colhead_array_name in appLevel.appConfig['OUTPUT_COLUMNHEAD_KEY']:
                   #val = 'ProviderSpeciality' and appLevel.appConfig['OUTPUT_COLUMNHEAD_KEY'][val] = 'Speciality'
                   if(colhead_array_name != 'ProviderDemographics' and colhead_array_name.upper() != 'STAGED'):
                       #record['Provider'][colhead_array_name]
                       array_key_col = appLevel.appConfig['OUTPUT_COLUMNHEAD_KEY'][colhead_array_name]
                       #logger.info("array_key_col: {a}".format(a=array_key_col))
                       for arr in record['Provider'][colhead_array_name]:
                           #logger.info("{colhead} array_key_col: {a}".format(colhead=colhead_array_name,a=array_key_col))
                           if(arr[array_key_col] == "" or arr[array_key_col] == None):
                                record['Provider'][colhead_array_name].remove(arr)
            logger.info("CleanUp completed!")
        except Exception as e:
            logger.error("CleanUp issue occured!")
            logger.debug("Exception: cleanUp issue.\n{0}\n{1} \n".format(e, e.args))
            logger.exception(e)
            pass
