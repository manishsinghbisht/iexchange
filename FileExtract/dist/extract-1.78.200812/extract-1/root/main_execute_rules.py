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
import root.app_level as app_level
import datetime
from root.db_modules.mongo_db import mongo_db
import root.rule_modules.rule_IXTimeStamp as rule_IXTimeStamp
import root.rule_modules.rule_column_map as rule_column_map
import root.rule_modules.rule_default_fixed as rule_default_fixed
import root.rule_modules.rule_json_map as rule_json_map
import root.rule_modules.rule_append as rule_append
import root.rule_modules.rule_prepend as rule_prepend
import root.rule_modules.rule_replace as rule_replace
import root.rule_modules.rule_remove as rule_remove
import root.rule_modules.rule_split as rule_split
import root.rule_modules.rule_merge as rule_merge
import root.rule_modules.rule_lower_upper as rule_lower_upper
import root.rule_modules.rule_conditional as rule_conditional
import root.rule_modules.rule_phone_map as rule_phone_map
import root.rule_modules.rule_date_map as rule_date_map
import root.rule_modules.rule_time_map as rule_time_map
import root.rule_modules.rule_pick_char as rule_pick_char

import root.rule_modules.rule_post_process_print_text as rule_post_process_print_text


class main_execute_rules(object):
    """description of class"""

    # Class level attribute
    codername = "Manish"
    ruleTypeList = ["IXTimeStamp", "ColumnMap", "DefaultFixed", "JsonMap", "PhoneMap", "DateMap", "TimeMap", "Rule_NPI", "Rule_SSN", "Merge", "Split", "StringRemove", "Conditional", "StringAppend", "StringPrepend", "StringLowerUpper", "StringReplace", "PickChar"]
    ruleNameList = ["IXTimeStamp", "Rule_NPI", "Rule_SSN", "Merge", "Split"]
    rulePostProcessTypeList = ["PostProcess_PrintText", "PostProcess_PrintRecordCount"]
    mongoDbFileMappingObj = None

    # Constructor
    def __init__(self, importRequestId, sourceId, fileId, fileName, fileType, dataScope, layout_id):
        #self.logger = logging.getLogger()
        self.mongo_db = mongo_db()
        logger.info("Initiated class mainExecuteModule(object)")
        self.importRequestId = importRequestId
        self.sourceId = sourceId 
        self.fileId = fileId 
        self.fileName = fileName 
        self.fileType = fileType
        self.dataScope = dataScope
        self.layout_id = layout_id
        
        #System rule config check
        with open('systemRuleConfig.json', 'r') as f:
            app_level.systemRuleConfig = json.load(f)
            f.close()

        # Standard Rules
        std_rule_config_name = app_level.appConfig['DEFAULT']['RULE_CONFIG_STD'] 
        if(std_rule_config_name == None or std_rule_config_name == ''):
                std_rule_config_name = "ruleConfig.json"

        with open(std_rule_config_name, 'r') as rc:
            app_level.ruleConfig = json.load(rc)
        

        # Custom Rules. This loads mappings in app_level.customRuleConfig
        pvt_rule_config_name = app_level.appConfig['DEFAULT']['RULE_CONFIG_PVT'] 
        if(pvt_rule_config_name == None or pvt_rule_config_name == ''):
            self.mongo_db.load_extract_mappings(layout_id = self.layout_id)
        else:
             with open(pvt_rule_config_name, 'r') as rc:
                app_level.customRuleConfig = json.load(rc)

    
    def get_coder_name(self):
        return self.codername


    def execute(self):
        try:
            logger.info("Rules execution started")
            executeRuleStartTime = datetime.datetime.utcnow()
            # Run Sys rules
            logger.info("Total system rules: " + str(len(app_level.systemRuleConfig)))
            for r in app_level.systemRuleConfig:
                try:
                    rule = r #app_level.ruleConfig[r]
                    if((r in self.ruleNameList or rule['RuleType'] in self.ruleTypeList or rule['RuleKey'] in self.ruleNameList) and  rule['IsActive'] == True): #str(rule['IsActive']).upper() == "TRUE"
                        self.executeRule(r)
                except :
                    pass

            #sort json. do not handle key error as it is must to have RuleSection and RuleSeqNo
            logger.info("Total standard rules: " + str(0 if app_level.ruleConfig == None else len(app_level.ruleConfig)))
            if app_level.ruleConfig != None:
                app_level.ruleConfig  = sorted(app_level.ruleConfig, key=lambda k: (int(k['RuleSection']), float(k['RuleSeqNo'])), reverse=False)
                for r in app_level.ruleConfig:
                    try:
                        rule = r #app_level.ruleConfig[r]
                        if((r in self.ruleNameList or rule['RuleType'] in self.ruleTypeList or rule['RuleKey'] in self.ruleNameList) and  rule['IsActive'] == True):
                            self.executeRule(r)
                    except :
                        pass
            
            #sort json. do not hanlde key error as it is must to have RuleSection and RuleSeqNo
            logger.info("Total custom user defined rules: " + str(0 if app_level.customRuleConfig == None else len(app_level.customRuleConfig)))
            if app_level.customRuleConfig != None:
                app_level.customRuleConfig  = sorted(app_level.customRuleConfig, key=lambda k: (int(k['RuleSection']), float(k['RuleSeqNo'])), reverse=False)
                #Custom rule execution
                for r in app_level.customRuleConfig:
                    try:
                        rule = r 
                        if((r in self.ruleNameList or rule['RuleType'] in self.ruleTypeList or rule['RuleKey'] in self.ruleNameList) and  rule['IsActive'] == True):
                            self.executeRule(r)
                    except :
                        pass
                

            # cleanUp and Write the data buffer back to file after all rules are exceuted
            self.cleanUp()
            logger.info("Rules execution completed")
            executeRuleEndTime = datetime.datetime.utcnow()
            ruleExecutionTime = executeRuleEndTime - executeRuleStartTime
            datetime.timedelta(0, 8, 562000)
            logger.info('Rules execution time : {0}'.format(str(divmod(ruleExecutionTime.days * 86400 + ruleExecutionTime.seconds, 60))))
        except Exception as e:
            logger.error("Exception: Main execute error {0}. \n {1}\n".format(str(e), e.args))
            logger.exception(e)



    def executeRule(self, r):
        rule = r #app_level.ruleConfig[r]
        try:
            if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                logger.info("Rule listed:- Key/Type: " + rule['RuleKey'] + "/" + rule['RuleType']  + "  ||  Sec/Seq: " + rule['RuleSection'] + "/" + rule['RuleSeqNo'])

            if (rule['RuleType'] == "IXTimeStamp"):
                ruleIXTimeStampManagerObj = rule_IXTimeStamp.rule_IXTimeStamp(rule, self.importRequestId, self.sourceId, self.fileId, self.fileName, self.fileType)
                ruleIXTimeStampManagerObj.executeRule()
                ruleIXTimeStampManagerObj = None
            elif( rule['RuleType'] == "ColumnMap"):
                columnMapManagerObj = rule_column_map.rule_column_map(rule)
                columnMapManagerObj.executeRule()
                columnMapManagerObj = None
            elif( rule['RuleType'] == "DefaultFixed"):
                defaultfixedManagerObj = rule_default_fixed.rule_default_fixed(rule)
                defaultfixedManagerObj.executeRule()
                defaultfixedManagerObj = None
            elif( rule['RuleType'] == "JsonMap"):
                jsonMapManagerObj = rule_json_map.rule_json_map(rule)
                jsonMapManagerObj.executeRule()
                jsonMapManagerObj = None
            elif( rule['RuleType'] == "StringAppend"):
                stringAppendManagerObj = rule_append.rule_append(rule)
                stringAppendManagerObj.executeRule()
                stringAppendManagerObj = None
            elif( rule['RuleType'] == "StringPrepend"):
                stringPrependManagerObj = rule_append.rule_prepend(rule)
                stringPrependManagerObj.executeRule()
                stringPrependManagerObj = None
            elif( rule['RuleType'] == "StringReplace"):
                stringReplaceManagerObj = rule_replace.rule_replace(rule)
                stringReplaceManagerObj.executeRule()
                stringReplaceManagerObj = None
            elif( rule['RuleType'] == "StringRemove"):
                stringRemoveManagerObj = rule_remove.rule_remove(rule)
                stringRemoveManagerObj.executeRule()
                stringRemoveManagerObj = None
            elif( rule['RuleType'] == "StringLowerUpper"):
                stringLowerUpperManagerObj = rule_lower_upper.rule_lower_upper(rule)
                stringLowerUpperManagerObj.executeRule()
                stringLowerUpperManagerObj = None
            elif( rule['RuleType'] == "Merge"):
                stringMergeManagerObj = rule_merge.rule_merge(rule)
                stringMergeManagerObj.executeRule()
                stringMergeManagerObj = None
            elif( rule['RuleType'] == "Split"):
                stringSplitManagerObj = rule_split.rule_split(rule)
                stringSplitManagerObj.executeRule()
                stringSplitManagerObj = None
            elif( rule['RuleType'] == "Conditional"):
                ruleConditionalManagerObj = rule_conditional.rule_conditional(rule)
                ruleConditionalManagerObj.executeRule()
                ruleConditionalManagerObj = None
            elif( rule['RuleType'] == "DateMap"):
                rule_date_map_obj = rule_date_map.rule_date_map(rule)
                rule_date_map_obj.executeRule()
                rule_date_map_obj = None
            elif( rule['RuleType'] == "TimeMap"):
                rule_time_map_obj = rule_time_map.rule_time_map(rule)
                rule_time_map_obj.executeRule()
                rule_time_map_obj = None
            elif( rule['RuleType'] == "PhoneMap"):
                rule_phone_map_obj = rule_phone_map.rule_phone_map(rule)
                rule_phone_map_obj.executeRule()
                rule_phone_map_obj = None
            elif( rule['RuleType'] == "PickChar"):
                rule_pick_char_obj = rule_pick_char.rule_pick_char(rule)
                rule_pick_char_obj.executeRule()
                rule_pick_char_obj = None
            elif( rule['RuleType'] == "PostProcess_PrintText"):
                rule_pick_char_obj = rule_pick_char.rule_pick_char(rule)
                rule_pick_char_obj.executeRule()
                rule_pick_char_obj = None
            else:
                logger.info("No rule executed")

        except Exception as e:
            logger.error("Could not execute in mainExecute.executeRule class")
            logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(rule['RuleKey'], str(e), e.args))


    
    def cleanUp(self):
        try:
            logger.info("cleanUp initiated")
            # After deep Copy remove the original template
            for r in app_level.massagedJsonData:
                record = r 
                for colhead_array_name in app_level.appConfig['OUTPUT_COLUMNHEAD_KEY']:
                   #val = 'ProviderSpeciality' and app_level.appConfig['OUTPUT_COLUMNHEAD_KEY'][val] = 'Speciality'
                   if(colhead_array_name != 'ProviderDemographics'):
                       record['Provider'][colhead_array_name]
                       array_key_col = app_level.appConfig['OUTPUT_COLUMNHEAD_KEY'][colhead_array_name]
                       #logger.info("array_key_col: {a}".format(a=array_key_col))
                       for arr in record['Provider'][colhead_array_name]:
                           #logger.info("{colhead} array_key_col: {a}".format(colhead=colhead_array_name,a=array_key_col))
                           if(arr[array_key_col] == "" or arr[array_key_col] == None):
                                record['Provider'][colhead_array_name].remove(arr)
            logger.info("cleanUp completed")
        except Exception as e:
            logger.error("cleanUp failed")
            logger.error("Exception- cleanUp failed.. Col_Head={0},  e={1} , args={2} \n".format(colhead_array_name, e, e.args))
            logger.exception(e)


    def execute_post_process(self, file=None):
        if file==None:
            logger.error("After file completion rules execution missing output file name! ")
            return
        if app_level.customRuleConfig != None:
            app_level.customRuleConfig  = sorted(app_level.customRuleConfig, key=lambda k: (int(k['RuleSection']), float(k['RuleSeqNo'])), reverse=False)
            #PostProcess rule execution
            for r in app_level.customRuleConfig:
                try:
                    rule = r 
                    if((rule['RuleType'] in self.rulePostProcessTypeList) and  rule['IsActive'] == True):
                        self.executePostProcessRule(r, file)
                except :
                    pass


    def executePostProcessRule(self, r, file=None):
        rule = r #app_level.ruleConfig[r]
        try:
            if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                logger.info("Rule listed:- Key/Type: " + rule['RuleKey'] + "/" + rule['RuleType']  + "  ||  Sec/Seq: " + rule['RuleSection'] + "/" + rule['RuleSeqNo'])

            if (rule['RuleType'] == "PostProcess_PrintText"):
                obj = rule_post_process_print_text.rule_post_process_print_text(rule, file)
                obj.executeRule()
                obj = None
            else:
                logger.info("No rule executed")

        except Exception as e:
            logger.error("Could not execute in mainExecute.executeRule class")
            logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(rule['RuleKey'], str(e), e.args))