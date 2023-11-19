import logging
from logzero import logger
from root.rule_modules import Assign
import json
import io
import root.app_level as app_level
import datetime
from jsonmerge import merge, Merger
import copy

class rule_default_fixed(object):
    """description of class"""

    # Class level attribute
    RuleType = "DefaultFixed"
    Rule = None
    logger = logging.getLogger()
    #mongoDbFileMappingObj = None

 # Constructor
    def __init__(self, rule):
        if(self.RuleType != rule['RuleType']): 
            logger.error('Rule mismatch : {0}'.format(self.Rule))
        else:
            self.Rule = rule


    def executeRule(self):
        rule = self.Rule

        if rule['InputColumnType'] != "JSON":
            logger.error("DefaultFixed rule is only available for InputColumnType = JSON. Rule: {0}".format(rule['RuleKey']))
            return
        
        if rule['OutputColumn'] == None or rule['OutputColumn'] == '':
            logger.error("OutputColumn required. Rule {0}".format(rule['RuleKey']))
            return
        
        #Actual rule logic is here
        for r in app_level.massagedJsonData:
            try:
                record = r
                output_column_head = rule['OutputColumnHead']
                output_column = rule['OutputColumn']
                defaultValue = rule['DefaultValue']
                
                # Assign draftValue to output
                Assign.Assign_Draft_To_Output(rule, record, defaultValue, output_column_head = output_column_head, output_column = output_column) 
                
                if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                    #self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Success", message="Success")
                    logger.info("Completed {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
            except Exception as e:
                logger.error("Error in {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
                logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
                #self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Failed", message=e.args)