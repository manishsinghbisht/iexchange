import logging
from logzero import logger
from root.rule_modules import Assign
import json
import io
import root.app_level as app_level
import datetime
from jsonmerge import merge, Merger
import copy

class rule_column_map(object):
    """description of class"""

    # Class level attribute
    RuleType = "ColumnMap"
    Rule = None
    logger = logging.getLogger()
    #mongoDbFileMappingObj = None

 # Constructor
    def __init__(self, rule):
        #self.mongoDbFileMappingObj = mongoDbFileMapping()
        # logger.info('Initiated class columnMapManager(object)')
        if(self.RuleType != rule['RuleType']): 
            logger.error('Rule mismatch : {0}'.format(self.Rule))
        else:
            self.Rule = rule


    def executeRule(self):
        rule = self.Rule
        #Actual rule logic is here
        for r in app_level.massagedJsonData:
            try:
                record = r
                input_column = rule['InputColumn']
                output_column = rule['OutputColumn']

                if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                    logger.info("Executing {0} for {1}".format(self.Rule['RuleKey'], record['IX_REC_UID']))
                
                if input_column == "":
                    logger.error("InputColumn name cannot be blank or null. Error in {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
                    return

                # Assign input to draftValue
                draftValue = Assign.Assign_Input_To_Draft(rule, record)
                # Assign draftValue to output
                Assign.Assign_Draft_To_Output(rule, record, draftValue) 
                
                if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                    #self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Success", message="Success")
                    logger.info("Completed {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
            except Exception as e:
                logger.error("Error in {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
                logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
                #self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Failed", message=e.args)