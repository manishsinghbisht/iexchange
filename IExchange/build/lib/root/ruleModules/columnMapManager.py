import logging
from logzero import logger
from root.ruleModules import Assign
import json
import io
import root.appLevel as appLevel
import datetime
from jsonmerge import merge, Merger
import copy
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping

class columnMapManager(object):
    """description of class"""

    # Class level attribute
    RuleType = "ColumnMap"
    Rule = None
    logger = logging.getLogger()
    mongoDbFileMappingObj = None

 # Constructor
    def __init__(self, rule):
        self.mongoDbFileMappingObj = mongoDbFileMapping()
        # logger.info('Initiated class columnMapManager(object)')
        if(self.RuleType != rule['RuleType']): 
            logger.error('Rule mismatch : {0}'.format(self.Rule))
        else:
            self.Rule = rule


    def executeRule(self):
        # Log Initiation
        #logger.info("Execution of rule {0}".format(self.Rule['RuleKey']))
        try:
            rule = self.Rule
            #Actual rule logic is here
            for r in appLevel.massagedJsonData:
                try:
                    if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                        logger.info("Executing {0} for {1}".format(self.Rule['RuleKey'], r))
                    record = appLevel.massagedJsonData[r]
                    input_column = rule['InputColumn']
                    output_column = rule['OutputColumn']
                    # Assign default valude defined by default
                    draftValue = rule['DefaultValue']
                    draftValue = Assign.AssignInputVal(rule, record)
                    Assign.AssignVal(rule, record, draftValue)                        
                    if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                        self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Success", message="Success")
                        logger.info("Completed {0} for {1}".format(rule['RuleKey'], r))

                except Exception as e:
                    logger.error("Error in {0} for {1}".format(rule['RuleKey'], r))
                    logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
                    self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Failed", message=e.args)
                
        except Exception as e:
            logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
            self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Failed", message=e.args)
