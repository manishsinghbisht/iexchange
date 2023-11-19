import logging
from logzero import logger
from root.rule_modules import Assign
import json
import io
import root.app_level as app_level
import datetime
from jsonmerge import merge, Merger
import copy

class rule_lower_upper(object):
    """description of class"""

    # Class level attribute
    RuleType = "StringLowerUpper"
    Rule = None
    logger = logging.getLogger()
    

 # Constructor
    def __init__(self, rule):
        if(self.RuleType != rule['RuleType']): 
            logger.error('Rule mismatch : {0}'.format(self.Rule))
        else:
            self.Rule = rule

    def __repr__(self):
        return self.RuleType + ' ' + 'Manager'

    def executeRule(self):
        rule = self.Rule
        #Actual rule logic is here
        for r in app_level.massagedJsonData:
            record = r
            if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                logger.info("Executing {0} for row {1}".format(rule['RuleKey'], record['IX_REC_UID']))
            try:
                operation = rule['Operation'].upper()
                # Assign input to draftValue
                draftValue = Assign.Assign_Input_To_Draft(rule, record)
                if operation == 'LOWER':
                    draftValue = str(Assign.AssignInputVal(rule, record)).lower()
                elif operation == 'UPPER':
                    draftValue = str(Assign.AssignInputVal(rule, record)).upper()

                # Assign draftValue to output
                Assign.Assign_Draft_To_Output(rule, record, draftValue)                        
                        
                if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                    logger.info("Completed {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
            except Exception as e:
                logger.error("Error in {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
                logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
