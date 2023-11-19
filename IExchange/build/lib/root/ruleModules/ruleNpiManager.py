import logging
from logzero import logger
import json
import io
import root.appLevel as appLevel
import datetime
from jsonmerge import merge
import copy
from root.ruleModules import Assign
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping

class ruleNpiManager(object):
    """description of class"""

    # Class level attribute
    RuleType = "NPI"
    Rule = None
    logger = logging.getLogger()
    mongoDbFileMappingObj = None

 # Constructor
    def __init__(self, rule):
        self.mongoDbFileMappingObj = mongoDbFileMapping()
        #logger.info('Initiated class NPI(object)')
        if(self.RuleType != rule['RuleType']): 
            logger.error('Rule mismatch. Expected: {0}, Received: {1}'.format(self.RuleType,rule['RuleType']))
        else:
            self.Rule = rule

    def executeRule(self):
            # Log Initiation
            #logger.info("Execution of rule {0}".format(self.Rule['RuleKey']))
            try:
                rule = self.Rule
                #Actual rule logic is here
                for r in appLevel.massagedJsonData:
                    if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                        logger.info("\tExecuting {0} for row {1}".format(self.Rule['RuleKey'], r))
                    try:
                        record = appLevel.massagedJsonData[r]
                        output_column_head = rule['OutputColumnHead']
                        input_column = rule['InputColumn']
                        output_column = rule['OutputColumn']
                        try:
                            draftValue = int(record[input_column])
                            if len(str(abs(draftValue))) == 10 and str(abs(draftValue)).isdigit():
                                record['Provider'][rule['OutputColumnHead']]["NPITypes"] = "ACTUAL"
                                record['Provider'][rule['OutputColumnHead']][rule["OutputColumn"]] = draftValue
                            else:
                                record['Provider'][rule['OutputColumnHead']]["NPITypes"] = "DUMMY"
                                record['Provider'][rule['OutputColumnHead']][rule["OutputColumn"]] = ''
                        except :
                            record['Provider'][rule['OutputColumnHead']]["OutputColumn"] = ''
                            record['Provider'][rule['OutputColumnHead']]["NPITypes"] = "DUMMY"
                            if rule['ErrorOverrideWithDefaultValue'] == True :
                                record['Provider'][rule['OutputColumnHead']][rule["OutputColumn"]] = rule['DefaultValue']

                        if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                            logger.info("Completed {0} for row {1}".format(self.Rule['RuleKey'], r))
                            
                    except Exception as e:
                        logger.info("Error in {0} for row {1}".format(self.Rule['RuleKey'], r))
                        logger.info("Exception: Could not execute rule {0}.\n{1} \n {2} \n".format(self.Rule['RuleKey'], str(e), e.args))

                # Log completition
                #logger.info("Completed execution of rule {0}".format(self.Rule['RuleKey']))

            except Exception as e:
                logger.info("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
