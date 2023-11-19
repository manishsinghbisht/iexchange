import logging
from logzero import logger
import json
import io
import root.appLevel as appLevel
import datetime
from jsonmerge import merge
from root.ruleModules import Assign
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping

class mergeRuleManager(object):
    """description of class"""

    # Class level attribute
    RuleType = "Merge"
    Rule = None
    logger = logging.getLogger()
    mongoDbFileMappingObj = None

 # Constructor
    def __init__(self, rule):
        self.mongoDbFileMappingObj = mongoDbFileMapping()
        #logger.info('Initiated class mergeRuleManager(object)')

        if(self.RuleType not in rule['RuleType']): 
            logger.error('Rule mismatch. Expected: {0}, Received: {1}'.format(self.RuleType,rule['RuleType']))
        else:
            # Assign the rule name here as Split and Merger are generic rules
            self.Rule = rule
            self.RuleType = rule['RuleType'] 

     
    def executeRule(self):
        # Log Initiation
        #logger.info("Execution of rule {0}".format(self.Rule['RuleKey']))

        for r in appLevel.massagedJsonData:
            if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                logger.info("Executing {0} for row {1}".format(self.Rule['RuleKey'], r))

            record = appLevel.massagedJsonData[r]
            if(self.Rule['OutputColumnHead'] == "ProviderDemographics"):
                self.executeMerge(record['Provider'][self.Rule['OutputColumnHead']],record,r)
            else:
                for arrVal in record['Provider'][self.Rule['OutputColumnHead']]:
                    self.executeMerge(arrVal, record, r)


    def executeMerge(self, dataset, record, r):
        rule = self.Rule        
        try:
            output_column_head = rule['OutputColumnHead']
            inputColumnsArray = rule['InputColumn']
            output_column = rule['OutputColumn']
            thisList = []
            try:
                for input_column in inputColumnsArray:
                    if (rule['InputColumnType'].upper() == "STAGED"):
                        tempdraftValue = record['Provider']['Staged'][input_column]
                    else:
                        tempdraftValue = record[input_column]

                    thisList.append(tempdraftValue)
                                
                # Join the string based on '-' delimiter
                draftValue = rule['Delimiter'].join(thisList) 
            except :
                draftValue = ''
                if rule['ErrorOverrideWithDefaultValue'] == True :
                    draftValue = rule['DefaultValue']
                      
            # Scalar Object
            dataset[rule['OutputColumn']] = draftValue 
            
            if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                logger.info("Completed {0} for row {1} ".format(self.Rule['RuleKey'], r))
                self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Success", message="Success")
            
        except Exception as e:
            logger.error("Error in {0} for row {1}".format(self.Rule['RuleKey'], r))
            logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
            self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Failed", message=e.args)
                            
        # Log completition
        # logger.info("Completed {0} for row {1} ".format(self.Rule['RuleKey'], r))
