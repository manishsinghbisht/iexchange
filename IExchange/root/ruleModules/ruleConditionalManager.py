import logging
from logzero import logger
import json
import io
import root.appLevel as appLevel
import datetime
from jsonmerge import merge, Merger
import copy
from root.ruleModules import Assign
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping


class ruleConditionalManager(object):
    """description of class"""

    # Class level attribute
    RuleType = "Conditional"
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
            #logger.info("Initiating execution of rule {0}".format(self.Rule['RuleKey']))
            try:
                rule = self.Rule
                #Actual rule logic is here
                for r in appLevel.massagedJsonData:
                    if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                        logger.info("Executing {0} for row {1}".format(rule['RuleKey'], r))
                    try:
                        record = appLevel.massagedJsonData[r]
                        input_column = rule['InputColumn']
                        output_column = rule['OutputColumn']
                        # Assign default valude defined by default
                        c1 = "" if record[rule['ConditionInputColumn']] == None else record[rule['ConditionInputColumn']]

                        comp2Arr = rule['ConditionCompareValue'].split(',')
                        #comp2 = rule['ConditionCompareValue']
                        
                        draftValueArr =  rule['DefaultValue'].split(',')
                        #draftValue = rule['DefaultValue']

                        draftValue = ""
                        try:
                            # Static val
                            if(rule['InputColumn'] == ""): 
                                if(rule['ConditionOperator'] == "="):
                                    for (c2, dval) in zip(comp2Arr, draftValueArr): 
                                        if(c1 == c2):
                                            draftValue = dval
                                elif(rule['ConditionOperator'] == "<"):
                                    for (c2, dval) in zip(comp2Arr, draftValueArr): 
                                        if(c1 < c2):
                                            draftValue = dval
                                elif(rule['ConditionOperator'] == "!="):
                                    for (c2, dval) in zip(comp2Arr, draftValueArr): 
                                        if(c1 != c2):
                                            draftValue = dval
                            # Output from col
                            elif(rule['InputColumn'] != ""): # Output from col
                                if(rule['ConditionOperator'] == "="):
                                    for (c2, dval) in zip(comp2Arr, draftValueArr): 
                                        if(c1 == c2):
                                            draftValue = Assign.AssignInputVal(rule, record)
                                elif(rule['ConditionOperator'] == "!="):
                                    for (c2, dval) in zip(comp2Arr, draftValueArr): 
                                        if(c1 != c2):
                                            draftValue = Assign.AssignInputVal(rule, record)
                        except :
                            record[input_column] = ''
                            if rule['ErrorOverrideWithDefaultValue'] == True :
                                draftValue = rule['DefaultValue']
                        
                        # Do assignment only if draft value is assigned with some value (which will only be in case of true)
                        if (draftValue != "" and draftValue != '' and draftValue != None): Assign.AssignVal(rule, record, draftValue)                        
                        
                        if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                            self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Success", message="Success")
                            logger.info("Completed {0} for {1}".format(rule['RuleKey'], r))
                            
                    except Exception as e:
                        logger.error("Error in {0} for {1}".format(rule['RuleKey'], r))
                        logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
                        self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Failed", message=e.args)
                  
                #Log completition
                #logger.info("Completed execution of rule {0}".format(self.Rule['RuleKey']))

            except Exception as e:
                logger.info("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
                self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Failed", message=e.args)