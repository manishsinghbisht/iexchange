import logging
from logzero import logger
import json
import io
import root.app_level as app_level
import datetime
from jsonmerge import merge, Merger
import copy
from root.rule_modules import Assign

class rule_conditional(object):
    """description of class"""

    # Class level attribute
    RuleType = "Conditional"
    Rule = None
    logger = logging.getLogger()
    
 # Constructor
    def __init__(self, rule):
        if(self.RuleType != rule['RuleType']): 
            logger.error('Rule mismatch : {0}'.format(self.Rule))
        else:
            self.Rule = rule


    def executeRule(self):
        rule = self.Rule
        #Actual rule logic is here
        for r in app_level.massagedJsonData:
            record = r

            if not rule['ConditionInputColumn'] in record.keys():
                logger.error("ConditionInputColumn not present in record. Error in {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
                return

            if rule["InputColumnType"] == "DB":
                c1 = "" if record[rule['ConditionInputColumn']] == None else record[rule['ConditionInputColumn']]
            elif rule["InputColumnType"] == "JSON":
                c1 = "" if record[rule['ConditionInputColumn']] == None else Assign.Assign_Input_To_Draft(rule, record, input_column_head = rule['ConditionInputColumnHead'], input_column = rule['ConditionInputColumn'], draft_value = '')
                            
            comp2Arr = rule['ConditionCompareValue'].split(',')
            #comp2 = rule['ConditionCompareValue']
                        
            draftValueArr =  rule['DefaultValue'].split(',')
            #draftValue = rule['DefaultValue']

            draftValue = ""
            try:
                # Static value case, when you have to pick a default fixed value if condition mets 
                if(rule['InputColumn'] == ""): 
                    if(rule['ConditionOperator'] == "="):
                        for (c2, dval) in zip(comp2Arr, draftValueArr): 
                            if(c1 == c2):
                                draftValue = dval
                    elif(rule['ConditionOperator'] == "!="):
                        for (c2, dval) in zip(comp2Arr, draftValueArr): 
                            if(c1 != c2):
                                draftValue = dval
                    elif(rule['ConditionOperator'] == "<="):
                        for (c2, dval) in zip(comp2Arr, draftValueArr): 
                            if(c1 <= c2):
                                draftValue = dval
                    elif(rule['ConditionOperator'] == ">="):
                        for (c2, dval) in zip(comp2Arr, draftValueArr): 
                            if(c1 >= c2):
                                draftValue = dval
                # Output from col case, when you have to pick a value from input column if condition mets 
                elif(rule['InputColumn'] != ""): # Output from col
                    if(rule['ConditionOperator'] == "="):
                        for (c2, dval) in zip(comp2Arr, draftValueArr): 
                            if(c1 == c2):
                                draftValue = Assign.Assign_Input_To_Draft(rule, record)
                    elif(rule['ConditionOperator'] == "!="):
                        for (c2, dval) in zip(comp2Arr, draftValueArr): 
                            if(c1 != c2):
                                draftValue = Assign.Assign_Input_To_Draft(rule, record)
                    elif(rule['ConditionOperator'] == "<="):
                        for (c2, dval) in zip(comp2Arr, draftValueArr): 
                            if(c1 <= c2):
                                draftValue = Assign.Assign_Input_To_Draft(rule, record)
                    elif(rule['ConditionOperator'] == ">="):
                        for (c2, dval) in zip(comp2Arr, draftValueArr): 
                            if(c1 >= c2):
                                draftValue = Assign.Assign_Input_To_Draft(rule, record)
            except Exception as e:
                logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
                record[rule['InputColumn']] = ''
                if rule['ErrorOverrideWithDefaultValue'] == True :
                    draftValue = rule['DefaultValue']

            Assign.Assign_Draft_To_Output(rule, record, draftValue)  
                        
            if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                logger.info("Completed {0} for {1}".format(rule['RuleKey'], r))

