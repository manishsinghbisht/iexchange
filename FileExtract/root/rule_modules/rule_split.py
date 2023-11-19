import logging
from logzero import logger
from root.rule_modules import Assign
import json
import io
import root.app_level as app_level
import datetime
from jsonmerge import merge, Merger
import copy

class rule_split(object):
    """description of class"""

    # Class level attribute
    RuleType = "Split"
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
        if rule['InputColumnType'] != "JSON":
            logger.error("Split rule is only available for InputColumnType = JSON. {0}".format(rule['RuleKey']))
            return

        if rule['InputColumnHead'] == None or rule['InputColumnHead'] == '':
            logger.error("InputColumnHead required for rule {0}".format(rule['RuleKey']))
            return

        #Actual rule logic is here
        for r in app_level.massagedJsonData:
            record = r
            if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                logger.info("Executing {0} for row {1}".format(rule['RuleKey'], record['IX_REC_UID']))
            try:
                # Assign input to draftValue
                thisList = []
                output_column_head = rule['OutputColumnHead']
                output_column_array = rule['OutputColumn']
                input_column_head = rule['InputColumnHead']
                inputColumn = rule['InputColumn']

                draftValues = Assign.Assign_Input_To_Draft(rule, record)
                delimiter_type = rule['DelimiterType']
                if delimiter_type == "Length":
                    # Split the string based length
                    draftlist = list(self.chunkstring(draftValues, int(rule['Delimiter'])))
                else:
                    # Split the string based on space or delimiter
                    draftlist = draftValues.split(rule['Delimiter']) 

                 # Apend for each repeat of column to a dictionary with
                # ColumnName:Value
                for num, name in enumerate(draftlist, start=0):
                    if len(output_column_array) > num:
                        #thisdict[output_column_array[num]] = name
                        Assign.Assign_Draft_To_Output(rule, record, draft_value = name, output_column_head = None, output_column = output_column_array[num])
                
                if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                    logger.info("Completed {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
            except Exception as e:
                logger.error("Error in {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
                logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))


    # Separates string in chunks by length
    # Ex: list(chunkstring("abcdefghijklmnopqrstuvwxyz", 5))
    #output: ['abcde', 'fghij', 'klmno', 'pqrst', 'uvwxy', 'z']
    def chunkstring(self, string, length):
        #do_work
        return (string[0+i:length+i] for i in range(0, len(string), length))
