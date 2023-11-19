import logging
from logzero import logger
import json
import io
import root.appLevel as appLevel
import datetime
from jsonmerge import merge
from root.ruleModules import Assign
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping
import uuid

class splitToJsonManager(object):
    """description of class"""

    # Class level attribute
    RuleType = "SplitToJson"
    Rule = None
    logger = logging.getLogger()
    mongoDbFileMappingObj = None

    # Constructor
    def __init__(self, rule):
        self.mongoDbFileMappingObj = mongoDbFileMapping()
        #logger.info('Initiated class splitRuleManager(object)')

        if(self.RuleType not in rule['RuleType']): 
            logger.error('Rule mismatch. Expected: {0}, Received: {1}'.format(self.RuleType,rule['RuleType']))
        else:
            # Assign the rule name here as Split and Merger are generic rules
            self.Rule = rule
            self.RuleType = rule['RuleType'] 


    def executeRule(self):
        if (self.Rule['OutputColumnHead'] == "ProviderDemographics" or self.Rule['OutputColumnHead'] == "STAGED"):
                logger.error("SplitToJson type rule cannot be applied to demographic or staged json objects!")
                return
        
        self.executeSplit()


    def executeSplit(self):
        # Log Initiation
        rule = self.Rule       
        try:
            # There will be no InputColumnHead in SplitToJson rule
            output_column_head = rule['OutputColumnHead']
            input_column_dict = rule['InputColumn']
            output_column_dict = rule['OutputColumn']
            delimiter_dict = rule['Delimiter']
            delimiter_type = rule['DelimiterType']
           

            if len(input_column_dict) != len(output_column_dict):
                logger.error("Error in rule {0}. Input columns and output columns item counts are not equal!".format(self.Rule['RuleKey']))
                return
            
            if len(input_column_dict) != len(delimiter_dict):
                logger.error("Error in rule {0}. Delimiters and I/O columns counts are not equal!".format(self.Rule['RuleKey']))
                return

            try:
                for r in appLevel.massagedJsonData:
                    record = appLevel.massagedJsonData[r]
                    # Actual rule logic is here
                    if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                        logger.info("Executing {0} for row {1}".format(self.Rule['RuleKey'], r))

                     # Clear OutputColumnHeadof empty object present default in template
                    record['Provider'][output_column_head] = []
                    
                    # for key, i_col in input_column_dict.items():
                    for i in range(0,  len(input_column_dict)): 
                        key = str(i+1)
                        i_col = input_column_dict[key].strip()
                        o_col_arr = str(output_column_dict[key]).split(",")
                        o_col_arr_length = len(o_col_arr) 
                        for i in range(o_col_arr_length): 
                            o_col_arr[i] = o_col_arr[i].strip()

                        delimiter = delimiter_dict[key]
                        draftValues = record[i_col]
                        if delimiter_type == "Length":
                            # Split the string based length
                            draftlist = list(self.chunkstring(draftValues, int(delimiter)))
                        else:
                            # Split the string based on space or delimiter
                            draftlist = draftValues.split(delimiter) 

                        # Apend for each repeat of column to a dictionary with
                        # ColumnName:Value
                        for num, name in enumerate(draftlist, start=0):
                            if len(o_col_arr) > num:
                                if("1" == key):
                                    record['Provider'][output_column_head].append({"SYSKEY":str(uuid.uuid4()), o_col_arr[num].strip():name.strip()})
                                else:
                                    record['Provider'][output_column_head][num][o_col_arr[num].strip()] = name.strip() 
            except Exception as e:
                logger.error("Exception: Could not execute rule {0}.\n {1}\n {2} \n".format(self.Rule['RuleKey'], str(e), e.args))
           
            if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Success", message="Success")
                logger.info("Completed {0} for row {1}".format(self.Rule['RuleKey'], r))
        except Exception as e:
            logger.error("Error in {0} for row {1}".format(self.Rule['RuleKey'], r))
            logger.error("Exception: Could not execute rule {0}.\n {1}\n {2} \n".format(self.Rule['RuleKey'], str(e), e.args))
            self.mongoDbFileMappingObj.updateRuleStatusInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], rule=rule, status="Failed", message=e.args)             


    # Separates string in chunks by length
    # Ex: list(chunkstring("abcdefghijklmnopqrstuvwxyz", 5))
    #output: ['abcde', 'fghij', 'klmno', 'pqrst', 'uvwxy', 'z']
    def chunkstring(self, string, length):
        #do_work
        return (string[0+i:length+i] for i in range(0, len(string), length))
