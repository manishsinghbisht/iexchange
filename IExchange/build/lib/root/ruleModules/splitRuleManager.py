import logging
from logzero import logger
import json
import io
import root.appLevel as appLevel
import datetime
from jsonmerge import merge
from root.ruleModules import Assign
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping

class splitRuleManager(object):
    """description of class"""

    # Class level attribute
    RuleType = "Split"
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
        # Log Initiation
        #logger.info("Execution of rule {0}".format(self.Rule['RuleKey']))

        for r in appLevel.massagedJsonData:
            record = appLevel.massagedJsonData[r]
            if (self.Rule['OutputColumnHead'] == "ProviderDemographics"):
                self.executeSplit(record['Provider'][self.Rule['OutputColumnHead']], record, r)
            else:
                for arrVal in record['Provider'][self.Rule['OutputColumnHead']]:
                    self.executeSplit(arrVal, record, r)


    def executeSplit(self, dataset, record, r):
        # Log Initiation
        rule = self.Rule

        #Actual rule logic is here
        if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
            logger.info("Executing {0} for row {1}".format(self.Rule['RuleKey'], r))
        #else:
        #    logger.info("Executing {0} for row {1} : {2}".format(self.Rule['RuleKey'], r, dataset))
        
        try:
            output_column_head = rule['OutputColumnHead']
            input_column = rule['InputColumn']
            outputColumnsArray = rule['OutputColumn']
            thisdict = {}
            try:
                draftValues = Assign.AssignInputVal(rule, record)
                key = "DelimiterType"
                if key in rule.keys():
                    delimiter_type = rule['DelimiterType']
                    if delimiter_type == "Length":
                        # Split the string based length
                        draftlist = list(self.chunkstring(draftValues, int(rule['Delimiter'])))
                    else:
                        # Split the string based on space or delimiter
                        draftlist = draftValues.split(rule['Delimiter']) 
                else:#Remove this condition once all rule configs contain key "DelimiterType". This is temporary to support old deployment
                    # Split the string based on space or delimiter
                    draftlist = draftValues.split(rule['Delimiter']) 

                # Apend for each repeat of column to a dictionary with
                # ColumnName:Value
                for num, name in enumerate(draftlist, start=0):
                    thisdict[outputColumnsArray[num]] = name   #or we can write: draftlist[num]
            except Exception as e:
                logger.error("Exception: Could not execute rule {0}.\n {1}\n {2} \n".format(self.Rule['RuleKey'], str(e), e.args))
                draftValue = ''
                if rule['ErrorOverrideWithDefaultValue'] == True :
                    draftValue = rule['DefaultValue']
                      
            for dkey, dvalue in thisdict.items():
                # Scalar Object
                dataset[dkey] = dvalue
            
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
