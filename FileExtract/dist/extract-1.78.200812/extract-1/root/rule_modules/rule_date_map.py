import logging
from logzero import logger
from root.rule_modules import Assign
import json
import io
import root.app_level as app_level
import datetime
from jsonmerge import merge, Merger
import copy


class rule_date_map(object):
    """description of class"""

    # Class level attribute
    RuleType = "DateMap"
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
                # Assign input to draftValue
                draftValue = Assign.Assign_Input_To_Draft(rule, record)

                ##strptime() method creates a datetime object from a given string (representing date and time).
                #date_string = "24 June, 2020"
                #print("date_string =", date_string)
                #date_object = datetime.datetime.strptime(date_string, "%d %B, %Y")

                ##strftime() - datetime object to string
                s1 = datetime.datetime.now().strftime("%Y/%m/%d, %H:%M:%S")

                try:
                    if rule['InputColumn'] != "":
                        #date_string = "21 June, 2018"
                        # string to dattime object
                        if type(draftValue) == datetime.datetime:
                            date_format = rule['date_format']
                            if(date_format == 'm/d/yyyy'):
                                draftValue = draftValue.strftime('%m/%d/%Y')
                            elif(date_format == 'MM/DD/YYYY'):
                                draftValue = draftValue.strftime('%M/%D/%Y')
                            elif(date_format == 'MMDDYYYY'):
                                draftValue = draftValue.strftime('%M/%D/%Y')
                                draftValue = str(draftValue)
                                draftValue = draftValue.replace('/','')
                        else:
                            draftValue = str(draftValue)
                            
                except Exception as e:
                    logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
                    record[rule['InputColumn']] = ''
                    if rule['ErrorOverrideWithDefaultValue'] == True :
                        draftValue = rule['DefaultValue']
                # Assign draftValue to output        
                Assign.Assign_Draft_To_Output(rule, record, draftValue)                        
                if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                    logger.info("Completed {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
            except Exception as e:
                logger.error("Error in {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
                logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
