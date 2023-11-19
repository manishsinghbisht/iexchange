import logging
from logzero import logger
import json
import io
import root.appLevel as appLevel
import datetime
from jsonmerge import merge
import random
import copy
from root.ruleModules import Assign
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping

class ruleSsnManager(object):
    """description of class"""

    # Class level attribute
    RuleType = "SSN"
    Rule = None
    logger = logging.getLogger()
    mongoDbFileMappingObj = None

 # Constructor
    def __init__(self, rule):
        self.mongoDbFileMappingObj = mongoDbFileMapping()
        #logger.info('Initiated class SSN(object)')
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
                        logger.info("Executing {0} for {1}".format(self.Rule['RuleKey'], r))
                    try:
                        record = appLevel.massagedJsonData[r]
                        output_column_head = rule['OutputColumnHead']
                        input_column = rule['InputColumn']
                        output_column = rule['OutputColumn']
                        try:
                            draftValue = record[input_column]
                        except :
                            record[input_column] = ''
                            draftValue = ''
                        ######################
                        #Generate Randon SSN
                        for x in range(10):
                            draftValue = random.randint(111111111,999999999)
                        
                        ######################

                        if (rule['OutputColumnHead'] == "ProviderDemographics"):
                            # Scalar Object
                            record['Provider'][rule['OutputColumnHead']][rule['OutputColumn']] = draftValue
                        else:
                            # Array/List Type Object
                            base = { rule['OutputColumnHead']: copy.deepcopy(appLevel.templateJsonData['Provider'][rule['OutputColumnHead']]) }
                            merger = Merger(appLevel.mergeSchema)
                            base[rule['OutputColumnHead']][0][rule['OutputColumn']] = draftValue
                            if(rule['OutputColumn'] == "SSN"):
                                base[rule['OutputColumnHead']][0]['Ssn'] = draftValue
                            elif(rule['OutputColumn'] == "Ssn"):
                                base[rule['OutputColumnHead']][0]['SSN'] = draftValue
                            record['Provider'] = merger.merge(record['Provider'], base)

                        if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                            logger.info("Completed {0} for row {1}".format(self.Rule['RuleKey'], r))
                            
                    except Exception as e:
                        logger.info("Error in {0} for row {1}".format(self.Rule['RuleKey'], r))
                        logger.info("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), str(e.args)))
                            
                ## Log completition
                #logger.info("Completed execution of rule {0}".format(self.Rule['RuleKey']))

            except Exception as e:
                logger.info("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), str(e.args)))
