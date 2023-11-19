import logging
from logzero import logger
from root.ruleModules import Assign
import json
import io
import root.appLevel as appLevel
import datetime
from jsonmerge import merge, Merger
import copy
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping

class removeColumnManager(object):
    """description of class"""

    # Class level attribute
    RuleType = "RemoveColumn"
    Rule = None
    logger = logging.getLogger()
    mongoDbFileMappingObj = None

 # Constructor
    def __init__(self, rule):
        if(self.RuleType != rule['RuleType']): 
            logger.error('Rule mismatch : {0}'.format(self.Rule))
        else:
            self.Rule = rule


    def executeRule(self):
        rule = self.Rule
        #Actual rule logic is here
        for r in appLevel.massagedJsonData:
            try:
                record = appLevel.massagedJsonData[r]
                output_columnHead_key = appLevel.appConfig['OUTPUT_COLUMNHEAD_KEY'][rule['OutputColumnHead']]
                for item in record['Provider'][rule['OutputColumnHead']]:
                    if(item[output_columnHead_key] ==  record[rule['InputColumnHead']]):
                        item.pop(rule['OutputColumn']) 
                        #logger.info("Completed {0} for {1}".format(rule['RuleKey'], r))
            except Exception as e:
                logger.error("Error in {0} for {1}".format(rule['RuleKey'], r))
                