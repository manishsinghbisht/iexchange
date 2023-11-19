import logging
from logzero import logger
import json
import io
import root.appLevel as appLevel
import datetime
from jsonmerge import merge
import random
import copy
import uuid
from root.ruleModules import Assign
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping

class ruleIXTimeStampManager(object):
    """description of class"""

    # Class level attribute
    RuleType = "IXTimeStamp"
    Rule = None
    #logger = logging.getLogger()
    mongoDbFileMappingObj = None

 # Constructor
    def __init__(self, rule, importRequestId, sourceId, fileId, fileName, fileType):
        self.mongoDbFileMappingObj = mongoDbFileMapping()
        self.importRequestId = importRequestId
        self.sourceId = sourceId 
        self.fileId = fileId 
        self.fileName = fileName 
        self.fileType = fileType
        #logger.info('Initiated class SSN(object)')
        if(self.RuleType != rule['RuleType']): 
            logger.error('Rule mismatch. Expected: {0}, Received: {1}'.format(self.RuleType,rule['RuleType']))
        else:
            self.Rule = rule

    def executeRule(self):
            # Log Initiation
            # logger.info("Execution of rule {0}".format(self.Rule['RuleKey']))
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
                            record['IXTimeStamp'] = appLevel.currentDateTimeStampString
                            record['IX_REC_UID'] =  str(uuid.uuid4())
                            record['UNIQUE_API_HIT_ID'] = str(uuid.uuid4())
                            record['ImportRequestId'] =  str(self.importRequestId)
                            record['SourceId'] =  str(self.sourceId)
                            record['FileId'] =  str(self.fileId)
                            record['FileName'] =  str(self.fileName)
                            record['FileType'] =  str(self.fileType)
                        except :
                            record[input_column] = ''
                            draftValue = ''

                        self.mongoDbFileMappingObj.createIXLogForRecordInMongoDB(mongoImportRequestId = self.importRequestId, ixTimeStamp = appLevel.currentDateTimeStampString, sourceId = self.sourceId, fileId = self.fileId, fileName = self.fileName, fileType = self.fileType, IX_REC_UID = record['IX_REC_UID'], UNIQUE_API_HIT_ID = record['UNIQUE_API_HIT_ID'])
                        if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                            logger.info("Completed {0} for row {1}".format(self.Rule['RuleKey'], r))
                    except Exception as e:
                        logger.error("Error in {0} for row {1}".format(self.Rule['RuleKey'], r))
                        logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
                        
                            
                # Log completition
                #logger.info("Completed execution of rule {0}".format(self.Rule['RuleKey']))

            except Exception as e:
                logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
                