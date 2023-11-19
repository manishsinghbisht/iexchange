import logging
from logzero import logger
import random
from random import randint
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import root.appLevel as appLevel
import json
from bson import json_util 
import io
from datetime import datetime

class mongoDbFileMapping(object):
    """description of class"""

    # Class level attribute
    codername = "Manish"

    # Constructor
    def __init__(self):
        #self.logger = logging.getLogger()
        #logger.info("Initiated class mongoDbFileMapping(object)")
        mongoConnectionClient = appLevel.appConfig['MongoConnections']['Client'] 
        mongoConnectionDatabase = appLevel.appConfig['MongoConnections']['Database'] 
        self.connectionstring = mongoConnectionClient # Instance level attribute, accessible across instance metahod

        #To connect to a MongoDB instance with authentication enabled, specify
        #a URI in the following format:
        # mongodb://[username:password@]host1[:port1]
        #Ex: client = MongoClient('mongodb://alice:abc123@localhost:27017')
        client = MongoClient(self.connectionstring)
        try:
            # The ismaster command is cheap and does not require auth.
            client.admin.command('ismaster')
        except ConnectionFailure as cf:
            logger.error("ConnectionFailure: Mongo Server not available")
            logger.error("Exception: Main error {0}.\n{1}\n".format(cf, cf.args))
        except Exception as e:
            logger.error("Mongo Connection Error")
            logger.error("Exception: Main error {0}.\n{1}\n".format(e, e.args))

        # Get the database
        self.db = client[mongoConnectionDatabase]
        # Issue the serverStatus command and print the results
        #serverStatusResult = self.db.command("serverStatus")
        #print(serverStatusResult)


    def get_coder_name(self):
        return self.codername


    # Showcasing the insert
    def remove_and_insert(self):
        self.db.FileMappingRules.remove({})
        for r in appLevel.ruleConfig:
            rule = r #appLevel.ruleConfig[r]
            #Step 3: Insert business object directly into MongoDB via
            #insert_one
            result = self.db.FileMappingRules.insert_one(rule)
            #Step 4: Print to the console the ObjectID of the new document
            #logger.info('Created: {0}'.format(result.inserted_id))


    # Showcasing the update
    def update(self):
        originalDocument = self.db.FileMappingRules.find_one({'name' : 'Manish'})
        pprint(originalDocument)
        result = self.db.FileMappingRules.update_one({'_id' : originalDocument.get('_id') }, {'$inc': {'likes': 1}})
        print('Number of documents modified : ' + str(result.modified_count))
        UpdatedDocument = db.FileMappingRules.find_one({'_id':originalDocument.get('_id')})
        print('The updated document:')
        pprint(UpdatedDocument)


    # Returns a queuedImportRequests
    def get_QueuedImportRequest(self):
        if self.db.IX_IMPORT_REQUEST.count_documents({'SourceRequestStatus': 'Queued'}) > 0:
            # 'RequestStatus': 'Queued'
            queuedImportRequests = self.db.IX_IMPORT_REQUEST.find({'SourceRequestStatus': 'Queued'})
            return queuedImportRequests;
        else:
            return None


    # Returns a queuedImportRequests for source
    def get_QueuedImportRequestBySource(self, sourceId):
        if self.db.IX_IMPORT_REQUEST.count_documents({'SourceSetupId': sourceId, 'SourceRequestStatus': 'Queued'}) > 0:
            queuedImportRequests = self.db.IX_IMPORT_REQUEST.find({'SourceSetupId': sourceId, 'SourceRequestStatus': 'Queued'})
            return queuedImportRequests;
        else:
            return None

    # Updates a queuedImportSourceRequests
    def update_IX_Import_SourceRequest(self, requestId, newRequestStatus, errorDesc = None):
        originalDocument = self.db.IX_IMPORT_REQUEST.find_one({'_id' : requestId})
        #pprint(originalDocument)
        if newRequestStatus == "InProcess":
            result = self.db.IX_IMPORT_REQUEST.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'SourceRequestStatus': newRequestStatus, 'StartDate' : str(datetime.now()), 'EndDate' : None }},  upsert=False)
            return result.modified_count;
        elif newRequestStatus == "Completed":
            result = self.db.IX_IMPORT_REQUEST.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'SourceRequestStatus': newRequestStatus, 'EndDate' : str(datetime.now()) }},  upsert=False)
        elif newRequestStatus == "Error":
            result = self.db.IX_IMPORT_REQUEST.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'SourceRequestStatus': newRequestStatus, 'EndDate' : str(datetime.now()), 'ErrorDescription' : str(errorDesc) }},  upsert=False)
        else:
            result = self.db.IX_IMPORT_REQUEST.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'SourceRequestStatus': newRequestStatus }},  upsert=False)
            return result.modified_count;

    # Updates a queuedImportSourceRequests
    def update_IX_Import_SourceRequest_InProcess2Completed(self, requestId):
        originalDocument = self.db.IX_IMPORT_REQUEST.find_one({'_id' : requestId, 'SourceRequestStatus': 'InProcess'})
        #pprint(originalDocument)
        if originalDocument != None:
            result = self.db.IX_IMPORT_REQUEST.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'SourceRequestStatus': 'Completed', 'EndDate' : str(datetime.now()) }},  upsert=False)
            #print('Number of documents modified : ' + str(result.modified_count))
            return result.modified_count;


     # Updates a ImportFileRequests
    def update_IX_Import_FileRequest(self, requestId, fileId, newRequestStatus, errorDesc = None):
        originalDocument = self.db.IX_IMPORT_REQUEST.find_one({'_id' : requestId})
        #pprint(originalDocument)
        if newRequestStatus == "InProcess":
            result = self.db.IX_IMPORT_REQUEST.update_one({'_id' : originalDocument.get('_id'), 'Files.Id': fileId}, 
                                                          { 
                                                            '$set': { 'Files.$.FileRequestStatus' : newRequestStatus, 'Files.$.StartDate' : str(datetime.now()), 'Files.$.EndDate' : None }
                                                           },
                                                          upsert=False)
        elif newRequestStatus == "Completed":
            result = self.db.IX_IMPORT_REQUEST.update_one({'_id' : originalDocument.get('_id'), 'Files.Id': fileId}, 
                                                          { 
                                                            '$set': { 'Files.$.FileRequestStatus' : newRequestStatus, 'Files.$.EndDate' : str(datetime.now()) }
                                                           },
                                                          upsert=False)
        elif newRequestStatus == "Error":
            result = self.db.IX_IMPORT_REQUEST.update_one({'_id' : originalDocument.get('_id'), 'Files.Id': fileId}, 
                                                          { 
                                                            '$set': { 'Files.$.FileRequestStatus' : newRequestStatus, 'Files.$.EndDate' : str(datetime.now()), 'Files.$.ErrorDescription' : str(errorDesc) }
                                                           },
                                                          upsert=False)
        else:
            result = self.db.IX_IMPORT_REQUEST.update_one({'_id' : originalDocument.get('_id'), 'Files.Id': fileId}, 
                                                          { 
                                                            '$set': { 'Files.$.FileRequestStatus' : newRequestStatus }
                                                           },
                                                          upsert=False)

        #print('Number of documents modified : ' + str(result.modified_count))
        return result.modified_count;


    # Returns a cursor over all documents that match the search criteria
    def get_all_ColumnMappings(self, fileId, fileName):
        try:
            collection = self.db['FileMappingRules']
            if collection.count_documents({'FileId': fileId}) == 0:
                appLevel.ruleConfig = None
                return

            mappings = collection.find({'FileId': fileId}).sort([("RuleSection", 1), ("RuleSeqNo", 1)])

            ## To write rule to a new file 
            # json.dump(json_util.dumps(mappings), open("mappings.json", "w"))
            appLevel.ruleConfig = None
            ruleFileName = "rulemappings_"+ appLevel.currentDateTimeStampString + ".json"
            ##json loads -> returns an object from a string representing a json object.
            ##json dumps -> returns a string representing a json object from an object.
            ##load and dump -> read/write from/to file instead of string

            rulesJsonString = json_util.dumps(mappings)
            appLevel.ruleConfig = json.loads(rulesJsonString)
            with io.open("../" + appLevel.appConfig['DEFAULT']['DATAFOLDER'] + '/' + ruleFileName, 'w', encoding='utf8') as outfile:
                outfile.write(rulesJsonString)

        except Exception as e:
            print("\n Exception: get_all_ColumnMappings {0}.\n{1}\n".format(e, e.args))
            logger.exception(e)



    # Returns a cursor over all documents that match the search criteria
    def get_ColumnMapping(self, sourceFile, sourceColumnName):
        mapping = self.db.FileMappingRules.find({'SourceFile': sourceFile, 'SourceColumnName': sourceColumnName})
        print(mapping)


    # Showcasing the count() method of find, count the total number of 5
    # ratings
    def get_ColumnMappings_count(self, sourceFile, sourceColumnName):
        fivestarcount = self.db.FileMappingRules.find({'SourceFile': sourceFile, 'SourceColumnName': sourceColumnName}).count()
        print(fivestarcount)

    # Showcasing the insert
    def insert_Final_Output(self):
        try:
            data = appLevel.massagedJsonData
            
            #Step 3: Insert business object directly into MongoDB via
            #insert_one
            if(appLevel.appConfig['DEFAULT']['UPDATE_LOG_IN_DB'] == "TRUE"):
                with open(appLevel.log_file_name, 'r') as d:
                    logData = d.read()
                    d.close
                    # data['Log_'+ appLevel.instanceInitiateDateTimeStampString] = {appLevel.currentDateTimeStampString : logData}

                result = self.db.IX_PY_RESULT.insert_one({"IXTimeStamp" : appLevel.currentDateTimeStampString, "data" : data, "log" : logData})
            else:
                result = self.db.IX_PY_RESULT.insert_one({"IXTimeStamp" : appLevel.currentDateTimeStampString, "data" : data})

            #Step 4: Print to the console the ObjectID of the new document
            # logger.info('Created: {0}'.format(result.inserted_id))
        except Exception as e:
            logger.debug("Could not execute in insert_Final_Output. "+ str(e))
            logger.exception(e)

    
            
    def createIXLogForRecordInMongoDB(self, mongoImportRequestId, ixTimeStamp, sourceId, fileId, fileName, fileType, IX_REC_UID, UNIQUE_API_HIT_ID):
        IX_LOG_DOCUMENT = {
            'Source':'IEXCHANGE',
            'ixTimeStamp' : ixTimeStamp,
            'IX_REC_UID': IX_REC_UID,
            'UNIQUE_API_HIT_ID' : UNIQUE_API_HIT_ID,
            'mongoImportRequestId': mongoImportRequestId,
            'SourceId':sourceId,
            'FileId' : fileId,
            'FileName' : fileName,
            'FileType' : fileType,
            'ixRulesStatus':[],
            'ixLastStatus':'',
            'APIResponse':'',
            'StatusLog':[],
            'ProviderStatus': {
                    'ProviderDemographics':[]
                }
        }

        #Step 3: Insert business object directly into MongoDB via isnert_one
        result = self.db.IX_LOG.insert_one(IX_LOG_DOCUMENT)
        #Step 4: Print to the console the ObjectID of the new document
        #logger.info('Created: {0}'.format(result.inserted_id))
        return result.inserted_id
  

    # Updates Rule status in Mongo
    def updateRuleStatusInIXLogInMongoDB(self, IX_REC_UID, rule, status, message):
        originalDocument = self.db.IX_LOG.find_one({'IX_REC_UID' : IX_REC_UID})
        DOCUMENT_RULE = { 
            'IX_REC_UID': IX_REC_UID,
            'FileId': rule['FileId'],
            'RuleKey': rule['RuleKey'], 
            'RuleType': rule['RuleType'], 
            'RuleType': rule['RuleType'], 
            'RuleSection': rule['RuleSection'],
            'RuleSeqNo': rule['RuleSeqNo'],
            'InputColumn': rule['InputColumn'], 
            'InputColumnHead': rule['InputColumnHead'],
            'OutputColumn': rule['OutputColumn'], 
            'OutputColumnHead': rule['OutputColumnHead'],
            'Status':status,
            'Message':message
            }

        result = self.db.IX_LOG.update_one({'_id' : originalDocument.get('_id')}, 
                                                      { 
                                                        '$set': { 'ixLastStatus' : 'Executing Rules' },
                                                        '$push': {'StatusLog': 'Executing Rules'},
                                                        '$push': {'ixRulesStatus': DOCUMENT_RULE}
                                                       },
                                                      upsert=False)
        #print('Number of documents modified : ' + str(result.modified_count))
        return result.modified_count;

    # Updates status in Mongo
    def updateAPIResponseInIXLogInMongoDB(self, IX_REC_UID, response):
        originalDocument = self.db.IX_LOG.find_one({'IX_REC_UID' : IX_REC_UID})

        result = self.db.IX_LOG.update_one({'_id' : originalDocument.get('_id')}, 
                                                      { 
                                                        '$set': { 'ixLastStatus' : 'API Call', 'ixAPIResponseRcvd':response },
                                                        '$push': {'StatusLog': 'API Call'},
                                                       },
                                                      upsert=False)
        #print('Number of documents modified : ' + str(result.modified_count))
        return result.modified_count;

     # Updates UUID status in Mongo log
    def update_IX_REC_UID_InIXLogInMongoDB(self, _id, IX_REC_UID):
        originalDocument = self.db.IX_LOG.find_one({'_id' : _id})

        result = self.db.IX_LOG.update_one({'_id' : originalDocument.get('_id')}, 
                                                      { 
                                                        '$set': { 'IX_REC_UID' : IX_REC_UID }
                                                       },
                                                      upsert=False)
        #print('Number of documents modified : ' + str(result.modified_count))
        return result.modified_count;