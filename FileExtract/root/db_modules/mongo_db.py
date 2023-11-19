import logging
from logzero import logger
import random
from random import randint
import pymongo
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import root.app_level as app_level
import json
from bson import json_util 
import io

class mongo_db(object):
    """description of class"""

    # Class level attribute
    codername = "Manish"

    # Constructor
    def __init__(self):
        #self.logger = logging.getLogger()
        #logger.info("Initiated class mongoDbFileMapping(object)")
        mongoConnectionClient = app_level.appConfig['MongoConnections']['Client'] 
        mongoConnectionDatabase = app_level.appConfig['MongoConnections']['Database'] 
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
        for r in app_level.ruleConfig:
            rule = r #app_level.ruleConfig[r]
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
    def update_extract_request(self, requestId, newRequestStatus, desc = None):
        if app_level.appConfig['DEFAULT']['DEBUG'] == "TRUE":
            return
        originalDocument = self.db.extract_request.find_one({'_id' : requestId})
        #pprint(originalDocument)
        if newRequestStatus == "InProcess":
            result = self.db.extract_request.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'ExtractRequestStatus': newRequestStatus, 'StartDate' : str(datetime.now()), 'EndDate' : None }},  upsert=False)
            return result.modified_count;
        elif newRequestStatus == "Completed":
            result = self.db.extract_request.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'ExtractRequestStatus': newRequestStatus, 'EndDate' : str(datetime.now()) }},  upsert=False)
        elif newRequestStatus == "Error":
            result = self.db.extract_request.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'ExtractRequestStatus': newRequestStatus, 'EndDate' : str(datetime.now()), 'ErrorDescription' : str(desc) }},  upsert=False)
        else:
            result = self.db.extract_request.update_one({'_id' : originalDocument.get('_id') }, {'$set': {'ExtractRequestStatus': newRequestStatus }},  upsert=False)
            return result.modified_count;


     # Updates a ImportFileRequests
    def update_extract_request_file(self, requestId, fileId, newRequestStatus, desc = None):
        if app_level.appConfig['DEFAULT']['DEBUG'] == "TRUE":
            return
        originalDocument = self.db.extract_request.find_one({'_id' : requestId})
        #pprint(originalDocument)
        if newRequestStatus == "InProcess":
            result = self.db.extract_request.update_one({'_id' : originalDocument.get('_id'), 'Files.Id': fileId}, 
                                                          { 
                                                            '$set': { 'Files.$.FileRequestStatus' : newRequestStatus, 'Files.$.StartDate' : str(datetime.now()), 'Files.$.EndDate' : None }
                                                           },
                                                          upsert=False)
        elif newRequestStatus == "Completed":
            result = self.db.extract_request.update_one({'_id' : originalDocument.get('_id'), 'Files.Id': fileId}, 
                                                          { 
                                                            '$set': { 'Files.$.FileRequestStatus' : newRequestStatus, 'Files.$.EndDate' : str(datetime.now()), 'Files.$.output_file_name' : str(desc) }
                                                           },
                                                          upsert=False)
        elif newRequestStatus == "Error":
            result = self.db.extract_request.update_one({'_id' : originalDocument.get('_id'), 'Files.Id': fileId}, 
                                                          { 
                                                            '$set': { 'Files.$.FileRequestStatus' : newRequestStatus, 'Files.$.EndDate' : str(datetime.now()), 'Files.$.ErrorDescription' : str(desc) }
                                                           },
                                                          upsert=False)
        else:
            result = self.db.extract_request.update_one({'_id' : originalDocument.get('_id'), 'Files.Id': fileId}, 
                                                          { 
                                                            '$set': { 'Files.$.FileRequestStatus' : newRequestStatus }
                                                           },
                                                          upsert=False)

        #print('Number of documents modified : ' + str(result.modified_count))
        return result.modified_count;



    # Returns a cursor over all documents that match the search criteria
    def load_extract_mappings(self, layout_id = ''):
        app_level.customRuleConfig = None
        try:
            collection = self.db['extract_rules']
            if collection.count_documents({'layout_id': layout_id}) == 0:
                app_level.customRuleConfig = None
                return
            logger.info("Getting custom rules for layout_id: {0}".format(layout_id))
            mappings = collection.find({'layout_id': layout_id}).sort([("RuleSection", 1), ("RuleSeqNo", 1)])
            # db.collection.find({name:{'$regex' : '^string', '$options' : 'i'}})
            #mappings = collection.find({'FileId': file_id, 'RuleType': {'$regex' : '^string', '$options' : 'i'}}).sort([("RuleSection", 1), ("RuleSeqNo", 1)])
            ## To write rule to a new file 
            # json.dump(json_util.dumps(mappings), open("mappings.json", "w"))            
            ruleFileName = "customrulemappings_"+ app_level.currentDateTimeStampString + ".json"
            ##json loads -> returns an object from a string representing a json object.
            ##json dumps -> returns a string representing a json object from an object.
            ##load and dump -> read/write from/to file instead of string
            rulesJsonString = json_util.dumps(mappings)
            app_level.customRuleConfig = json.loads(rulesJsonString)
            with io.open("../" + app_level.appConfig['DEFAULT']['DATAFOLDER'] + '/' + ruleFileName, 'w', encoding='utf8') as outfile:
                outfile.write(rulesJsonString)
        except Exception as e:
            logger.error("Exception: get_all_extract_mappings {0}.\n {1}".format(e, e.args))
            logger.exception(e)

