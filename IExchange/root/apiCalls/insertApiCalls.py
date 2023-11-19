import requests
import json
import logging
from logzero import logger
import root.appLevel as appLevel
from jsonmerge import merge
import copy
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping

class insertApiCalls(object):
    """description of class"""
    
   # Class level attribute
    codername = "Manish"
   
   
   # Constructor
    def __init__(self, jsonData='{}'):
        self.jsonData = jsonData  # Instance level attribute, accessible across instance metahod
        #self.logger = logging.getLogger()
        self.mongoObj = mongoDbFileMapping()

    def get_coder_name(self):
        return self.codername


    #def ExtractApiImportId(self, resp):
    #    try:
    #        #startIndex = resp.find('apiImportId:') # apiImportId:5bab48feedc32905400a8b6c
    #        #startIndex = startIndex + 12
    #        #endIndex = resp.find(' ***')
    #        #newUNQUE_API_HIT_ID = resp[startIndex:endIndex]
    #        #if(self.UNQUE_API_HIT_ID == None):
    #        #    self.UNQUE_API_HIT_ID = newUNQUE_API_HIT_ID
    #        #else:
    #        #    if(self.UNQUE_API_HIT_ID != newUNQUE_API_HIT_ID):
    #        #        raise ValueError("Import Id mismatch. UNQUE_API_HIT_ID {0} is not equal to new UNQUE_API_HIT_ID {1}".format(self.UNQUE_API_HIT_ID, newUNQUE_API_HIT_ID))
    #    except ValueError as e:
    #        #print('API HIT Id mismatch. UNQUE_API_HIT_ID != newUNQUE_API_HIT_ID')
    #        #logger.info("\t API HIT Id mismatch. UNQUE_API_HIT_ID {0} is not equal to newUNQUE_API_HIT_ID {1}".format(self.UNQUE_API_HIT_ID, newUNQUE_API_HIT_ID))
    #    except Exception as e:
    #        #print("\t Error in Extracting UNQUE_API_HIT_ID ")
    #        #logger.info("\t Error in Extracting UNQUE_API_HIT_ID ")

    
    def insert_addProvider(self, jsonData='{}'):
        try:
            if(appLevel.appConfig['API']['CALL_API'] == "TRUE"):
                url = 'http://localhost:31529/api/IExchange/IExchangeApiCall'
                url = appLevel.appConfig['API']['API_URL']
                record = None
                if(appLevel.appConfig['API']['USE_TEST_JSON'] == "TRUE"):
                    with open('JsonForAPITest.json', 'r') as testJson:
                        record = json.load(testJson)
                    # Call API
                    response = requests.post(url, json = record)
                    #Recieve response
                    response_content = 'API response status for test json: ' + str(response.status_code) + '|| reason: ' + str(response.reason) + ' || json: ' + str(response.json) + ' || content: ' + str(response.content)
                    logger.info(response_content)
                else:
                    for r in appLevel.massagedJsonData:
                        try:
                            # r = requests.get(url, auth=('user', 'pass'))
                            # Get the prepared/massaged data
                            record = appLevel.massagedJsonData[r] 
                   
                            # Call API
                            response = requests.post(url, json = record)
                            #Recieve response
                            response_content = 'API response status: ' + str(response.status_code) + '|| reason: ' + str(response.reason) + ' || json: ' + str(response.json) + ' || content: ' + str(response.content)
                            self.mongoObj.updateAPIResponseInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], response = response_content);
                            if(appLevel.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                                logger.info(response_content)
                        except Exception as e:
                            logger.error("Error in insert_addProvider for record {0}".format(r))
                            #logger.error("Exception insert_addProvider API call for record: {0}. \n {1} \n {2} \n".format(r, str(e), str(e.args)))
                            logger.exception(e)
                            self.mongoObj.updateAPIResponseInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], response = "Error in API Call");
            else:
                logger.info("API call defferred.")
                # record is out of scope here, so can't update database
                # self.mongoObj.updateAPIResponseInIXLogInMongoDB(IX_REC_UID = record['IX_REC_UID'], response = "API Call deferred!");

        except Exception as e:
            #logger.error("Exception: Could not execute {0}.\n {1} \n {2} \n".format('insert_addProvider', str(e), e.args))
            logger.exception(e)

    def iexchange_cache_init(self, jsonData='{}'):
        if(appLevel.appConfig['API']['CALL_API'] == "TRUE"):
            url = 'http://localhost:31529/api/IExchange/IExchangeApiCallCacheInit'
            url = appLevel.appConfig['API']['IX_CACHE_INIT']
            record = None
            try:
                # r = requests.get(url, auth=('user', 'pass'))
                # Get the prepared/massaged data
                # Call API
                response = requests.post(url)
                #Recieve response
                response_content = 'IX Cache init status: ' + str(response.status_code) + '|| reason: ' + str(response.reason) + ' || content: ' + str(response.content)                    
                logger.info(response_content)
            except Exception as e:
                logger.error("Error in iexchange_cache_init")
                logger.exception(e)
        else:
            logger.info("iexchange_cache_init: API call defferred.")
            

