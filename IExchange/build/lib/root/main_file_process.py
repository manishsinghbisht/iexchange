# python --version
# python main.py

#import configparser
import os
import datetime
import logging
from logzero import logger
import shutil
import root.appLevel as appLevel
import pandas
import json
from jsonmerge import merge
import copy
from root.fileModules.CsvManager import CsvManager
from root.fileModules.FwfManager import FwfManager
import root.main_execute as main_execute
from root.dbModules.mongoDbCon import mongoDbCon
from root.dbModules.mongoDbFileMapping import mongoDbFileMapping
from root.apiCalls.insertApiCalls import insertApiCalls

class mainFileProcess(object):
    """description of class"""
    #logger = logging.getLogger()

     # Constructor
    def __init__(self, importRequestId, sourceId, input_filename, input_filetype, input_fileId = None, input_delimiter = None, col_Names = None, col_Widths = None):
        self.importRequestId = importRequestId
        self.sourceId = sourceId
        self.input_filename = input_filename
        self.input_filetype = input_filetype
        self.input_fileId = input_fileId
        self.input_delimiter = input_delimiter
        self.col_Names = col_Names
        self.col_Widths = col_Widths
        self.mongoObj = mongoDbFileMapping()
        #mainFileProcess check
        logger.info("mainFileProcess run started..")

        #Load output templates to app level var
        with open('dataTemplate.json', 'r') as t:
                logger.info("Reading data template..")
                appLevel.templateJsonData = json.load(t)
                t.close()

        # Update 'FileRequestStatus' in mongoDB 'IX_IMPORT_REQUEST' collection
        if(self.importRequestId != ""):
            logger.info("[REF#0011] Updating file request to InProcess")
            self.mongoObj.update_IX_Import_FileRequest(self.importRequestId, self.input_fileId, "InProcess")


    def __execute(self, inputJsonData):
        appLevel.inputJsonData = inputJsonData
        try:
            # Merge masagedJsonData at appLevel var by merging template json and input json
            appLevel.massagedJsonData = None
            appLevel.massagedJsonData = copy.deepcopy(appLevel.inputJsonData) 
            for r in appLevel.massagedJsonData:
                record = appLevel.massagedJsonData[r]
                base = copy.deepcopy(appLevel.templateJsonData) 
                appLevel.massagedJsonData[r] = merge(base, record)
            
            # Nullify FInput json to clear memory
            appLevel.inputJsonData = None
            logger.info("Completed creation of massagedJsonData file")

            logger.info("Initiating main execute module")
            # Send Json data to execute rules
            mainExecuteObj = main_execute.mainExecuteModule(importRequestId = self.importRequestId, sourceId = self.sourceId, fileId = self.input_fileId, fileName = self.input_filename, fileType = self.input_filetype)
            logger.info("Executing mainExecuteObj.execute()")             
            mainExecuteObj.execute()
            logger.info("Completed mainExecuteObj.execute()")

            #API Call
            logger.info("API: Executing ApiCalls")
            apiStartTime = datetime.datetime.utcnow()
            apiObj = insertApiCalls()
            apiObj.insert_addProvider('{}')
            apiEndTime = datetime.datetime.utcnow()
            logger.info("API: Completed ApiCalls")
            apiTime = apiEndTime - apiStartTime
            datetime.timedelta(0, 8, 562000)
            logger.info('API execution time : {0}'.format(str(divmod(apiTime.days * 86400 + apiTime.seconds, 60))))
           
            #raise Exception('Test File Exception')
           
            # File Processing loop ends
            return "SUCCESS"
        except Exception as e:
            logger.error("[REF#1111] Exception: mainFileProcess {0}.\n {1}\n".format(e, e.args))
            logger.exception(e)
            # Update 'SourceRequestStatus' in mongoDB 'IX_IMPORT_REQUEST' collection after all files are completed
            if(self.importRequestId != ""):
                self.mongoObj.update_IX_Import_FileRequest(self.importRequestId, self.input_fileId, "Error", e)
                return "ERROR"


    def __processChunkData(self, inputJsonData):
        f_result = self.__execute(appLevel.inputJsonData)
        if(f_result == "ERROR"):
            self.mongoObj.update_IX_Import_SourceRequest(self.importRequestId, "Error")


    def process_inputfile_in_chunks(self):
        try:
            # Start Initialize IX cache
            logger.info("Starting Cache Init for IX")
            apiStartTime = datetime.datetime.utcnow()
            apiforCache = insertApiCalls()
            apiforCache.iexchange_cache_init('{}')
            apiEndTime = datetime.datetime.utcnow()
            logger.info("Completed Cache Init for IX")
            apiTime = apiEndTime - apiStartTime
            datetime.timedelta(0, 8, 562000)
            logger.info('API Cache initialization time : {0}'.format(str(divmod(apiTime.days * 86400 + apiTime.seconds, 60))))
            # End Initialize IX cache

            # After cache init, start processing 
            logger.info('Initiating processing file in chunk!')
            chunk_counter = 1
            chunksize =  int(appLevel.appConfig['DEFAULT']['CHUNK_SIZE']) # 10 ** 2
            rawInputfileNamewithPath = appLevel.appConfig['InputFileLocation'] + "/" + self.input_filename
            if self.input_filetype == "CSV":
                csvManagerObject = CsvManager()
                # Default initialization of delimiter
                delimiter = ","
                if self.input_delimiter == None or self.input_delimiter == "":
                    delimiter = ","
                else:
                    delimiter = self.input_delimiter
                # Setup columns
                if self.col_Names != None:
                    my_input_colNames = self.col_Names.split(",") 
                else:
                    df_for_columns = pandas.DataFrame(pandas.read_csv(rawInputfileNamewithPath, chunksize=chunksize, sep = delimiter))
                    my_input_colNames = [1] * len(df_for_columns.columns)
                    
                
                for chunk in pandas.read_csv(rawInputfileNamewithPath, chunksize=chunksize, sep = delimiter, header = 0, index_col = False, encoding = 'unicode_escape', names = my_input_colNames, 
                                            converters={i: str for i in range(0, 10000)}):
                    logger.info('Processing chunk number {0} of {1} records...'.format(str(chunk_counter), str(appLevel.appConfig['DEFAULT']['CHUNK_SIZE'])))
                    # Convert CSV to JSON string and then JSON string to JSON Object
                    appLevel.inputJsonData = json.loads(csvManagerObject.df_ToJson(chunk))
                    # Pass JSON Object for further execution
                    self.__processChunkData(appLevel.inputJsonData)
                    chunk_counter = chunk_counter + 1
            elif self.input_filetype == "FWF":
                fwfManagerObject = FwfManager()
                my_input_colNames = self.col_Names.split(",") 
                my_fwidth = map(int, self.col_Widths.split(","))
                for chunk in pandas.read_fwf(rawInputfileNamewithPath, chunksize=chunksize, widths=my_fwidth, encoding = 'unicode_escape', names=my_input_colNames, 
                                            converters={i: str for i in range(0, len(my_input_colNames))}):
                    logger.info('Processing chunk number {0} of {1} records...'.format(str(chunk_counter), str(appLevel.appConfig['DEFAULT']['CHUNK_SIZE'])))
                    # Convert FWF to JSON string and then JSON string to JSON Object
                    appLevel.inputJsonData = json.loads(fwfManagerObject.fwf_ToJson(chunk))
                    # Pass JSON Object for further execution
                    self.__processChunkData(appLevel.inputJsonData)
                    chunk_counter = chunk_counter + 1
            # After all chunks are processes process file level tasks
            self.__processFileLevelTasks()
            return "SUCCESS"
        except Exception as e:
            logger.error("[REF#1112] Exception: process_inputfile_in_chunks {0}.\n {1}\n".format(e, e.args))
            logger.exception(e)
            shutil.move(appLevel.appConfig['InputFileLocation'] + "\\" + self.input_filename, appLevel.appConfig['ErrorFileLocation'] + "\\" + self.input_filename)
            return "ERROR"
       

    # After all chunks are processes process file level tasks
    def __processFileLevelTasks(self):
         # Update 'FileRequestStatus' in mongoDB 'IX_IMPORT_REQUEST' collection
        if(self.importRequestId != ""):
            logger.info("Updating file request to Completed")
            self.mongoObj.update_IX_Import_FileRequest(self.importRequestId, self.input_fileId, "Completed")
            shutil.move(appLevel.appConfig['InputFileLocation'] + "\\" + self.input_filename, appLevel.appConfig['ArchiveFileLocation'] + "\\" + self.input_filename)

        ## Insert in Final output in mongoDB 'IX_PY_RESULT' collection
        #if(appLevel.appConfig['DEFAULT']['UPDATE_FINALOUTPUT_IN_DB'] == "TRUE"):
        #    logger.info("Initiating IX_PY_RESULT writing...")
        #    objFileMapperIXInserter = mongoDbFileMapping()
        #    objFileMapperIXInserter.insert_Final_Output()
        #    logger.info("IX_PY_RESULT written.")
        #else:
        #    logger.info("IX_PY_RESULT database update defferred.")

        ###########################################################
        logger.info("IX - " + appLevel.currentDateTimeStampString + " operation completed.")
        ###########################################################
