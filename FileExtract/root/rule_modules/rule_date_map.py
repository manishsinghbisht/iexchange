import logging
from logzero import logger
from root.rule_modules import Assign
import json
import io
import root.app_level as app_level
from datetime import datetime
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
               
                #=====================================================================================

                # Agreed datetime input from database is 
                # Format : YYYYMMDD 
                # Example: 20200114
                # Format: HH:MM AM/PM 
                # Example: 12:25 AM or 01:30 PM

                # Create Combine date time : '19501225 08:00:00 AM'
                # datetime.strptime(date_time_str, '%Y%m%d %I:%M:%S').time() == (8,0)
                # datetime.strptime(date_time_str, '%Y%m%d %I:%M%S %p').date() == (1950, 12, 25)
                # or
                # Create Combine date time : '19501225 02:00 PM'
                # datetime.strptime(date_time_str, '%Y%m%d %I:%M %p').time() == (14,0)
                # datetime.strptime(date_time_str, '%Y%m%d %I:%M %p').date() == (1950, 12, 25)

                # sample date time string creation
                #min_date_str = datetime.min.date().strftime("%Y%m%d") # YYYYMMDD : 20200114
                #min_time_str = datetime.min.time().strftime("%I:%M %p") # HH:MM AM/PM : 01:30 PM
                #default_date_time_str = "{date} {time}".format(date=min_date_str, time=min_time_str)

                ##strftime() - datetime object to string
                #s1 = datetime.datetime.now().strftime("%Y/%m/%d, %H:%M:%S")

                #=====================================================================================

                 # Assign input to draftValue
                draftValue = Assign.Assign_Input_To_Draft(rule, record)

                #if(draftValue != ''):
                #    logger.info('Date map debug!')
                
                try:
                    # Check the time format
                    date_value=datetime.strptime(draftValue, '%Y%m%d').date()
                    date_format = rule['date_format']
                    if(date_format == 'm/d/yyyy'):
                        draftValue = date_value.strftime('%m/%d/%Y')
                    elif(date_format == 'MM/DD/YYYY'):
                        draftValue = date_value.strftime('%M/%D/%Y')
                    elif(date_format == 'MMDDYYYY'):
                        draftValue = date_value.strftime('%M/%D/%Y')
                        draftValue = str(draftValue)
                        draftValue = draftValue.replace('/','')
                except ValueError:
                    logger.error("Incorrect date string format. It should be 'YYYYMMDD' Ex- '20201215' Rule: {0} Record: {1}".format(self.Rule['RuleKey'], record['IX_REC_UID']))
                    draftValue = str(draftValue)

                # Assign draftValue to output        
                Assign.Assign_Draft_To_Output(rule, record, draftValue)                        
                if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
                    logger.info("Completed {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
            except Exception as e:
                logger.error("Error in {0} for {1}".format(rule['RuleKey'], record['IX_REC_UID']))
                logger.error("Exception: Could not execute rule {0}.\n{1}\n{2} \n".format(self.Rule['RuleKey'], str(e), e.args))
