import logging
from logzero import logger
import json
import io
import root.appLevel as appLevel
import datetime
from jsonmerge import merge, Merger
import copy
import uuid

def AssignVal(rule, record, draftValue):
    try:
        #logger = logging.getLogger()
        if (rule['OutputColumnHead'] == "ProviderDemographics" or rule['OutputColumnHead'].upper() == "STAGED"):
            # Scalar Object
            record['Provider'][rule['OutputColumnHead']][rule['OutputColumn']] = draftValue
        else:
            #if(rule['RuleKey'] == "Rule_Key_AddOns1"):
            #    logger.info("Test")
            # Array/List Type Object
            # Not an ARRAY KEY element
            output_columnHead_key = appLevel.appConfig['OUTPUT_COLUMNHEAD_KEY'][rule['OutputColumnHead']]
            if(rule['OutputColumn'] != appLevel.appConfig['OUTPUT_COLUMNHEAD_KEY'][rule['OutputColumnHead']]):
                for item in record['Provider'][rule['OutputColumnHead']]:
                    #if (rule['RuleKey'] == "Rule_Key_AddOns1" or rule['RuleKey'] == "Rule_Key_AddOns2" ):
                    #    logger.info("Test")
                        
                    if(item[output_columnHead_key] ==  record[rule['InputColumnHead']]):
                        item[rule['OutputColumn']] = draftValue
            # ARRAY KEY 
            if(rule['OutputColumn'] == appLevel.appConfig['OUTPUT_COLUMNHEAD_KEY'][rule['OutputColumnHead']]):
                # Check if Key column value is empty or none
                if(draftValue == None or draftValue == ""):
                    return
                base = { rule['OutputColumnHead']: copy.deepcopy(appLevel.templateJsonData['Provider'][rule['OutputColumnHead']]) }
                merger = Merger(appLevel.mergeSchema)
                base[rule['OutputColumnHead']][0][rule['OutputColumn']] = draftValue
                record['Provider'] = merger.merge(record['Provider'], base)
    except Exception as e:
        logger.error("Output assignment Error in {0} for file {1}".format(rule['RuleKey'], rule['FileId']))


def AssignInputVal(rule, record):
    try:
        # Assign default valude defined by default
        draftValue = rule['DefaultValue']
        if rule['InputColumn'] != "":
            if (rule['InputColumnType'].upper() == "STAGED"):
                draftValue = record['Provider']['STAGED'][rule['InputColumn']]
            else:
                if rule['InputColumn'].upper() == "SYSKEY":
                    draftValue = str(uuid.uuid4())
                    #Create a value for InputColumnHead as well, to add values in array later
                    record[rule['InputColumnHead']] = draftValue
                else:
                    draftValue = record[rule['InputColumn']]
        
        #Assign default value if draft value is empty or none
        if draftValue == "" or draftValue == '' or draftValue == None:
            draftValue = rule['DefaultValue']

        return draftValue
    except Exception as e:
        logger.error("Input assignment Error in {0} for file {1} and InputColumn: {2}. Default value will be assigned.".format(rule['RuleKey'], rule['FileId'], rule['InputColumn']))
        rule['InputColumn'] = ''
        if rule['ErrorOverrideWithDefaultValue'] == True :
            draftValue = rule['DefaultValue']
        return draftValue
           