import logging
from logzero import logger
import json
import io
import root.app_level as app_level
import datetime
from jsonmerge import merge, Merger
import copy

def Assign_Draft_To_Output(rule, record, draft_value, output_column_head = None, output_column = None):

    if "InputColumnType" in rule.keys():
        if rule["InputColumnType"] == "JSON":
            if output_column == None:
                output_column = rule['OutputColumn']
            if output_column_head == None:
                output_column_head = rule['OutputColumnHead']

            _draftvalue_to_extract_object(rule, record, output_column_head, output_column, draft_value)
        elif rule["InputColumnType"] == "DB" and rule["OutputColumnHead"] != "" and rule["OutputColumnHead"] != None:
            _draftvalue_to_JSON_object(rule, record, draft_value)
    else:
        logger.error("Input column type missing in rule! Assignment Error in {0}.".format(rule['RuleKey']))


def _draftvalue_to_JSON_object(rule, record, draft_value):
    try:
        #logger = logging.getLogger()
        if (rule['OutputColumnHead'] == "ProviderDemographics"):
            # Scalar Object
            record['Provider'][rule['OutputColumnHead']][rule['OutputColumn']] = draft_value
        else:
            # Array/List Type Object
            # Not an ARRAY KEY element
            output_columnHead_key = app_level.appConfig['OUTPUT_COLUMNHEAD_KEY'][rule['OutputColumnHead']]
            if(rule['OutputColumn'] != app_level.appConfig['OUTPUT_COLUMNHEAD_KEY'][rule['OutputColumnHead']]):
                for item in record['Provider'][rule['OutputColumnHead']]:
                    # Value of output array key should equal to input column head
                    if(item[output_columnHead_key] ==  record[rule['InputColumnHead']]):
                        item[rule['OutputColumn']] = draft_value
            # ARRAY KEY 
            if(rule['OutputColumn'] == app_level.appConfig['OUTPUT_COLUMNHEAD_KEY'][rule['OutputColumnHead']]):
                # Check if Key column value is empty or none
                if(draft_value == None or draft_value == ""):
                    return
                base = { rule['OutputColumnHead']: copy.deepcopy(app_level.templateJsonData['Provider'][rule['OutputColumnHead']]) }
                merger = Merger(app_level.mergeSchema)
                base[rule['OutputColumnHead']][0][rule['OutputColumn']] = draft_value
                record['Provider'] = merger.merge(record['Provider'], base)
    except Exception as e:
        logger.error("Assignment Error in {0} for file {1}".format(rule['RuleKey'], rule['FileId']))


def _draftvalue_to_extract_object(rule, record, output_column_head = None, output_column = None, draft_value = None):
    if rule["InputColumnType"] == "JSON":
        # Assignment to extract object
        record['Extract'][output_column] = draft_value
    #if rule["InputColumnType"] == "JSON" and output_column_head == "":
    #    # Assignment to extract object
    #    record['Extract'][output_column] = draft_value
    #elif rule["InputColumnType"] == "JSON" and output_column_head != "" and output_column_head != None:
    #    # Massaging rule to add on in existing JSON object
    #    # record['Provider'][rule["OutputColumnHead"]][rule['OutputColumn']] = draftValue
    #    record['Extract'][output_column_head][output_column] = draft_value


def Assign_Input_To_Draft(rule, record, input_column_head = None, input_column = None, draft_value = None):
    
    if input_column_head == None:
        input_column_head = rule['InputColumnHead']

    if input_column == None:
        input_column = rule['InputColumn']
    
    if draft_value == None:
        draft_value = rule['DefaultValue']

    if not input_column in record.keys():
        logger.error("Exception: RuleKey: {0} - Input column not found in record: {1} ".format(rule['RuleKey'], input_column))
        return

    try:
        if rule["InputColumnType"] == "JSON":
            if input_column_head == "ProviderDemographics": # for non-arrays
                draft_value = record['Provider'][input_column_head][input_column]
            else: # for arrays
                filter_match_count = -1
                for item in record['Provider'][input_column_head]:
                    if rule['Input_Filter_Column'] != '' and rule['Input_Filter_Column'] != None:
                        # Value of output array key should equal to input column head
                        if(item[rule['Input_Filter_Column']] ==  rule['Input_Filter_Value']):
                            filter_match_count += 1
                            if(int(rule['Input_Filter_Index']) ==  filter_match_count):
                                draft_value = record[input_column]
        elif rule["InputColumnType"] == "DB":
            if input_column != "" and input_column != None:
                draft_value = record[input_column]
    except Exception as e:
        logger.error("Exception: assign_input_to_draft. Rule {0}.\n{1}\n{2} \n".format(rule['RuleKey'], str(e), e.args))

    # return draft value
    return draft_value

