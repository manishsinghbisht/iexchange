import logging
from logzero import logger
from root.rule_modules import Assign
import json
import io
import root.app_level as app_level
import datetime
from jsonmerge import merge, Merger
import copy
import shutil
import sys

class rule_post_process_print_text(object):
    """description of class"""

    # Class level attribute
    RuleType = "PostProcess_PrintText"
    Rule = None
    logger = logging.getLogger()

 # Constructor
    def __init__(self, rule, file=None):
        if file==None:
            logger.error("After file completion rules execution missing output file name or batch count! ")
            return

        if(self.RuleType != rule['RuleType']): 
            logger.error('Rule mismatch : {0}'.format(self.Rule))
            return
        
        self.Rule = rule
        self.file = file


    def executeRule(self):
        rule = self.Rule
        file = self.file
        

        if(app_level.appConfig['DEFAULT']['SUMMARY_LOGGING_ONLY'] == "FALSE"):
            logger.info("Executing {0}".format(self.Rule['RuleKey']))
                
        if not str(rule['RuleType']).lower().startswith('postprocess'):
            logger.error("Invalid post process rule. Error in {0}".format(rule['RuleKey']))
            return

        default_value = str(rule['DefaultValue'])
        if rule['TextType'] == "plain_text":
            default_value = str(rule['DefaultValue'])
        elif rule['TextType'] == "record_count":
                with open(file) as f:
                    for i, l in enumerate(f):
                        pass
                default_value = str(i)
            

        default_value = str(rule['Prefix']) + ' ' + default_value
        if rule['TextPosition'] == 'EOF':
            with open(file, 'a') as file_object:
                file_object.write("\n")
                file_object.write(default_value)
        if rule['TextPosition'] == 'BOF':
            line = default_value
            with open(file, 'r+') as f:
                content = f.read()
                f.seek(0, 0)
                f.write(line.rstrip('\r\n') + '\n' + content)

        
                