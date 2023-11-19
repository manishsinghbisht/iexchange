# -*- coding: utf-8 -*-
import json

# Make it work for Python 2+3 and with Unicode
import io


class JsonManager(object):

    # Class level attribute
    codername = "Manish"

    # Constructor
    def __init__(self, filename='data.json'):
        self.inputFileName = filename  # Instance level attribute, accessible across instance metahod


    def get_coder_name(self):
        return self.codername

     # Write Json
    def write_file(self):
        try:
            to_unicode = unicode
        except NameError:
            to_unicode = str

        # Define data
        data = {'a list': [1, 42, 3.141, 1337, 'help', u'€'],
                'a string': 'bla',
                'another dict': {'foo': 'bar',
                                 'key': 'value',
                                 'the answer': 42}}

        # Write JSON file
        with io.open('data.json', 'w', encoding='utf8') as outfile:
            str_ = json.dumps(data,
                              indent=4, sort_keys=True,
                              separators=(',', ': '), ensure_ascii=False)
            outfile.write(to_unicode(str_))


     # Read Json
    def read_file(self):
        try:
            to_unicode = unicode
        except NameError:
            to_unicode = str

        # Read JSON file
        with open('data.json') as data_file:
            data_loaded = json.load(data_file)

        #print(data == data_loaded)
    