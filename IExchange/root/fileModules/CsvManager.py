import pandas


class CsvManager(object):

    # Class level attribute
    codername = "Manish"

    # Constructor
    def __init__(self):
        self.codername = "Manish"


    def get_coder_name(self):
        return self.codername


    # Read CSV and convert to Json and write json file
    # Writes JSON file and return Json string
    def csv_ToJson_ToFile(self, sourceCsvFileName, destJsonFileName, colNames):
        #csv_file = pandas.DataFrame(pandas.read_csv("path/to/{0}".format(sourceCsvFileName), sep = ",", header = 0, index_col = False))
        #csv_file.to_json("/path/to/new/{0}".format(destJsonFileName), orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)
        df = pandas.DataFrame(pandas.read_csv("{0}".format(sourceCsvFileName), sep = ",", 
                                                    header = 0, index_col = False, names = colNames, 
                                                    converters={i: str for i in range(0, 10000)}))
        df.to_json("{0}".format(destJsonFileName), orient = "index", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)
        #return df.to_json(orient = "index", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)
     
        
    # Read CSV from variable and convert to Json 
    # Writes JSON file and return Json string
    def df_ToJson(self, df):
        return df.to_json(orient = "index", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)
        
