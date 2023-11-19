import pandas

# $ pip install pandas
# Sample CSV
# Name,Hire Date,Salary,Sick Days remaining
# Graham Chapman,03/15/14,50000.00,10
# John Cleese,06/01/15,65000.00,8
# Eric Idle,05/12/14,45000.00,10
# Terry Jones,11/01/13,70000.00,3
# Terry Gilliam,08/12/14,48000.00,7
# Michael Palin,05/23/13,66000.00,8

class fwf_manager(object):

    # Class level attribute
    codername = "Manish"

    # Constructor
    def __init__(self):
        self.generateSampleFwfFile = True

    def get_coder_name(self):
        return self.codername


    #Convert FWF to JSON and writes Json file
    def fwf_ToJson_ToFile(self, inputFileName, destJsonFileName, fwidths, colNames):
        #fwidths = [11,11,11,11,11,11]
        df = pandas.read_fwf(inputFileName, 
                        widths = fwidths, # [11,11,11,11,11,11]
                        names = colNames, #['col0', 'col1', 'col2', 'col3', 'col4', 'col5']
                        converters={i: str for i in range(0, len(colNames))})  

        df.to_json(destJsonFileName, orient = "index", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)


    #Convert FWF to JSON and writes Json file
    def fwf_ToJson_Return(self, inputFileName, fwidths, colNames):
        #fwidths = [11,11,11,11,11,11]
        df = pandas.read_fwf(inputFileName, 
                        widths = fwidths, # [11,11,11,11,11,11]
                        names = colNames, #['col0', 'col1', 'col2', 'col3', 'col4', 'col5']
                        converters={i: str for i in range(0, len(colNames))})  

        return df.to_json(orient = "index", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)



    #Convert FWF to JSON  and return Json string
    def fwf_ToJson(self, df):
        return df.to_json(orient = "index", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)

    