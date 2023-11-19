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
class ExpiredPandasCsvManager(object):

    # Class level attribute
    codername = "Manish"

    # Constructor
    def __init__(self, filename='inputDataPanda.csv'):
        self.inputFileName = filename  # Instance level attribute, accessible across instance metahod


    def get_coder_name(self):
        return self.codername

    # Read CSV
    def read_file(self):
        try:
            df = pandas.read_csv(self.inputFileName, 
            # By default, Pandas is uses zero-based integer indices in the
            # DataFrame.  To use a different column as the DataFrame index, add
            # the index_col optional parameter
            index_col='Employee', 
            # We can force pandas to read data as a date with the parse_dates
            # optional parameter
            parse_dates=['Hired'], 
            # Ignore existing column names using the header=0 optional
            # parameter
            header=0, 
            # If CSV files doesn’t have column names in the first line, we can
            # use the names optional parameter to provide a list of column
            # names.
            # We can also use this if we want to override the column names
            # provided in the first line.
            names=['Employee', 'Hired','Salary', 'Sick Days'])
            
            print('Hired data type: ')
            print(type(df['Hired'][0]))
            print(df) #print(type(df['Hire Date'][0]))
        except Exception:
            print("Could not process file")

    
    # Write CSV
    def write_file(self, outputFileName='outputDataPanda.csv'):
        try:
            df = pandas.read_csv(self.inputFileName, 
            # By default, Pandas is uses zero-based integer indices in the
            # DataFrame.  To use a different column as the DataFrame index, add
            # the index_col optional parameter
            index_col='Employee', 
            # We can force pandas to read data as a date with the parse_dates
            # optional parameter
            parse_dates=['Hired'],
            # Ignore existing column names using the header=0 optional
            # parameter
            header=0, 
            # If CSV files doesn’t have column names in the first line, we can
            # use the names optional parameter to provide a list of column
            # names.
            # We can also use this if we want to override the column names
            # provided in the first line.
            names=['Employee', 'Hired', 'Salary', 'Sick Days']) # Overriding CSV column names
            
            df.to_csv(outputFileName) #print(type(df['Hire Date'][0]))
        except Exception:
            print("Could not process file")



    # Read CSV default
    def read_file_default(self):
        try:
            df = pandas.read_csv(self.inputFileName)
            print('Hire Date data type: ')
            print(type(df['Hire Date'][0]))
            print(df) #print(type(df['Hire Date'][0]))
        except Exception:
            print("Could not process file")


    
            
    def csvToJson(self, colNames = None):
        # List of column names to use. If file contains no header row, then you should explicitly pass header=None
        if(colNames == None):
            # File contan headers
            csv_file = pandas.DataFrame(pandas.read_csv("path/to/{0}".format(self.inputFileName), sep = ",", header = 0, index_col = False))
        else:
            # File contan headers
            csv_file = pandas.DataFrame(pandas.read_csv("path/to/{0}".format(self.inputFileName), sep = ",", names = colNames, header = None, index_col = False))
        
        csv_file.to_json("/path/to/new/file.json", orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)



    def JsonToCsv(self, jsonFileName):
        json_file = pandas.read_json(jsonFileName)
        json_file.to_csv()
