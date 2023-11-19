import csv
import json

# Sample CSV data:
# name,department,birthday month
# John Smith,Accounting,November
# Erica Meyers,IT,March
class expiredBuiltInCsvManager(object):

    # Class level attribute
    codername = "Manish"

    # Constructor
    def __init__(self, filename='inputDataPyBuiltIn.csv'):
        self.inputFileName = filename  # Instance level attribute, accessible across instance metahod


    def get_coder_name(self):
        return self.codername

    # Read unsing Python's built in CVS library
    # Input File Name
    def read_file(self):
        try:
            with open(self.inputFileName, mode='r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                line_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        print(f'Column names are {", ".join(row)}')
                        line_count += 1
                    print(f'\t{row["name"]} works in the {row["department"]} department, and was born in {row["birthday month"]}.')
                    line_count += 1
                print(f'Processed {line_count} lines.')
        except Exception:
            print("Could not process file")


    # Write CSV using Python's built in CVS library
    def write_file(self, outputFileName = 'outputDataPyBuiltIn.csv'):
        try:
            with open(outputFileName, mode='w') as csv_file:
                fieldnames = ['emp_name', 'dept', 'birth_month']
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                writer.writeheader()
                writer.writerow({'emp_name': 'John Smith', 'dept': 'Accounting', 'birth_month': 'November'})
                writer.writerow({'emp_name': 'Erica Meyers', 'dept': 'IT', 'birth_month': 'March'})
        except Exception:
            print("Could not process file")


    def csvToJson(self):
        csvfile = open('file.csv', 'r')
        jsonfile = open('file.json', 'w')

        fieldnames = ("FirstName","LastName","IDNumber","Message")
        reader = csv.DictReader( csvfile, fieldnames)
        for row in reader:
            json.dump(row, jsonfile)
            jsonfile.write('\n')
