import random
from random import randint
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import logging
from logzero import logger

class mongoDbCon(object):
    """description of class"""

    # Class level attribute
    codername = "Manish"

    # Constructor
    def __init__(self, connectionstring='mongodb://localhost:27017'):
        #self.logger = logging.getLogger()
        mongoConnectionClient = appLevel.appConfig['MongoConnections']['Client'] 
        mongoConnectionDatabase = appLevel.appConfig['MongoConnections']['Database'] 
        self.connectionstring = mongoConnectionClient # Instance level attribute, accessible across instance metahod

        #To connect to a MongoDB instance with authentication enabled, specify a URI in the following format:
        # mongodb://[username:password@]host1[:port1]
        #Ex: client = MongoClient('mongodb://alice:abc123@localhost:27017')
        client = MongoClient(self.connectionstring)
        try:
            # The ismaster command is cheap and does not require auth.
            client.admin.command('ismaster')
        except ConnectionFailure:
            logger.info("ConnectionFailure: Mongo Server not available")
            logger.info("\n Exception: MongoDB error {0}.\n{1}\n".format(e, e.args))
            logger.error("\n Exception: Main error {0}.\n{1}\n".format(e, e.args))
        except Exception as e:
            logger.info("Mongo Connection Error")
            logger.info("\n Exception: MongoDB error {0}.\n{1}\n".format(e, e.args))
            logger.error("\n Exception: Main error {0}.\n{1}\n".format(e, e.args))

        # Get the sampleDB database
        self.db = client[mongoConnectionDatabase]

        # Issue the serverStatus command and print the results
        #serverStatusResult = self.db.command("serverStatus")
        #print(serverStatusResult)


    def get_coder_name(self):
        return self.codername


    def create_samples(self):
        #Step 2: Create sample data
        names = ['Kitchen','Animal','State', 'Tastey', 'Big','City','Fish', 'Pizza','Goat', 'Salty','Sandwich','Lazy', 'Fun']
        company_type = ['LLC','Inc','Company','Corporation']
        company_cuisine = ['Pizza', 'Bar Food', 'Fast Food', 'Italian', 'Mexican', 'American', 'Sushi Bar', 'Vegetarian']
        for x in xrange(1, 501):
            business = {
                'name' : names[randint(0, (len(names)-1))] + ' ' + names[randint(0, (len(names)-1))]  + ' ' + company_type[randint(0, (len(company_type)-1))],
                'rating' : randint(1, 5),
                'cuisine' : company_cuisine[randint(0, (len(company_cuisine)-1))] 
            }
            #Step 3: Insert business object directly into MongoDB via isnert_one
            result=self.db.reviews.insert_one(business)
            #Step 4: Print to the console the ObjectID of the new document
            print('Created {0} of 100 as {1}'.format(x,result.inserted_id))
        #Step 5: Tell us that you are done
        print('finished creating 100 business reviews')

    # Showcasing the insert
    def insert(self):
        names = ['Kitchen','Animal','State', 'Tastey', 'Big','City','Fish', 'Pizza','Goat', 'Salty','Sandwich','Lazy', 'Fun']
        company_type = ['LLC','Inc','Company','Corporation']
        company_cuisine = ['Pizza', 'Bar Food', 'Fast Food', 'Italian', 'Mexican', 'American', 'Sushi Bar', 'Vegetarian']
        business = {
            'name' : names[randint(0, (len(names)-1))] + ' ' + names[randint(0, (len(names)-1))]  + ' ' + company_type[randint(0, (len(company_type)-1))],
            'rating' : randint(1, 5),
            'cuisine' : company_cuisine[randint(0, (len(company_cuisine)-1))] 
        }
        #Step 3: Insert business object directly into MongoDB via isnert_one
        result = self.db.reviews.insert_one(business)
        #Step 4: Print to the console the ObjectID of the new document
        #logger.info('Created: {0}'.format(result.inserted_id))


    # Showcasing the update
    def update(self):
        originalDocument = db.reviews.find_one({'name' : 'Manish'})
        pprint(originalDocument)

        result = self.db.reviews.update_one({'_id' : originalDocument.get('_id') }, {'$inc': {'likes': 1}})
        print('Number of documents modified : ' + str(result.modified_count))

        UpdatedDocument = db.reviews.find_one({'_id':originalDocument.get('_id')})
        print('The updated document:')
        pprint(UpdatedDocument)


    # Returns a cursor over all documents that match the search criteria
    def get_5starsRatings_documents(self):
        fivestar = self.db.reviews.find({'rating': 5})
        print(fivestar)

    # Showcasing the count() method of find, count the total number of 5 ratings 
    def get_5starsRatings_count(self):
        fivestarcount = self.db.reviews.find({'rating': 5}).count()
        print(fivestarcount)


    # Use the aggregation framework to sum the occurrence of each rating across the entire data set
    def get_allRatings_aggregate(self):
        print('\nThe sum of each rating occurance across all data grouped by rating ')
        stargroup=self.db.reviews.aggregate(
        # The Aggregation Pipeline is defined as an array of different operations
        [
        # The first stage in this pipe is to group data
        { '$group':
            { '_id': "$rating",
             "count" : 
                         { '$sum' :1 }
            }
        },
        # The second stage in this pipe is to sort the data
        {"$sort":  { "_id":1}
        }
        # Close the array with the ] tag             
        ] )
        # Print the result
        for group in stargroup:
            print(group)
