# MongoDB Exploration
# Jack Hobbs

# Libraries and Distributions
import os
import pymongo
import multiprocessing as mp
from pymongo.errors import ConnectionFailure

# Function Modules
import MongoDBTools

# Environmental Params for DB access and data pathing
PMCLIENT = list(os.environ['pmclient'].split(","))  # Client to use
PMDATABASE = os.environ['pmdatabase']  # Database to query
USER = os.environ['user']  # Mongo username
PASSWORD = os.environ['pmpassword']  # Password for access to the cloud Mongo server

def main():
    # multiprocessing info
    mp_max = mp.cpu_count()     # for the discerning connoisseur of computational cores
    print("Number of available processors: ", mp.cpu_count())

    # establishing the Mongo client
    # I'm exploring options to dynamically handle active database/collection assignment
    uri = f"mongodb+srv://{USER}:{PASSWORD}@jacktestingcluster.z8hzo.mongodb.net/{PMDATABASE}?retryWrites=true&w=majority"
    pymongo_client = pymongo.MongoClient(uri)
    active_db = MongoDBTools.grab_db(pymongo_client, 'Newspaper')    # grabs or assigns the database
    main_collection = active_db.newspaper_collection  # establish connection to the main collection in the active db
    sub_collection = active_db.newspaper_content_collection   # sub  houses a huge amount of metadata on papers

    # Connectivity error handling
    try:
        # The ping command is cheap and does not require auth.
        pymongo_client.admin.command('ping')
        print('Connection Established')
    except ConnectionFailure:
        print("Server not available")
        quit()

    # grab list of newspaper json objects (which are passed as one huge value list to the one key in the dict)
    json_list = MongoDBTools.request_json("https://chroniclingamerica.loc.gov/newspapers.json")['newspapers']
    print('Initial JSONs Acquired')

    # capture newspaper metadata using url in each newspaper_json object
    url_upload_list = list()  # list to hold newspaper metadata urls
    for json_objects in json_list:
        url_upload_list.append(json_objects['url'])
    print("List of metadata JSON URLs generated")

    class ParallelRequests:
        """
        class that handles multi-process url requests to dramatically increase efficiency
        """
        def __init__(self):
            self.urls = url_upload_list
            print("ParallelRequests Initialized")

        def parallelized_request(self, processor_count: int):
            """
            makes multiprocess request for url list
            :param processor_count: number of cores to use (mp_max recommended)
            :return: list of JSON objects
            """
            print(f'Making {len(self.urls)} requests on {processor_count} cores...')
            pool = mp.Pool(processor_count)
            results = pool.map(MongoDBTools.request_json, [url for url in self.urls])
            pool.close()
            print(f'Requests processed')
            return results

    fast_request = ParallelRequests()
    metadata_json_list = fast_request.parallelized_request(mp_max)

    try:    # push collections of JSON objects to MongoDB Atlas
        MongoDBTools.post_documents(json_list, main_collection)
        MongoDBTools.post_documents(metadata_json_list, sub_collection)
        print('Success: data pushed to MongoDB Atlas')
    except:     # too broad of an except, but I'd make this more specific in a production environment
        print('Failed: check JSON schema aligns with required input -> [{<JSON},{<JSON},...]')


if __name__ == "__main__":
    main()