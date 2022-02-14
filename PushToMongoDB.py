# MongoDB Exploration
# Jack Hobbs

# libraries and Distributions
import os
import multiprocessing as mp
from pymongo.errors import OperationFailure
from typing import List

# function Modules
import MongoDBTools

# environmental Vars for DB access and data-pathing
# as far as I can tell, access token generation is not possible in the free version of Atlas
# passing user credentials as string variables is not a best practice
PMCLIENT = list(os.environ['pmclient'].split(","))  # Client to use
PMDATABASE = os.environ['pmdatabase']  # Database to query
USER = os.environ['user']  # Mongo username
PASSWORD = os.environ['pmpassword']  # Password for access to the cloud Mongo server
uri = f"mongodb+srv://{USER}:{PASSWORD}@jacktestingcluster.z8hzo.mongodb.net/{PMDATABASE}?retryWrites=true&w=majority"

# user configuration for api call and Mongo parameters
# list is looped through, each dict a separate database
mongo_database_list = [{'database name': 'Newspaper',
                        'index collection': 'newspaper_index',
                        'detail collections': [{'collection name': 'newspaper_details',
                                                'pymongo index query': {'_id': 0, 'url': 1}}],
                        'url': 'https://chroniclingamerica.loc.gov/newspapers.json',
                        'uri': uri,
                        'objects access key': 'newspapers'}]

# multiprocessing info
mp_max = mp.cpu_count()     # for the discerning connoisseur of computational cores


def main(database_list: List):
    """
    executes insertion of data to MongoDB
    :param database_list: list of dicts containing database and collection parameters
    """
    # validate user inputs
    MongoDBTools.input_type_governor(database_list)

    # establishing the Mongo client
    pymongo_client = MongoDBTools.pymongo_client(uri)

    for database_dict in database_list:
        # initialize database and collections from details in dictionary
        active_database = MongoDBTools.initiate_database(
            client=pymongo_client,
            database_str=database_dict['database name'])

        index_collection = MongoDBTools.initiate_collection(
            database_obj=active_database,
            collection=database_dict['index collection'])

        # grab url from database_dict that links to the index JSON
        index_json_dict = MongoDBTools.parallelized_request(mp_max, [database_dict['url']])
        index_json_list = [doc for doc in index_json_dict[0][database_dict['objects access key']]]

        # post all records from the index JSON to the index collection
        MongoDBTools.post_documents(documents=index_json_list, collection=index_collection)

        # loop through each detail collection and refresh required documents
        for detail_collection in database_dict['detail collections']:
            active_detail_collection = MongoDBTools.initiate_collection(
                database_obj=active_database,
                collection=detail_collection['collection name'])

            # grab list of urls to access
            url_upload_list = MongoDBTools.grab_collection_fields(
                collection=index_collection,
                filter_bool=False,
                object_key=detail_collection['pymongo index query'])

            details_json_list = MongoDBTools.parallelized_request(mp_max, url_upload_list)

            # push details documents to selected collection
            MongoDBTools.post_documents(documents=details_json_list,
                                        collection=active_detail_collection)

    # close client to prevent accidental billing
    pymongo_client.close()


if __name__ == "__main__":
    try:
        main(mongo_database_list)
    except OperationFailure:
        pymongo_client = MongoDBTools.pymongo_client(uri)
        pymongo_client.drop_database('Newspaper')
        main(mongo_database_list)
