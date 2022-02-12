# MongoDB Exploration
# Jack Hobbs

# libraries and Distributions
import os
from typing import AnyStr

# function Modules
import MongoDBTools
import PushFunctions

# environmental Vars for DB access and data-pathing
# as far as I can tell, access token generation is not possible in the free version of Atlas
# passing user credentials as string variables is not a best practice
PMCLIENT = list(os.environ['pmclient'].split(","))  # Client to use
PMDATABASE = os.environ['pmdatabase']  # Database to query
USER = os.environ['user']  # Mongo username
PASSWORD = os.environ['pmpassword']  # Password for access to the cloud Mongo server


def main(init_db: bool, init_collections: bool, update_index: bool, update_details: bool,
         client_database: AnyStr, index_collection: AnyStr):
    """
    main function to execute insertion and extraction of data from MongoDB
    :param init_db: generate new databases in client
    :param init_collections: generate new collections in database
    :param update_index: update data in index collection
    :param update_details: update data in details collection (user will be prompted to select collection)
    :param client_database: primary database, if one already exists
    :param index_collection: index collection name
    :return: inputs and outputs based on configuration
    """

    # establishing the Mongo client
    uri = f"mongodb+srv://{USER}:{PASSWORD}@jacktestingcluster.z8hzo.mongodb.net/{PMDATABASE}?retryWrites=true&w=majority"
    pymongo_client = MongoDBTools.pymongo_client(uri)

    # initiate new database?
    if init_db:
        PushFunctions.initiate_database(client=pymongo_client)

    # generate list of existing collections in selected database
    database_collections = MongoDBTools.grab_collection_list(client=pymongo_client, database=client_database)

    # initiate new collections?
    if init_collections:
        PushFunctions.initiate_collections(client=pymongo_client, database=client_database)
        index_collection = MongoDBTools.select_pymongo_object(
                pm_obj_list=database_collections,
                operation_desc=f'Select index collection',
                pm_obj_desc=f'Collections in {client_database}')

    if update_index:
        PushFunctions.push(
            client=pymongo_client, database=client_database,
            collection=index_collection,
            req_url_list=["https://chroniclingamerica.loc.gov/newspapers.json"],
            parent=True, req_key='newspapers', uri=uri)

    if update_details:
        # grab list of urls to access
        url_upload_list = MongoDBTools.grab_collection_object_values(
            client=pymongo_client, database=client_database,
            collection=index_collection,
            filter_bool=False, object_key={'url': 1})

        details_url_list = list()      # list of all URLs to grab details JSONs

        for url in url_upload_list:
            try:
                details_url_list.append(url['url'])    # append url values to list for requests
            except KeyError:
                print(f'KeyError occurred; passing over record: {url}')

        # push details documents to selected collection
        PushFunctions.push(
            client=pymongo_client, database=client_database,
            collection=MongoDBTools.select_pymongo_object(
                pm_obj_list=database_collections,
                operation_desc=f'Load details data into {client_database} collection',
                pm_obj_desc=f'Collection in {client_database}'),
            req_url_list=details_url_list,
            parent=False, req_key='newspapers', uri=uri)


if __name__ == "__main__":
    main(init_db=True, init_collections=True, update_index=True, update_details=True,
         client_database='Newspapers', index_collection='newspaper_index_data')
