# MongoDB Exploration
# Jack Hobbs

# libraries and Distributions
import os
import pandas as pd
import multiprocessing as mp
from pprint import pprint
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

# user configuration for data to pull from Mongo
# list is looped through, each dict a separate database
extract_database_list = [{'database name': 'Newspaper',
                          'index field': 'lccn',
                          'collections': [{'collection name': 'newspaper_index',
                                           'extraction drop fields': [{'_id': 0, 'lccn': 1, 'state': 1,
                                                                       'title': 1}]},
                                          {'collection name': 'newspaper_details',
                                           'extraction drop fields': [{'_id': 0, 'lccn': 1, 'name': 1,
                                                                       'publisher': 1,
                                                                       'start_year': 1, 'end_year': 1},
                                                                      {'_id': 0, 'lccn': 1,
                                                                       'issues.date_issued': 1}
                                                                      ]}]}]

# multiprocessing info
mp_max = mp.cpu_count()     # for the discerning connoisseur of computational cores


def main(database_list: List):
    """
    executes insertion of data to MongoDB
    :param database_list: list of dicts containing database and collection parameters
    """
    # validate user inputs
    MongoDBTools.pull_input_type_governor(database_list=database_list)

    # establishing the Mongo client
    pymongo_client_connection = MongoDBTools.pymongo_client(link=uri)

    for database_dict in database_list:
        # initialize database and collections from details in dictionary
        active_database = MongoDBTools.initiate_database(
            client=pymongo_client_connection,
            database_str=database_dict['database name'])

        # storage for collection level pandas-ready dictionaries
        collection_dataframe_dicts = {}

        for collection in database_dict['collections']:
            active_collection = MongoDBTools.initiate_collection(
                database_obj=active_database,
                collection=collection['collection name'])

            # storage for query level pandas-ready dictionaries
            query_dataframe_dicts = {}
            query_index = 0

            for query in collection['extraction drop fields']:
                # list of column names based on filtering
                # split on period to handle nested queries (i.e {Array.Field: 1})
                query_cols = [col.split('.')[0] for col in list(query.keys())]

                # dict to append items to and pass to Pandas; set items in query dictionary, excluding _id
                query_dict = {}
                for col in query_cols:
                    if col != '_id':
                        query_dict.update({col: []})

                collection_objects = MongoDBTools.grab_collection_fields(
                    collection=active_collection,
                    filter_bool=False,
                    push_bool=False,
                    object_key=query)

                # query and collection objects inherit keys from the same source allowing keys to be interchangeable
                for doc in collection_objects:
                    for item in list(doc.keys()):
                        query_dict[item].append(doc[item])

                ''' CUSTOM TRANSFORMATIONS FOR COLLECTIONS'''
                # transform year strings into integers
                for field in ['start_year', 'end_year']:
                    if field in list(query_dict.keys()):
                        query_dict[field] = MongoDBTools.year_to_int(year_list=query_dict[field])

                # extract issue dates from issues list and create new dict with individual flat record for each date
                # I am aware this would be better held in a table, but I wanted to do some transformations with Pandas
                if 'issues' in query_cols:
                    query_dict = MongoDBTools.break_out_daily(collection_obj_list=collection_objects,
                                                              obj_record_key='lccn',
                                                              obj_list_key='issues',
                                                              value_list_key='date_issued')

                active_query_df = pd.DataFrame.from_dict(query_dict)

                # testing pandas lookup functionality by merging state data into daily records
                if 'issues' in query_cols:
                    lookup_df = pd.DataFrame.from_dict(
                        data=collection_dataframe_dicts['newspaper_index']['newspaper_index-0'])
                    active_query_df = active_query_df.merge(right=lookup_df[['lccn', 'state']], how='left', on='lccn')
                    query_dict = active_query_df.to_dict()

                # store dataframe dict at collection level
                # I'd consider something like Pickle if I HAD to store dataframes (rather than dicts or in a database)
                query_dataframe_dicts.update({f'{collection["collection name"]}-{query_index}': query_dict})
                query_index += 1

            # store collection dataframes at database level
            collection_dataframe_dicts.update({f'{collection["collection name"]}': query_dataframe_dicts})

        return collection_dataframe_dicts


if __name__ == "__main__":
    main(extract_database_list)
