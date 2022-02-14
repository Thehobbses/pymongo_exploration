import os
from time import time
import requests
import multiprocessing as mp
import pymongo as pm
from pymongo import database
from pymongo.errors import ConnectionFailure
from typing import List, Dict, AnyStr, Set


def pymongo_local_client(host: AnyStr, port: int) -> pm.MongoClient:
    """
    Establish a client with a specified name, host, and port
    :param host: specifies the host (i.e 'localhost')
    :param port: specifies the port (i.e 27017)
    :return: Mongo client connection
    """
    client = pm.MongoClient(host, port)
    return client


def pymongo_client(link: AnyStr) -> pm.MongoClient:
    """
    Establish a client with a specified name, host, and port
    :param link: specifies the uri to call
    :return: Mongo client connection
    """
    client = pm.MongoClient(link)

    # The ping command is cheap and does not require auth.
    client.admin.command('ping')
    print('Connection Established')
    return client


def initiate_database(client: pm.MongoClient, database_str: AnyStr) -> pm.database:
    """
    Grabs a database from the specified client
    :param client: Mongo client connection
    :param database_str: Mongo database string name
    :return: PyMongo database connection
    """
    database_obj = client[database_str]
    return database_obj


def initiate_collection(database_obj: pm.database, collection: AnyStr) -> pm.collection:
    """
    Grabs a database collection from the specified client
    :param database_obj: Mongo database connection
    :param collection: Mongo collection string name
    :return: a PyMongo collection
    """
    collection_obj = database_obj[collection]
    return collection_obj


def grab_collection_fields(collection: pm.collection, filter_bool: bool, object_key: Dict) -> List:
    """
    Grabs a set of specified fields from a collection; result is received as a pymongo cursor
    For more detail on structuring object_key: https://docs.mongodb.com/manual/reference/method/db.collection.find/
    :param collection: Mongo collection connection
    :param filter_bool: return selected fields for all objects (F), or objects with field values matching object_key (T)
    :param object_key: a Mongo formatted dict to return fields based on criteria
    :return: a list of values from all objects in the collection
    """
    return_list = []

    if filter_bool:
        result = collection.find(object_key)
    else:
        result = collection.find({}, object_key)

    for record in result:
        cleaned_record = unlist_object(list(record.values()))   # cleaning gross list nesting
        return_list.append(cleaned_record)  # append values to list for requests

    return return_list


def post_documents(documents: List, collection: pm.collection) -> List:
    """
    posts documents to a specified database
    :param collection: Mongo collection object
    :param documents: list of document dictionaries to post to the database
    :return: list of uploaded object ids
    """
    err_loop = 0
    while True:    # sometimes the remote connection fails on large upload and needs to be reset
        try:
            batch_upload = collection.insert_many(documents)

        except ConnectionResetError:
            err_loop += 1
            print(f'Connection to remote host failed... [Attempt: {err_loop}]')
            batch_upload = collection.insert_many(documents)

        return batch_upload.inserted_ids


def is_restricted(user_input: AnyStr):
    """
    tests if the given string contains Mongo restricted characters
    :param user_input: string value from the database dictionary
    :return: raises error if containing restricted
    """
    restricted_char = [r'/', r'\\', r'.', r'"', '$', '*', '<', '>', ':', '|', '?']  # Mongo restricted characters
    if any([char in restricted_char for char in user_input]):
        raise ValueError(f'User Input: {user_input} includes Mongo restricted characters ({restricted_char})')


def input_type_governor(database_list: List):
    """
    parses a list of databases and checks that each field is passing correct data types
    :param database_list: list of dicts containing database and collection parameters
    :return: pass or fail codes
    """
    index = 0   # provides context for failure points when DB list grows in length
    for database_dict in database_list:
        db_name = database_dict['database name']
        idx_name = database_dict['index collection']
        url_str = database_dict['url']
        uri_str = database_dict['uri']
        ojk_str = database_dict['objects access key']

        name_strings = [db_name, idx_name, ojk_str]
        link_strings = [url_str, uri_str]
        index_queries = []

        for objects in database_dict['detail collections']:
            dcol_name = objects['collection name']
            idx_qry = objects['pymongo index query']
            name_strings.append(dcol_name)
            index_queries.append(idx_qry)

        # check if name fields are strings and include no restricted chars
        for strings in name_strings:
            if type(strings) != str:
                raise ValueError(f'Name in database, object access key, index or details collection not of type: string [Dict Index: {index}]')
            is_restricted(strings)

        # check if links are strings and test primary url
        for strings in link_strings:
            if type(strings) != str:
                raise ValueError(f'Name in url, uri not of type: string [Dict Index: {index}]')
            try:
                requests.get(url_str)
            except ConnectionError:
                raise ConnectionError(f'Provided URL is not able to be reaches: {url_str} [Dict Index: {index}]')

        # check to see if index collection queries are in valid dict form
        for queries in index_queries:
            if type(queries) != dict:
                raise ValueError(f'Query in details collection pymongo index queries not of type: dict [Dict Index: {index}]')
        index += 1


def unlist_object(obj: List):
    """
    recursively removes lists around variable, used to clean up dict values
    :param obj: list or nested lists
    :return: non-list variable
    """
    if not type(obj) == list:
        raise ValueError(f'passed object must be of type List; object is of type {type(obj)}')
    while type(obj) == list:
        obj = obj[0]
    return obj


def request_json(url: AnyStr):
    """
    requests a JSON file based on url
    :param url: URL in string format
    :return: JSON formatted object
    """
    json_data = requests.get(url).json()
    return json_data


def parallelized_request(processor_count: int, link_list: List) -> List:
    """
    makes multiprocess request for url list
    :param processor_count: number of cores to use (mp_max recommended)
    :param link_list: list of url links
    :return: list of JSON objects
    """
    pool = mp.Pool(processor_count)
    results = pool.map(request_json, [url for url in link_list])
    pool.close()
    return results

