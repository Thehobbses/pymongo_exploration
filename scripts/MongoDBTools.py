import requests
import json
import multiprocessing as mp
import pymongo as pm
from pymongo import database
from typing import List, Dict, AnyStr


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


def grab_collection_fields(collection: pm.collection, filter_bool: bool, push_bool: bool, object_key: Dict) -> List:
    """
    Grabs a set of specified fields from a collection; result is received as a pymongo cursor
    For more detail on structuring object_key: https://docs.mongodb.com/manual/reference/method/db.collection.find/
    :param collection: Mongo collection connection
    :param filter_bool: return selected fields for all objects (F), or objects with field values matching object_key (T)
    :param push_bool: set to True if used in Push script
    :param object_key: a Mongo formatted dict to return fields based on criteria
    :return: a list of values from all objects in the collection
    """
    return_list = []

    if filter_bool:
        result = collection.find(object_key)
    else:
        result = collection.find({}, object_key)

    for record in result:
        cleaned_record = list(record.values())   # cleaning gross list nesting

        if len(cleaned_record) == 1:    # records of length 1 should be passed to return list as their sole value
            cleaned_record = cleaned_record[0]

        if push_bool:   # push requires cleaned records, pull needs dict keys
            return_list.append(cleaned_record)
        else:
            return_list.append(record)

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


def push_input_type_governor(database_list: List):
    """
    parses a push list of databases and checks that each field is passing correct data types
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


def pull_input_type_governor(database_list: List):
    """
    parses a pull list of databases and checks that each field is passing correct data types
    :param database_list: list of dicts containing database and collection parameters
    :return: pass or fail codes
    """
    index = 0   # provides context for failure points when DB list grows in length
    for database_dict in database_list:
        db_name = database_dict['database name']

        name_strings = [db_name]
        index_queries = []

        for objects in database_dict['collections']:
            dcol_name = objects['collection name']
            idx_qry = objects['extraction drop fields']
            name_strings.append(dcol_name)
            for qry in idx_qry:
                index_queries.append(qry)

        # check if name fields are strings and include no restricted chars
        for strings in name_strings:
            if type(strings) != str:
                raise ValueError(f'Name in database or collections not of type: string [Dict Index: {index}]')
            is_restricted(strings)

        # check to see if index collection queries are in valid dict form
        for queries in index_queries:
            if type(queries) != dict:
                raise ValueError(f'Query in details collection pymongo index queries not of type: dict [Dict Index: {index}]')
        index += 1


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


def year_to_int(year_list: list) -> List:
    """
    cleans year lists, converting strings to ints and handling non-numeric inputs
    0 values will be filtered out of vis and treated as censored records
    :param year_list: list of string years
    :return: list of int years
    """
    cleaned_year = []
    for year in year_list:
        try:
            cleaned_year.append(int(year))
        except ValueError:
            if year == 'current':
                cleaned_year.append(2022)
            else:
                cleaned_year.append(0)
    return cleaned_year


def break_out_daily(collection_obj_list: list, obj_record_key: AnyStr, obj_list_key: AnyStr, value_list_key: AnyStr) -> Dict:
    """
    takes daily issue data and generates a dict where each entry is a single day associated with a record
    :param collection_obj_list: list of objects from collection
    :param obj_record_key: string key to access correct record value (i.e 'lccn' for LoC Newspapers)
    :param obj_list_key: string key to access desired list in collection_obj_list
    :param value_list_key: string key to access desired value in inner list objects
    :return: flat tab dictionary of daily records
    """
    daily_dict = {obj_record_key: [], value_list_key: []}

    for record in collection_obj_list:
        for day in record[obj_list_key]:
            daily_dict[obj_record_key].append(record[obj_record_key])
            daily_dict[value_list_key].append(day[value_list_key])

    return daily_dict


def save_dataframes_to_file(dataframes_dicts: dict):
    """
    Save data frames as json object for speed, in practice this data would be stored in a queryable service or -
      saved as json and loaded if accessed frequently but with limited need for refresh
    :param dataframes_dicts: dictionary of dataframe dictionaries
    :return: JSON file of dictionaries
    """
    df_json_file = json.dumps(dataframes_dicts)
    json_file_obj = open('dataframe_dicts.json','w')
    json_file_obj.write(df_json_file)
    json_file_obj.close()
