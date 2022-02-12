import os
from time import time
import requests
import multiprocessing as mp
import pymongo as pm
from pymongo.errors import ConnectionFailure
from typing import List, Dict, AnyStr, Set


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
    start_time = time()
    print(f'Making {len(link_list)} requests on {processor_count} cores...')
    pool = mp.Pool(processor_count)
    results = pool.map(request_json, [url for url in link_list])
    pool.close()
    print(f'Requests processed in {round(time()-start_time, 2)} seconds\n')
    return results


def get_input(prompt: AnyStr, blacklist: bool, restricted: List, intgr: bool, return_bool: bool):
    """
    simplifies display of input requests, and captures user response while handling restrictions
    :param prompt: string to prompt user input
    :param blacklist: changes the character restriction type (True = blacklist, False = whitelist)
    :param restricted: list of restricted characters
    :param intgr: logic for if the user input should be returned as int or str (True = int)
    :param return_bool: if true, the first value in restricted list is considered True and the others False (y/n)
    :return: user input value
    """
    user_input = input(prompt)

    if intgr:
        user_input = int(user_input)

    if blacklist:
        while any([char in restricted for char in user_input]):
            print(f'Please do not use restricted characters: {restricted}')
            user_input = input(prompt)
            if intgr:
                user_input = int(user_input)

    else:
        while user_input not in restricted:
            print(f'Please only use the following characters: {restricted}')
            user_input = input(prompt)
            if intgr:
                user_input = int(user_input)

    if return_bool:
        if user_input == restricted[0]:
            return True
        else:
            return False

    else:
        return user_input


def select_pymongo_object(pm_obj_list: list, operation_desc: AnyStr, pm_obj_desc: AnyStr):
    """
    accepts user input to select the desired MongoDB object to read and write from
    :param pm_obj_list: list of MongoDB objects, generally from <client>.list_all_databases or <db>.list_all_collections
    :param operation_desc: description of operation
    :param pm_obj_desc: short string description of object type in pm_obj_list
    :return: string corresponding to a object in the list based on user provided index
    """
    # pull in list of objects and print their list index and string name
    index = -1
    string_list = f'\n{operation_desc}:\n'
    for obj in pm_obj_list:     # append index: object for each in list
        index += 1
        string_list += f'{index}: {obj}\n'

    # if the string is empty, print None
    if string_list == '':
        print(f'No {pm_obj_desc} found. Initiate new {pm_obj_desc} then try again.\n')

    else:
        try:
            print(string_list)
            # get user to select from list of available objects, or make new object
            select_prompt = f'Input index number from list above: '
            user_input = get_input(prompt=select_prompt, blacklist=False,
                                   restricted=[num for num in range(len(pm_obj_list))],
                                   intgr=True, return_bool=False)
            return pm_obj_list[user_input]
        except ValueError:  # probably just a little quick on the trigger and passed nothing
            select_pymongo_object(pm_obj_list, pm_obj_desc)


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


def grab_db(client: pm.MongoClient, database: AnyStr) -> pm.database.Database:
    """
    Grabs a database from the specified client
    :param client: Mongo client connection
    :param database: Mongo database string name
    :return: PyMongo database connection
    """
    db = client[database]
    return db


def grab_db_list(client: pm.MongoClient) -> List:
    """
    Lists all databases for the supplied client
    :param client: Mongo client connection
    :return: list of database names
    """
    database_list = client.list_database_names()
    return database_list


def drop_db(client: pm.MongoClient, database: AnyStr) -> pm.database.Database:
    """
    Drop a database from the specified client
    :param client: Mongo client connection
    :param database: Mongo database string name
    :return: None
    """
    client[database].drop()
    print(f'Dropped database: {database}')


def grab_collection(client: pm.MongoClient, database: AnyStr, collection: AnyStr) -> pm.collection:
    """
    Grabs a database collection from the specified client
    :param client: Mongo client connection
    :param database: Mongo database string name
    :param collection: Mongo collection string name
    :return: a PyMongo collection
    """
    db = client[database]
    col = db[collection]
    return col


def grab_collection_list(client: pm.MongoClient, database: AnyStr) -> List:
    """
    Lists all collections in a database
    :param client: Mongo client connection
    :param database: Mongo database string name
    :return: a list of collection names, filter removes system collections
    """
    filter_sys = {"name": {"$regex": r"^(?!system\.)"}}     # Drops all system objects
    collection_dict_list = client[database].list_collections(filter=filter_sys)
    collection_list = list()
    for each in collection_dict_list:
        collection_list.append(each['name'])
    return collection_list


def grab_collection_object_values(client: pm.MongoClient, database: AnyStr, collection: AnyStr,
                                  filter_bool: bool, object_key: dict) -> list:
    """
    Grabs a set of specified fields from the specified collection
    For more detail on structuring object_key: https://docs.mongodb.com/manual/reference/method/db.collection.find/
    :param client: Mongo client connection
    :param database: Mongo database string name
    :param collection: Mongo collection string name
    :param filter_bool: return selected fields for all objects (F), or objects with field values matching object_key (T)
    :param object_key: a Mongo formatted dict to return fields based on criteria
    :return: a list of values from all objects in the collection
    """
    db = client[database]
    col = db[collection]
    if filter_bool:
        result = col.find(object_key)
    else:
        result = col.find({}, object_key)
    return result


def drop_collection(client: pm.MongoClient, database: AnyStr, collection: AnyStr) -> pm.collection:
    """
    Drop a database collection from the specified client
    :param client: Mongo client connection
    :param database: Mongo database string name
    :param collection: Mongo collection string name
    :return: None
    """

    db = client[database]
    col = db[collection]
    col.drop()
    print(f'Dropped database collection: {collection}')


def drop_collection_records(documents, doc_desc: AnyStr, client: pm.MongoClient,
                            database: AnyStr, collection: AnyStr) -> pm.collection:
    """
    Drop a collection record from the specified client
    :param documents: list of documents in collection, or single dict
    :param doc_desc: description of documents dropped
    :param client: Mongo client connection
    :param database: Mongo database string name
    :param collection: Mongo collection string name
    :return: None
    """
    db = client[database]
    col = db[collection]
    col.delete_many(documents)
    print(f'Dropped {doc_desc} from collection: {collection}')


def post_documents(documents: List, client: pm.MongoClient,
                   database: AnyStr, collection: AnyStr, uri: AnyStr) -> List:
    """
    posts documents to a specified database
    :param client: Mongo client connection
    :param database: Mongo database string name
    :param collection: Mongo collection string name
    :param documents: list of document dictionaries to post to the database
    :param uri: connection link url for error handling
    :return: list of uploaded object ids
    """
    start_time = time()
    print(f'Posting {len(documents)} documents to {collection}')
    db = client[database]
    col = db[collection]
    err_loop = 0

    while True:    # sometimes the remote connection fails on large upload and needs to be reset
        try:
            batch_upload = col.insert_many(documents)

        except ConnectionResetError:
            err_loop += 1
            print(f'Connection to remote host failed... [Attempt: {err_loop}]')
            client = pymongo_client(uri)
            db = client[database]
            col = db[collection]
            batch_upload = col.insert_many(documents)

        print(f'Success: data pushed to MongoDB Atlas in {round(time() - start_time, 2)} seconds')
        return batch_upload.inserted_ids

