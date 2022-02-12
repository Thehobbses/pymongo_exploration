import pymongo
import MongoDBTools
import multiprocessing as mp
from typing import List, AnyStr

restricted_char = [r'/', r'\\', r'.', r'"', '$', '*', '<', '>', ':', '|', '?']  # list of Mongo restricted characters


def push(client: pymongo.MongoClient, database: AnyStr, collection: AnyStr,
         req_url_list: List, parent: bool, req_key: AnyStr, uri: AnyStr):
    """
    push function handles the process of posting documents to a selected database collection
    :param client: pymongo client connection
    :param database: name of database
    :param collection: name of collection
    :param req_url_list: list of urls to query
    :param parent: does the url point at a parent JSON file (i.e len = 1, form ~ {<key_str>:[<many objects>])
    :param req_key: key for parent dict
    :param uri: connection link url for error handling
    :return: documents loaded into collection
    """
    # multiprocessing info
    mp_max = mp.cpu_count()     # for the discerning connoisseur of computational cores

    # grab list of json objects
    # if the object is a parent, it requires a key to grab the list of requested dictionaries
    # many LoC JSON trees are built in this way, with the parent object taking the form {<key_str>:[<many_records>]}
    if parent:
        json_list = MongoDBTools.parallelized_request(mp_max, req_url_list)[0][req_key]
    else:
        json_list = MongoDBTools.parallelized_request(mp_max, req_url_list)

    # push collections of JSON objects to MongoDB Atlas
    MongoDBTools.post_documents(documents=json_list, client=client,
                                database=database, collection=collection,
                                uri=uri)

    # drop default initialization documents
    MongoDBTools.drop_collection(client=client, database=database, collection='default')
    MongoDBTools.drop_collection_records(documents={'default': ['default']},
                                         doc_desc='initialization records',
                                         client=client, database=database,
                                         collection=collection)


def initiate_database(client: pymongo.MongoClient):
    """
    Prompts user to create new database in their client
    :param client: pymongo client connection
    :return: new database in MongoDB Atlas
    """
    input_loop = True
    while input_loop:
        database_name = MongoDBTools.get_input('New database name: ', blacklist=True,
                                               restricted=restricted_char,
                                               intgr=False, return_bool=False)  # get database name
        db = MongoDBTools.grab_db(client=client, database=database_name)
        db['default'].insert_many([{'default': ['default']}])   # initialize requires a document is passed
        # see if user wants to add more databases
        loop_again = MongoDBTools.get_input('Add another database (y/n): ', blacklist=False,
                                            restricted=['y', 'n'], intgr=False, return_bool=True)
        if not loop_again:
            input_loop = False


def initiate_collections(client: pymongo.MongoClient, database: AnyStr):
    """
    Prompts user to create new collection in their database
    :param client: pymongo client connection
    :param database: Mongo database name
    :return: new collection in database
    """
    input_loop = True
    while input_loop:
        collection_name = MongoDBTools.get_input('New collection name: ', blacklist=True,
                                                 restricted=restricted_char, intgr=False,
                                                 return_bool=False)  # get collection name
        col = MongoDBTools.grab_collection(client=client, database=database, collection=collection_name)
        col.insert_many([{'default': ['default']}])   # initialize requires a document is passed
        # see if user wants to add more collections
        loop_again = MongoDBTools.get_input('Add another collection (y/n): ', blacklist=False,
                                            restricted=['y', 'n'],
                                            intgr=False, return_bool=True)
        if not loop_again:
            input_loop = False
