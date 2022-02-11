import requests
import pymongo as pm
from typing import List, Dict, AnyStr, Set

def request_json(url: AnyStr):
    """
    requests a JSON file based on url
    :param url: URL in string format
    :return: a JSON formatted object
    """
    json_data = requests.get(url).json()
    return json_data


def pymongo_local_client(host: AnyStr, port: int) -> pm.MongoClient:
    """
    Establish a client with a specified name, host, and port
    :param host: specifies the host (i.e 'localhost')
    :param port: specifies the port (i.e 27017)
    :return: a PyMongo client connection
    """
    client = pm.MongoClient(host, port)
    return client


def pymongo_client(link: AnyStr) -> pm.MongoClient:
    """
    Establish a client with a specified name, host, and port
    :param link: specifies the uri to call
    :return: a PyMongo client connection
    """
    client =  pm.MongoClient(link)
    return  client

def grab_db(client: pm.MongoClient, db_name: AnyStr) -> pm.database.Database:
    """
    Grabs a database from the specified client
    :param client: a PyMongo client connection
    :param db_name: the string name of the database you wish to return
    :return: a PyMongo database connection
    """
    db = client[db_name]
    return db


def grab_collection(database: pm.database.Database, collection_name: AnyStr) -> pm.collection:
    """
    Grabs a database from the specified client
    :param database: a PyMongo database
    :param collection_name: the string name of the database you wish to return
    :return: a PyMongo collection
    """
    collection = database[collection_name]
    return collection


def post_single_document(document: Dict, collection: pm.collection) -> List:
    """
    posts a document to a specified database
    :param collection: a PyMongo collection
    :param document: a list of document dictionaries to post to the database
    :return: a list of uploaded object ids
    """
    upload = collection.insert_many(document)
    return upload.inserted_id


def post_documents(documents: List, collection: pm.collection) -> List:
    """
    batch posts documents to a specified database
    :param collection: a PyMongo collection
    :param documents: a list of document dictionaries to post to the database
    :return: a list of uploaded object ids
    """
    batch_upload = collection.insert_many(documents)
    return batch_upload.inserted_ids