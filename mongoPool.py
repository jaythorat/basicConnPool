import config
from pymongo import MongoClient, errors
from queue import Queue, Empty
from threading import Lock
from pymongo import MongoClient, errors
from bson.objectid import ObjectId
import sys
import time
import json

class MongoConnectionPool:
    def __init__(self):
        self.mongoHost = config.mongoHost
        self.mongoPort = config.mongoPort
        self.mongoUser = config.mongoUser
        self.mongoPassword = config.mongoPassword
        self.mongoDatabase = config.mongoDatabase
        self.pool_size= config.connectionPoolSize
        self.db, self.client = None, None
        self.pool = Queue(maxsize=self.pool_size)
        self.lock = Lock()
        self._initialize_pool()

    def _initialize_pool(self):
        """Initialize the pool with connections."""
        for _ in range(self.pool_size):
            conn = self._create_new_connection()
            self.pool.put(conn)

    def _create_new_connection(self):
        """Create a new database connection."""
        try:
            self.client = MongoClient(
                f"mongodb://{self.mongoUser}:{self.mongoPassword}@{self.mongoHost}:{self.mongoPort}/{self.mongoDatabase}"
            )
            self.db = self.client[self.mongoDatabase]
            return [self.db, self.client]
        except:
            try:
                self.client = MongoClient(
                    f"mongodb://{self.mongoUser}:{self.mongoPassword}@{self.mongoHost}:{self.mongoPort}/{self.mongoDatabase}"
                )
                self.db = self.client[self.mongoDatabase]
                return [self.db, self.client]
            except:
                self.client = None
                self.db = None
                return [self.db, self.client]

    def get_connection(self, timeout=None):
        """
        Borrow a connection from the pool.
        :param timeout: Time in seconds to wait for a connection.
        :return: A database connection.
        """
        try:
            conn = self.pool.get(timeout=timeout)
            return conn
        except Empty:
            raise Exception("No available connections in the pool.")
    
    def getMongodb(
        self, collectionName, isActive=False, identifierName=None, identifierValue=None
    ):
        # self.__mongodbLogin__()
        self.db, self.client = self.get_connection()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db[collectionName]
                filterQuery = dict()
                if identifierName and isinstance(identifierValue, list):
                    filterQuery = {identifierName: {"$in": identifierValue}}
                elif (
                    (identifierName is None) and (identifierValue is None) and isActive
                ):
                    filterQuery = {"isActive": True}
                elif (
                    (identifierName is not None)
                    and (identifierValue is not None)
                    and not isActive
                ):
                    if identifierName == "_id":
                        identifierValue = ObjectId(identifierValue)
                    filterQuery = {identifierName: identifierValue}
                elif (
                    (identifierName is not None)
                    and (identifierValue is not None)
                    and isActive
                ):
                    if identifierName == "_id":
                        identifierValue = ObjectId(identifierValue)
                    filterQuery = {identifierName: identifierValue, "isActive": True}
                if not filterQuery:
                    documents = collection.find()
                else:
                    documents = collection.find(filterQuery)
                if documents:
                    result = list(documents)
                else:
                    result = []
                st4 = time.time()
                self.__convertObjectIdsToStr__(result)
                return result
            except Exception as e:
                return None
            finally:
                self.return_connection([self.db, self.client])
        else:
            return None


    def return_connection(self, conn):
        """
        Return a connection back to the pool.
        :param conn: The connection to return.
        """
        with self.lock:
            self.pool.put(conn)

    def close_all_connections(self):
        """Close all connections in the pool."""
        while not self.pool.empty():
            conn = self.pool.get()
            conn.close()

    def __convertObjectIdsToStr__(self, data):
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, ObjectId):
                    data[key] = str(value)
                elif isinstance(value, dict) or isinstance(value, list):
                    self.__convertObjectIdsToStr__(value)
        elif isinstance(data, list):
            for index, item in enumerate(data):
                if isinstance(item, ObjectId):
                    data[index] = str(item)
                elif isinstance(item, dict) or isinstance(item, list):
                    self.__convertObjectIdsToStr__(item)
        return data
