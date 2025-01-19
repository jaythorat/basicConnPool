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
        print('in mongo')
        # self.__mongodbLogin__()
        self.db, self.client = self.get_connection()
        if (self.client is not None) and (self.db is not None):
            try:
                st = time.time()
                collection = self.db[collectionName]
                st1 = time.time()
                print('getting collection: ', st1 - st)
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
                st2 = time.time()
                print('creating filter query: ', st2-st1)
                if not filterQuery:
                    documents = collection.find()
                else:
                    documents = collection.find(filterQuery)
                st3 = time.time()
                print('fetching documents: ', st3 - st2)
                print(sys.getsizeof(documents))
                if documents:
                    result = list(documents)
                else:
                    result = []
                print(sys.getsizeof(result))
                st4 = time.time()
                print('convert to list: ', st4 - st3)
                self.__convertObjectIdsToStr__(result)
                print('covert object id to str recursive call: ', time.time() - st4)
                print('result: ', sys.getsizeof(result))
                return result
            except Exception as e:
                print(f"An error occurred while fetching documents: {e}")
                return None
            finally:
                self.return_connection([self.db, self.client])
        else:
            print("Unable to reach mongodb server")
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
