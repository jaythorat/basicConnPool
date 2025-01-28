import config
from pymongo import MongoClient, errors
from queue import Queue, Empty
from threading import Lock
from bson.objectid import ObjectId

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
    
    def getMongodb(self, collectionName, isActive=False, identifierName=None, identifierValue=None):
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
                return result, "SUCCESS"
            except Exception as e:
                print(f"An error occurred while fetching documents: {e}")
                return None, "FAILURE"
            finally:
                self.return_connection([self.db, self.client])
        else:
            print("Unable to reach mongodb server")
            return None, "FAILURE"

    def createMongodb(self, collectionName, data):
        self.db, self.client = self.get_connection()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db[collectionName]
                inserted_id = collection.insert_one(data).inserted_id
                self.client.close()
                inserted_id = str(inserted_id)
                return inserted_id, "SUCCESS"
            except Exception as e:
                self.client.close()
                if "duplicate" in str(e):
                    print(f"Requested entity already exists")
                    return None, "FAILURE"
                else:
                    print("Mongodb execution failed: " + str(e))
                    return None, "FAILURE"
            finally:
                self.return_connection([self.db, self.client])
        else:
            print("Unable to reach mongodb server")
            return None, "FAILURE"

    def deleteMongodb(self, collectionName, identifierName, identifierValue):
        self.db, self.client = self.get_connection()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db[collectionName]
                if identifierName == "_id":
                    identifierValue = ObjectId(identifierValue)
                filter_query = {identifierName: identifierValue}
                deletedCount = collection.delete_one(filter_query).deleted_count
                self.client.close()
                return deletedCount, "SUCCESS"
            except Exception as e:
                self.client.close()
                print("Mongodb execution failed: " + str(e))
                return None, "FAILURE"
            finally:
                self.return_connection([self.db, self.client])
        else:
            print("Unable to reach mongodb server")
            return None, "FAILURE"
        

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
