import mysql.connector
from queue import Queue, Empty
from threading import Lock
import config


class MysqlConnectionPool:
    def __init__(self):
        self.sqlHost = config.sqlHost
        self.sqlPort = config.sqlPort
        self.sqlUser = config.sqlUser
        self.sqlPassword = config.sqlPassword
        self.sqlDatabase = config.sqlDatabase
        self.pool_size= config.connectionPoolSize
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
        return mysql.connector.connect(
                    host=self.sqlHost,
                    port=self.sqlPort,
                    user=self.sqlUser,
                    password=self.sqlPassword,
                    database=self.sqlDatabase
        )

    def get_connection(self, timeout=None):
        """
        Borrow a connection from the pool.
        :param timeout: Time in seconds to wait for a connection.
        :return: A database connection.
        """
        try:
            conn = self.pool.get(timeout=timeout)
            # Check if the connection is still valid
            if not conn.is_connected():
                conn = self._create_new_connection()
            return conn
        except Empty:
            raise Exception("No available connections in the pool.")

    def return_connection(self, conn):
        """
        Return a connection back to the pool.
        :param conn: The connection to return.
        """
        with self.lock:
            if conn.is_connected():
                self.pool.put(conn)
            else:
                # Replace with a new connection if the old one is invalid
                self.pool.put(self._create_new_connection())

    def getDataFromSqlProcedure(self, procedureCall, data, max_retries=3):
        for attempt in range(max_retries):
            connection = None
            try:
                connection = self.get_connection()
                with connection.cursor(dictionary=True) as cursor:
                    # cursor.execute("Select * from ProgramView;")
                    # results = cursor.fetchall()
                    cursor.callproc(procedureCall, data)
                    for result in cursor.stored_results():
                        rows = result.fetchall()
                
                # return results
                return rows
            # except (PoolError, OperationalError) as e:
            #     logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
            #     if attempt == max_retries - 1:
            #         self.setError(1215, f'Database operation failed after {max_retries} attempts: {str(e)}')
            #         return None
            except Exception as e:
                return None
            finally:
                if connection:
                    try:
                        # connection.close()
                        self.return_connection(connection)
                    except Exception as e:
                        print()
        print('Unable to connect to SQL Server after multiple attempts')
        return None


    def close_all_connections(self):
        """Close all connections in the pool."""
        while not self.pool.empty():
            conn = self.pool.get()
            conn.close()
