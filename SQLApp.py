import json

from parseEnviron import ParseEnviron as Pe
from sql.mysqlPool import MysqlConnectionPool
from sql.sql import SQL
from utils.utils import __convertDatesToStrings__ as datesToStrings

sqlConnPool = MysqlConnectionPool()


class WsgiApp:

    def __init__(self):
        pass

    def processRequestData(self, environ):
        pe = Pe(environ)
        url = pe.getUrl()
        postData = pe.getPostData()
        return url, postData

    def processResponse(self, resp):
        statusCode = str(resp["responseCode"])
        responseHeader = resp["responseHeaders"]
        encodeResponse = str(resp["responseBody"]).encode("UTF-8")
        responseHeaderList = [(key, value) for key, value in responseHeader.items()]
        return statusCode, responseHeaderList, encodeResponse


def app(environ, start_response):
    resp = {"responseCode": 200, "responseHeaders": {}}
    wsgiapp = WsgiApp()
    url = wsgiapp.processRequestData(environ)
    postData = json.loads(environ["wsgi.input"].read().decode("UTF-8"))
    if "mysqlget" in url:
        resp["responseBody"] = json.dumps(
            datesToStrings(
                SQL(sqlConnPool).getDataFromSql(
                    postData["procName"], postData["procParams"]
                )
            )
        )
    elif "mysqlpost" in url:
        resp["responseBody"] = json.dumps(
            datesToStrings(
                SQL(sqlConnPool).commitData(
                    postData["procName"], postData["procParams"]
                )
            )
        )
    else:
        resp["responseBody"] = []
    statusCode, responseHeaderList, encodeResponse = wsgiapp.processResponse(resp)
    start_response(statusCode, responseHeaderList)
    return [encodeResponse]
