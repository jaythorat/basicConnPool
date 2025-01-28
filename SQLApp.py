import json

from parseEnviron import ParseEnviron as Pe
from sql.mysqlPool import MysqlConnectionPool
from sql.sql import SQL
from utils.utils import JSONDateTimeParser

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


def getResponseDataAndStatusCode(url, resp, postData):
    if "mysqlget" in url:
        data, status = SQL(sqlConnPool).getDataFromSql(
            postData["procName"], postData["procParams"]
        )
        if status == "SUCCESS":
            respData = json.dumps(
                data,
                cls=JSONDateTimeParser,
            )
            return respData, 200
        elif status == "FAILURE":
            return json.dumps(data, cls=JSONDateTimeParser), 400
    elif "mysqlpost" in url:

        data, status = (
            SQL(sqlConnPool).commitData(postData["procName"], postData["procParams"]),
        )
        if status == "SUCCESS":
            respData = json.dumps(
                data,
                cls=JSONDateTimeParser,
            )
            return respData, 200
        elif status == "FAILURE":
            return json.dumps(data, cls=JSONDateTimeParser), 400
    else:
        return json.dumps([], cls=JSONDateTimeParser), 400


def app(environ, start_response):
    resp = {"responseCode": 200, "responseHeaders": {}}
    wsgiapp = WsgiApp()
    url, postData = wsgiapp.processRequestData(environ)
    resp["responseBody"], resp["responseCode"] = getResponseDataAndStatusCode(
        url, resp, postData
    )
    statusCode, responseHeaderList, encodeResponse = wsgiapp.processResponse(resp)
    start_response(statusCode, responseHeaderList)
    return [encodeResponse]
