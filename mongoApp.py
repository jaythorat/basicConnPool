from sql.mysqlPool import MysqlConnectionPool
from mongodb.mongoPool import MongoConnectionPool
from parseEnviron import ParseEnviron as Pe
import json
from utils.utils import jsonDatetimeParser

mongoPool = MongoConnectionPool()

class WsgiApp:
    def processRequestData(self, environ):
        pe = Pe(environ)
        url = pe.getUrl()
        requestHeaders = pe.getRequestHeaders()
        if "CONTENT_TYPE" in requestHeaders:
            postData = pe.getPostData()
            fileInput = pe.getFileInput()
        else:
            postData = {}
            fileInput = {}
        return url, requestHeaders, postData, fileInput

    def processResponse(self, resp):
        statusCode = str(resp["responseCode"])
        responseHeader = resp["responseHeaders"]
        encodeResponse = str(resp["responseBody"]).encode("UTF-8")
        responseHeaderList = [(key, value) for key, value in responseHeader.items()]
        return statusCode, responseHeaderList, encodeResponse
    
def processResponseDataAndStatusCode(url, postData):
    if 'mongoget' in url:
        responseBody, status = mongoPool.getMongodb(postData["collectionName"],postData["isActive"], postData['identifierName'], postData['identifierValue'])
        responseBody = json.dumps(responseBody, cls=jsonDatetimeParser)
        responseCode = 200 if status == "SUCCESS" else 400
    elif 'mongocreate' in url:
        responseBody, status = mongoPool.createMongodb(postData["collectionName"],postData["data"])
        responseBody = json.dumps(responseBody, cls=jsonDatetimeParser)
        responseCode = 200 if status == "SUCCESS" else 400
    elif 'mongodelete' in url:
        responseBody, status = mongoPool.deleteMongodb(postData["collectionName"],postData["identifierName"], postData['identifierValue'])
        responseBody = json.dumps(responseBody, cls=jsonDatetimeParser)
        responseCode = 200 if status == "SUCCESS" else 400
    else:
        responseBody, responseCode = [], 200
    return responseCode, responseBody

def app(environ, start_response):
    resp = {
        "responseHeaders": {}
    }
    wsgiapp = WsgiApp()
    url, requestHeaders, postData, fileInput = wsgiapp.processRequestData(environ)
    resp['responseCode'], resp['responseBody'] = processResponseDataAndStatusCode(url, postData)
    statusCode, responseHeaderList, encodeResponse = wsgiapp.processResponse(resp)
    start_response(statusCode, responseHeaderList)
    return [encodeResponse]
