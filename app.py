from mysqlPool import MysqlConnectionPool
from mongoPool import MongoConnectionPool
import time
from parseEnviron import ParseEnviron as Pe
import json

from mongoCalls import MongodbModel
sqlConnPool = MysqlConnectionPool()
# mongoPool = MongoConnectionPool()


class WsgiApp:

    def __init__(self):
        pass

    def processRequestData(self, environ):
        pe = Pe(environ)
        url = pe.getUrl()
        return url
        requestHeaders = pe.getRequestHeaders()
        # print(requestHeaders)
        if "CONTENT_TYPE" in requestHeaders:
            # print("blalblablalbabl")
            postData = pe.getPostData()
            # print("1:2 ",postData)
            # print("1:444 ",pe.getPostData())
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

def __convertDatesToStrings__(data):
        import datetime

        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, datetime.datetime):
                    # Modify only the date part, keep time intact
                    data[key] = value.strftime("%d-%m-%Y %H:%M:%S")
                elif isinstance(value, datetime.date):
                    # For date objects, no time part
                    data[key] = value.strftime("%d-%m-%Y")
                elif isinstance(value, dict):
                    data[key] = __convertDatesToStrings__(
                        value
                    )  # Handle nested dicts
                elif isinstance(value, list):
                    data[key] = [
                        (
                            __convertDatesToStrings__(item)
                            if isinstance(item, (dict, list))
                            else item
                        )
                        for item in value
                    ]
        elif isinstance(data, list):
            return [
                (
                    __convertDatesToStrings__(item)
                    if isinstance(item, (dict, list))
                    else item
                )
                for item in data
            ]
        return data

def getDataFromSql(procName,procParams):
    response = sqlConnPool.getDataFromSqlProcedure(procName,procParams)
    if response:
        response = [{k: v for k, v in i.items() if k not in ('createdAt', 'updatedAt')} for i in response]
    return response   

def commitDataToSql(proCName, procParams): 
    response = sqlConnPool.commitDataToSql(proCName, procParams)
    if response:
        return response 
    else: return []

# def getDataFromMongo(procName,procParams):
#     # mongoPool.getMongodb()
#     if procName == "getAllActiveData":
#         return mongoPool.getMongodb(procParams[0],procParams[1])
#     elif procName == "getAllData":
#         return mongoPool.getMongodb(procParams[0])
#     elif procName == "getDataById":
#         return mongoPool.getMongodb(procParams[0],procParams[1],procParams[2],procParams[3])
#     elif procName == "getAllData":
#         return mongoPool.getMongodb(procParams[0])
#     elif procName == "getActiveDataById":
#         return mongoPool.getMongodb(procParams[0],procParams[1],procParams[2],procParams[3])
#     elif procName == "checkEntityExists":
#         return mongoPool.getMongodb(procParams[0],procParams[1],procParams[2],procParams[3])
#     return None
        

def app(environ, start_response):
    
    start = time.time()
    resp = {
        "responseCode": 200,
        "responseHeaders": {}
    }
    wsgiapp = WsgiApp()
    url = wsgiapp.processRequestData(environ)
    # url = "mysql"
    postData = json.loads(environ["wsgi.input"].read().decode("UTF-8"))
    # print("jay is hererererer : ",postData,url)
    if 'mysqlget' in url:
        resp["responseBody"] = json.dumps(__convertDatesToStrings__(getDataFromSql(postData["procName"],postData["procParams"])))
    elif 'mysqlpost' in url:
        resp["responseBody"] = json.dumps(__convertDatesToStrings__(commitDataToSql(postData["procName"],postData["procParams"])))
    # elif 'mongo' in url:`
    #     resp["responseBody"] = json.dumps(__convertDatesToStrings__(getDataFromMongo(postData["procName"],postData["procParams"])))
    else:
        resp['responseBody'] = []
    # print(resp["responseBody"])
    statusCode, responseHeaderList, encodeResponse = wsgiapp.processResponse(resp)
    start_response(statusCode, responseHeaderList)
    # print('time yaken: ', time.time() - start)
    return [encodeResponse]
