class SQL:

    def __init__(self, sqlConnPool):
        self.sqlConnPool = sqlConnPool

    def getData(self, procName, procParams):
        response = self.sqlConnPool.getDataFromSqlProcedure(procName, procParams)
        if response:
            response = [
                {k: v for k, v in i.items() if k not in ("createdAt", "updatedAt")}
                for i in response
            ]
        return response

    def commitData(self, proCName, procParams):
        response = self.sqlConnPool.commitDataToSql(proCName, procParams)
        if response:
            return response
        else:
            return []
