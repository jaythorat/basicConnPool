import json
from datetime import datetime
from bson.objectid import ObjectId


class jsonDatetimeParser(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)