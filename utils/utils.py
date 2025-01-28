import json
from datetime import datetime


class JSONDateTimeParser(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()  # Converts datetime to ISO 8601 string
        return super().default(obj)
