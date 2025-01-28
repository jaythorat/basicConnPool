def datesToStrings(data):
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
                data[key] = __convertDatesToStrings__(value)  # Handle nested dicts
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
