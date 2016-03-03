import datetime

from flask.ext.jsontools import DynamicJSONEncoder


class DartJsonEncoder(DynamicJSONEncoder):
    def default(self, o):
        # Custom formats
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        if isinstance(o, datetime.date):
            return o.isoformat()
        if isinstance(o, set):
            return list(o)

        # Fallback
        return super(DartJsonEncoder, self).default(o)
