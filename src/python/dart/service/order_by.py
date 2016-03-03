import copy
import re

from sqlalchemy import Float, Integer, desc

from sqlalchemy.sql.expression import nullslast

from dart.context.locator import injectable
from dart.model.exception import DartValidationException
from dart.model.query import OrderBy, Direction


@injectable
class OrderByService(object):

    @staticmethod
    def from_string(o_string):
        pattern = re.compile(r'\s*(\S+?)\s+((ASC)|(DESC))\s*')
        m = pattern.match(o_string)
        try:
            return OrderBy(m.group(1), m.group(2))
        except:
            raise DartValidationException('could not parse order_by: %s' % o_string)

    def apply_order_by(self, order_by, query, dao, schemas):
        dir_fn = self._dir_fn(order_by)
        if order_by.key in ['id', 'version_id', 'created', 'updated']:
            field = getattr(dao, order_by.key)
            return query.order_by(nullslast(dir_fn(field)))

        field = dao.data[order_by.key]
        path_keys = order_by.key.split('.')
        type_ = 'string'
        for schema in schemas:
            result = self._get_type(path_keys, schema)
            if result:
                type_ = result
                break
        cast = _pg_cast(type_)
        dir_fn = self._dir_fn(order_by)
        return query.order_by(nullslast(dir_fn(cast(field))))

    @staticmethod
    def _dir_fn(order_by):
        f = lambda a: a
        return f if order_by.direction == Direction.ASC else desc

    @staticmethod
    def _get_type(path_keys, schema):
        array_indexes = []
        s = schema['properties']['data']
        for i, key in enumerate(path_keys):
            if not s:
                break
            if 'object' in s['type']:
                s = s['properties'].get(key)
                continue
            if 'array' in s['type']:
                array_indexes.append(i - 1)
                s = s['items'].get('properties', {}).get(key)
                continue

        if not s:
            return None

        type_ = s['type']
        if isinstance(type_, list):
            pt_copy = copy.copy(type_)
            pt_copy.remove('null')
            type_ = pt_copy[0]

        return type_

def _pg_cast(js_type):
    if js_type == 'integer':
        return lambda v: v.cast(Integer)
    if js_type == 'number':
        return lambda v: v.cast(Float)
    return lambda v: v.astext
