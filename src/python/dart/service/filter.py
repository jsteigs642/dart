from abc import abstractmethod
import copy
import re

from sqlalchemy import Float, Integer, text, exists, select, or_
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import column
from sqlalchemy.sql.operators import is_

from dart.context.locator import injectable
from dart.model.exception import DartValidationException
from dart.model.query import Operator, Filter


@injectable
class FilterService(object):
    def __init__(self):
        self._operator_handlers = {
            Operator.EQ: OperatorEquals(),
            Operator.NE: OperatorNotEquals(),
            Operator.LT: OperatorLessThan(),
            Operator.LE: OperatorLessThanOrEquals(),
            Operator.GT: OperatorGreaterThan(),
            Operator.GE: OperatorGreaterThanOrEquals(),
            Operator.IN: OperatorIn(),
            Operator.NOT_LIKE: OperatorNotLike(),
            Operator.LIKE: OperatorLike(),
            Operator.SEARCH: OperatorSearch(),
        }

    def from_string(self, f_string):
        pattern = re.compile(r'\s*(\S+?)\s*(' + '|'.join(self._operator_handlers.keys()) + ')\s*(\S+)\s*')
        m = pattern.match(f_string)
        try:
            return Filter(m.group(1), m.group(2), m.group(3))
        except:
            raise DartValidationException('could not parse filter: %s' % f_string)

    def apply_filter(self, f, query, dao, schemas):
        """ :type f: dart.model.query.Filter """
        op = self._operator_handlers[f.operator]
        if f.key in ['id', 'created', 'updated']:
            return query.filter(op.evaluate(lambda v: v, getattr(dao, f.key), str, f.value))

        # at this point, assume we are dealing with a data/JSONB filter
        path_keys = f.key.split('.')
        filters = []
        visited = {}
        for schema in schemas:
            type_, array_indexes = self._get_type(path_keys, schema)
            identifier = type_ + '@' + str(array_indexes)
            if identifier in visited:
                continue
            visited[identifier] = 1
            key_groups = self.get_key_groups(array_indexes, path_keys)
            last_is_array = array_indexes[-1] == len(path_keys) - 1 if len(array_indexes) > 0 else False
            filters.append(self.expr(0, 'data', dao.data, key_groups, type_, f.value, op, last_is_array))
        return query.filter(filters[0]) if len(filters) == 1 else query.filter(or_(*filters))

    def expr(self, i, alias, col, key_groups, t, v, op, last_is_array):
        if i < len(key_groups) - 1:
            subq, c = self.get_subquery(alias, i, key_groups)
            subq = subq.where(self.expr(i + 1, 'dart_a_%s.value' % i, c, key_groups, t, v, op, last_is_array))
            return exists(subq)
        if last_is_array:
            subq, c = self.get_subquery(alias, i, key_groups, True)
            subq = subq.where(op.evaluate(lambda x: x, c, _python_cast(t), v))
            return exists(subq)
        return op.evaluate(_pg_cast(t), col[key_groups[i]], _python_cast(t), v)

    @staticmethod
    def get_subquery(alias, i, key_groups, as_text=False):
        c = column('value', JSONB)
        bindvars = {'dart_var_%s' % i: '{' + ','.join(key_groups[i]) + '}'}
        suffix = '_text' if as_text else ''
        from_expr = text('jsonb_array_elements%s(%s #> :dart_var_%s) as dart_a_%s' % (suffix, alias, i, i))
        from_expr = from_expr.bindparams(**bindvars)
        subq = select([c]).select_from(from_expr)
        return subq, c

    @staticmethod
    def get_key_groups(array_indexes, path_keys):
        if len(array_indexes) <= 0 or len(path_keys) == 1:
            return [path_keys]
        array_groups = []
        prev = 0
        for i in array_indexes:
            array_groups.append(path_keys[prev:i + 1])
            prev = i + 1
        array_groups.append(path_keys[prev:])
        return array_groups

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
            return 'string', array_indexes

        type_ = s['type']
        if isinstance(type_, list):
            pt_copy = copy.copy(type_)
            pt_copy.remove('null')
            type_ = pt_copy[0]

        if type_ == 'array':
            array_indexes.append(len(path_keys) - 1)

        return type_, array_indexes


class OperatorEvaluator(object):
    @abstractmethod
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        raise NotImplementedError


class OperatorEquals(OperatorEvaluator):
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        return lhs_cast(lhs) == rhs_cast(rhs)


class OperatorNotEquals(OperatorEvaluator):
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        return or_(lhs_cast(lhs) != rhs_cast(rhs), is_(lhs_cast(lhs), None))


class OperatorLessThan(OperatorEvaluator):
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        return lhs_cast(lhs) < rhs_cast(rhs)


class OperatorLessThanOrEquals(OperatorEvaluator):
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        return lhs_cast(lhs) <= rhs_cast(rhs)


class OperatorGreaterThan(OperatorEvaluator):
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        return lhs_cast(lhs) > rhs_cast(rhs)


class OperatorGreaterThanOrEquals(OperatorEvaluator):
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        return lhs_cast(lhs) >= rhs_cast(rhs)


class OperatorIn(OperatorEvaluator):
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        return lhs_cast(lhs).in_([rhs_cast(v) for v in rhs.split(',')])


class OperatorNotLike(OperatorEvaluator):
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        return lhs_cast(lhs).notilike(rhs_cast(rhs))


class OperatorLike(OperatorEvaluator):
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        return lhs_cast(lhs).ilike(rhs_cast(rhs))


class OperatorSearch(OperatorEvaluator):
    def evaluate(self, lhs_cast, lhs, rhs_cast, rhs):
        only_alphanum = re.sub(r'\W+', '', rhs)
        search_string = '%' + '%'.join(only_alphanum) + '%'
        return lhs_cast(lhs).ilike(search_string)


def _pg_cast(js_type):
    if js_type == 'integer':
        return lambda v: v.cast(Integer)
    if js_type == 'number':
        return lambda v: v.cast(Float)
    return lambda v: v.astext


def _python_cast(js_type):
    if js_type == 'integer':
        return int
    if js_type == 'number':
        return float
    return str
