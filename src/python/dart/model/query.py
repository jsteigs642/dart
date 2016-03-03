from dart.model.base import dictable, BaseModel


class Operator(object):
    NE = '!='
    LE = '<='
    GE = '>='
    LT = '<'
    GT = '>'
    EQ = '='
    IN = 'IN'
    NOT_LIKE = 'NOT_LIKE'
    LIKE = 'LIKE'
    SEARCH = '~'


@dictable
class Filter(BaseModel):
    def __init__(self, key, operator, value):
        """
        :type key: str
        :type operator: str
        :type value: str
        """
        self.key = key
        self.operator = operator
        self.value = value


class Direction(object):
    ASC = 'ASC'
    DESC = 'DESC'


@dictable
class OrderBy(BaseModel):
    def __init__(self, key, direction):
        """
        :type key: str
        :type direction: str
        """
        self.key = key
        self.direction = direction
