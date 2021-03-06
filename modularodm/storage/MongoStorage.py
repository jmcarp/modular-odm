import re
import pymongo
import logging

from ..storage import Storage
from ..storage import KeyExistsException
from ..query.queryset import BaseQuerySet#MongoQuerySet
from ..query.query import QueryGroup
from ..query.query import RawQuery

# From mongoengine.queryset.transform
COMPARISON_OPERATORS = ('ne', 'gt', 'gte', 'lt', 'lte', 'in', 'nin', 'mod',
                        'all', 'size', 'exists', 'not')
GEO_OPERATORS        = ('within_distance', 'within_spherical_distance',
                        'within_box', 'within_polygon', 'near', 'near_sphere',
                        'max_distance', 'geo_within', 'geo_within_box',
                        'geo_within_polygon', 'geo_within_center',
                        'geo_within_sphere', 'geo_intersects')
STRING_OPERATORS     = ('contains', 'icontains', 'startswith',
                        'istartswith', 'endswith', 'iendswith',
                        'exact', 'iexact')
CUSTOM_OPERATORS     = ('match',)
MATCH_OPERATORS      = (COMPARISON_OPERATORS + GEO_OPERATORS +
                        STRING_OPERATORS + CUSTOM_OPERATORS)

UPDATE_OPERATORS     = ('set', 'unset', 'inc', 'dec', 'pop', 'push',
                        'push_all', 'pull', 'pull_all', 'add_to_set',
                        'set_on_insert')

# Adapted from mongoengine.fields
def prepare_query_value(op, value):

    if op.lstrip('i') in ('startswith', 'endswith', 'contains', 'exact'):
        flags = 0
        if op.startswith('i'):
            flags = re.IGNORECASE
            op = op.lstrip('i')

        regex = r'%s'
        if op == 'startswith':
            regex = r'^%s'
        elif op == 'endswith':
            regex = r'%s$'
        elif op == 'exact':
            regex = r'^%s$'

        # escape unsafe characters which could lead to a re.error
        value = re.escape(value)
        value = re.compile(regex % value, flags)
    return value

class MongoQuerySet(BaseQuerySet):

    def __init__(self, schema, cursor):

        super(MongoQuerySet, self).__init__(schema)
        self.data = cursor

    def __getitem__(self, index):

        super(MongoQuerySet, self).__getitem__(index)
        return self.schema.load(self.data[index][self.primary])

    def __iter__(self):

        return (self.schema.load(obj[self.primary]) for obj in self.data.clone())

    def __len__(self):

        return self.data.count(with_limit_and_skip=True)

    count = __len__

    def sort(self, *keys):

        sort_key = []

        for key in keys:

            if key.startswith('-'):
                key = key.lstrip('-')
                sign = pymongo.DESCENDING
            else:
                sign = pymongo.ASCENDING

            sort_key.append((key, sign))

        self.data = self.data.sort(sort_key)
        return self

    def offset(self, n):

        self.data = self.data.skip(n)
        return self

    def limit(self, n):

        self.data = self.data.limit(n)
        return self

class MongoStorage(Storage):

    QuerySet = MongoQuerySet

    def _ensure_index(self, key):
        print 'IN ENSURE INDEX', key
        self.store.ensure_index(key)

    def __init__(self, db, collection):
        self.collection = collection
        self.store = db[self.collection]

    def find_all(self):
        return self.store.find()

    def find(self, *query):
        mongo_query = self._translate_query(*query)
        return self.store.find(mongo_query)

    def find_one(self, *query):
        mongo_query = self._translate_query(*query)
        return self.store.find_one(mongo_query)

    def get(self, schema, key):
        return self.store.find_one({schema._primary_name : key})

    def insert(self, schema, key, value):
        if schema._primary_name not in value:
            value = value.copy()
            value[schema._primary_name] = key
        self.store.insert(value)

    # todo: add mongo-style updating (allow updating multiple records at once)
    def update(self, schema, key, value):
        self.store.update(
            {schema._primary_name : key},
            value
        )

    def remove(self, *query):
        mongo_query = self._translate_query(*query)
        self.store.remove(mongo_query)

    def flush(self):
        pass

    def __repr__(self):
        return self.find_all()

    def _translate_query(self, *query):

        if len(query) > 1:
            query = QueryGroup('and', *query)
        else:
            query = query[0]

        mongo_query = {}

        if isinstance(query, RawQuery):

            attribute, operator, argument = \
                query.attribute, query.operator, query.argument

            if operator == 'eq':

                mongo_query[attribute] = argument

            elif operator in COMPARISON_OPERATORS:

                mongo_operator = '$' + operator
                mongo_query[attribute] = {mongo_operator : argument}

            elif operator in STRING_OPERATORS:

                mongo_operator = '$regex'
                mongo_regex = prepare_query_value(operator, argument)
                mongo_query[attribute] = {mongo_operator : mongo_regex}

        elif isinstance(query, QueryGroup):

            if query.operator == 'and':

                mongo_query = {}

                for node in query.nodes:
                    mongo_query.update(self._translate_query(node))

                return mongo_query

            elif query.operator == 'or':

                return {'$or' : [self._translate_query(node) for node in query.nodes]}

            else:

                raise Exception('QueryGroup operator must be <and> or <or>.')

        else:

            raise Exception('Query must be a QueryGroup or Query object.')

        return mongo_query