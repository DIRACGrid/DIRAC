"""
This is used to test the ElasticSearchDB module. It is used to discover all possible changes of Elasticsearch api.
If you modify the test data, you have to update the test cases...
"""

import time

import pytest
from DIRAC import gConfig, gLogger
from DIRAC.Core.Utilities.ElasticSearchDB import ElasticSearchDB

# Useful methods


def setupDB():
    """Get configuration from a cfg file and instantiate a NoSQLDB"""
    gLogger.setLevel("DEBUG")

    result = gConfig.getOption("/Systems/NoSQLDatabases/Host")
    if not result["OK"]:
        result["Value"] = "localhost"
    host = result["Value"]

    result = gConfig.getOption("/Systems/NoSQLDatabases/Port")
    if not result["OK"]:
        result["Value"] = 9200
    port = int(result["Value"])

    result = gConfig.getOption("/Systems/NoSQLDatabases/User")
    if not result["OK"]:
        result["Value"] = "elastic"
    user = result["Value"]

    result = gConfig.getOption("/Systems/NoSQLDatabases/Password")
    if not result["OK"]:
        result["Value"] = "changeme"
    password = result["Value"]

    return getDB(host, user, password, port)


def getDB(host, user, password, port):
    """Return an ElasticSearchDB object"""
    return ElasticSearchDB(host, port, user, password, useSSL=False)


# Data


elasticSearchDB = setupDB()


data = [
    {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:00:00.0"},
    {"Color": "red", "quantity": 1, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"},
    {"Color": "red", "quantity": 1, "Product": "b", "timestamp": "2015-02-09 16:30:00.0"},
    {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:00:00.0"},
    {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:15:00.0"},
    {"Color": "red", "quantity": 2, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"},
    {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:15:00.0"},
    {"Color": "red", "quantity": 2, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"},
    {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:15:00.0"},
    {"Color": "red", "quantity": 2, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"},
]


moreData = [
    {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:00:00.0"},
    {"Color": "red", "quantity": 1, "Product": "b", "timestamp": "2015-02-09 09:15:00.0"},
    {"Color": "red", "quantity": 1, "Product": "c", "timestamp": "2015-02-09 09:30:00.0"},
    {"Color": "red", "quantity": 1, "Product": "d", "timestamp": "2015-02-09 10:00:00.0"},
    {"Color": "red", "quantity": 1, "Product": "e", "timestamp": "2015-02-09 10:15:00.0"},
    {"Color": "red", "quantity": 1, "Product": "f", "timestamp": "2015-02-09 10:30:00.0"},
    {"Color": "red", "quantity": 1, "Product": "g", "timestamp": "2015-02-09 10:45:00.0"},
    {"Color": "red", "quantity": 1, "Product": "h", "timestamp": "2015-02-09 11:00:00.0"},
    {"Color": "red", "quantity": 1, "Product": "i", "timestamp": "2015-02-09 11:15:00.0"},
    {"Color": "red", "quantity": 1, "Product": "l", "timestamp": "2015-02-09 11:30:00.0"},
]


# Index tests


def test_bulkindex():
    """bulk_index test"""
    result = elasticSearchDB.bulk_index("integrationtest", data)
    assert result["OK"]
    assert result["Value"] == 10
    time.sleep(5)
    indexes = elasticSearchDB.getIndexes()
    assert type(indexes) == list
    for index in indexes:
        res = elasticSearchDB.deleteIndex(index)
        assert res["OK"]


def test_bulkindexMonthly():
    """bulk_index test (month)"""
    result = elasticSearchDB.bulk_index(indexPrefix="integrationtestmontly", data=data, period="month")
    assert result["OK"]
    assert result["Value"] == 10
    time.sleep(5)
    indexes = elasticSearchDB.getIndexes()
    assert type(indexes) == list
    for index in indexes:
        res = elasticSearchDB.deleteIndex(index)
        assert res["OK"]


def test_index():
    """create index test"""
    result = elasticSearchDB.createIndex("integrationtest", {})
    assert result["OK"]
    index_name = result["Value"]

    for i in data:
        result = elasticSearchDB.index(index_name, i)
        assert result["OK"]


def test_wrongdataindex():
    """create index test (wrong insertion)"""
    result = elasticSearchDB.createIndex("dsh63tsdgad", {})
    assert result["OK"]
    index_name = result["Value"]
    result = elasticSearchDB.index(
        index_name, {"Color": "red", "quantity": 1, "Product": "a", "timestamp": 1458226213000}
    )
    assert result["OK"]
    result = elasticSearchDB.index(
        index_name, {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09T16:15:00Z"}
    )
    assert not (result["OK"])
    assert result["Message"]
    result = elasticSearchDB.deleteIndex(index_name)
    assert result["OK"]


# Deletion tests


def test_deleteNonExistingIndex():
    """delete non-existing index"""
    result = elasticSearchDB.deleteIndex("dsdssuu")
    assert result["OK"]


# Various tests chained


def setUp():
    result = elasticSearchDB.generateFullIndexName("integrationtest", "day")
    assert len(result) > len("integrationtest")
    index_name = result

    result = elasticSearchDB.index(
        index_name, {"Color": "red", "quantity": 1, "Product": "a", "timestamp": 1458226213000}
    )
    assert result["OK"]
    return index_name


def tearDown(index_name):
    elasticSearchDB.deleteIndex(index_name)


def test_getIndexes():
    """test fail if no indexes are present"""
    index_name = setUp()
    elasticSearchDB.deleteIndex(index_name)
    result = elasticSearchDB.getIndexes()
    assert not (result)  # it will be empty at this point
    tearDown(index_name)


def test_getDocTypes():
    """test get document types"""
    index_name = setUp()
    result = elasticSearchDB.getDocTypes(index_name)
    assert result
    if "_doc" in result["Value"]:
        assert set(result["Value"]["_doc"]["properties"]) == {"Color", "timestamp", "Product", "quantity"}
    else:
        assert set(result["Value"]["properties"]) == {"Color", "timestamp", "Product", "quantity"}
    tearDown(index_name)


def test_existingIndex():
    index_name = setUp()
    result = elasticSearchDB.existingIndex(index_name)
    assert result["OK"] and result["Value"]
    tearDown(index_name)


def test_generateFullIndexName():
    """Whatever we give as input, the full index should start with the prefix"""
    indexName = "test"
    for period in ["day", "week", "month", "year", "null", "notExpected"]:
        assert elasticSearchDB.generateFullIndexName(indexName, period).startswith(indexName)


def test_getUniqueValue():
    index_name = setUp()
    result = elasticSearchDB.getUniqueValue(index_name, "quantity")
    assert result["OK"]
    assert result["OK"]

    # this, and the next (Product) are not run because (possibly only for ES 6+):
    # # 'Fielddata is disabled on text fields by default.
    # # Set fielddata=true on [Color] in order to load fielddata in memory by uninverting the inverted index.
    # # Note that this can however use significant memory. Alternatively use a keyword field instead.'

    # result = elasticSearchDB.getUniqueValue(index_name, 'Color', )
    # assert (result['OK'])
    # assertEqual(result['Value'], [])
    # result = elasticSearchDB.getUniqueValue(index_name, 'Product')
    # assert (result['OK'])
    # assertEqual(result['Value'], [])


def test_querySimple():
    """simple query test"""
    index_name = setUp()
    elasticSearchDB.deleteIndex(index_name)
    # inserting 10 entries
    for i in moreData:
        result = elasticSearchDB.index(index_name, i)
        assert result["OK"]
    time.sleep(10)  # giving ES some time for indexing

    # this query returns everything, so we are expecting 10 hits
    body = {"query": {"match_all": {}}}
    result = elasticSearchDB.query(index_name, body)
    assert result["OK"]
    assert isinstance(result["Value"], dict)
    assert len(result["Value"]["hits"]["hits"]) == 10

    # this query returns nothing
    body = {"query": {"match_none": {}}}
    result = elasticSearchDB.query(index_name, body)
    assert result["OK"]
    assert isinstance(result["Value"], dict)
    assert result["Value"]["hits"]["hits"] == []

    # this is a wrong query
    body = {"pippo": {"bool": {"must": [], "filter": []}}}
    result = elasticSearchDB.query(index_name, body)
    assert not (result["OK"])

    # this query should also return everything
    body = {"query": {"bool": {"must": [], "filter": []}}}
    result = elasticSearchDB.query(index_name, body)
    assert result["OK"]
    assert isinstance(result["Value"], dict)
    assert len(result["Value"]["hits"]["hits"]) == 10
    tearDown(index_name)


# def test_query():
#   body = {"size": 0,
#           {"query": {"query_string": {"query": "*"}},
#            "filter": {"bool":
#                       {"must": [{"range":
#                                  {"timestamp":
#                                   {"gte": 1423399451544,
#                                    "lte": 1423631917911
#                                   }
#                                  }
#                                 }],
#                        "must_not": []
#                       }
#                      }
#           }
#                    },
#           "aggs": {
#               "3": {
#                   "date_histogram": {
#                       "field": "timestamp",
#                       "interval": "3600000ms",
#                       "min_doc_count": 1,
#                       "extended_bounds": {
#                           "min": 1423399451544,
#                           "max": 1423631917911
#                       }
#                   },
#                   "aggs": {
#                       "4": {
#                           "terms": {
#                               "field": "Product",
#                               "size": 0,
#                               "order": {
#                                   "1": "desc"
#                               }
#                           },
#                           "aggs": {
#                               "1": {
#                                   "sum": {
#                                       "field": "quantity"
#                                   }
#                               }
#                           }
#                       }
#                   }
#               }
#           }
#          }
#   result = elasticSearchDB.query(index_name, body)
#   assertEqual(result['aggregations'],
#                    {u'3': {u'buckets': [{u'4': {u'buckets': [{u'1': {u'value': 5.0},
#                                                               u'key': u'a',
#                                                               u'doc_count': 5}],
#                                                 u'sum_other_doc_count': 0,
#                                                 u'doc_count_error_upper_bound': 0},
#                                          u'key': 1423468800000,
#                                          u'doc_count': 5},
#                                         {u'4': {u'buckets': [{u'1': {u'value': 8.0},
#                                                               u'key': u'b',
#                                                               u'doc_count': 5}],
#                                                 u'sum_other_doc_count': 0,
#                                                 u'doc_count_error_upper_bound': 0},
#                                          u'key': 1423494000000,
#                                          u'doc_count': 5}]}})


def test_Search():
    index_name = setUp()
    elasticSearchDB.deleteIndex(index_name)
    # inserting 10 entries
    for i in moreData:
        result = elasticSearchDB.index(index_name, i)
        assert result["OK"]
    time.sleep(10)  # giving ES some time for indexing

    s = elasticSearchDB._Search(index_name)
    result = s.execute()
    assert len(result.hits) == 10
    assert dir(result.hits[0]) == ["Color", "Product", "meta", "quantity", "timestamp"]

    q = elasticSearchDB._Q("range", timestamp={"lte": 1423501337292, "gte": 1423497057518})
    s = elasticSearchDB._Search(index_name)
    s = s.filter("bool", must=q)
    query = s.to_dict()
    assert (
        query
        == {
            "query": {
                "bool": {
                    "filter": [
                        {"bool": {"must": [{"range": {"timestamp": {"gte": 1423497057518, "lte": 1423501337292}}}]}}
                    ]
                }
            }
        },
    )
    result = s.execute()
    assert len(result.hits) == 0

    q = elasticSearchDB._Q("range", timestamp={"lte": 1423631917911, "gte": 1423399451544})
    s = elasticSearchDB._Search(index_name)
    s = s.filter("bool", must=q)
    query = s.to_dict()
    assert (
        query
        == {
            "query": {
                "bool": {
                    "filter": [
                        {"bool": {"must": [{"range": {"timestamp": {"gte": 1423399451544, "lte": 1423631917911}}}]}}
                    ]
                }
            }
        },
    )
    result = s.execute()
    assert len(result.hits) == 0
    tearDown(index_name)

    # q = [
    #     elasticSearchDB._Q(
    #         'range',
    #         timestamp={
    #             'lte': 1423631917911,
    #             'gte': 1423399451544}),
    #     elasticSearchDB._Q(
    #         'match',
    #         Product='a')]
    # s = elasticSearchDB._Search(index_name)
    # s = s.filter('bool', must=q)
    # query = s.to_dict()
    # assertEqual(query, {'query': {'bool': {'filter': [{'bool': {
    #                  'must': [{'range': {'timestamp': {'gte': 1423399451544, 'lte': 1423631917911}}},
    #                  {'match': {'Product': 'a'}}]}}]}}})
    # result = s.execute()
    # assertEqual(len(result.hits), 5)
    # assertEqual(result.hits[0].Product, 'a')
    # assertEqual(result.hits[4].Product, 'a')


# def test_A1():
#   q = [elasticSearchDB._Q('range', timestamp={'lte': 1423631917911, 'gte': 1423399451544})]
#   s = elasticSearchDB._Search(index_name)
#   s = s.filter('bool', must=q)
#   a1 = elasticSearchDB._A('terms', field='Product', size=0)
#   s.aggs.bucket('2', a1)
#   query = s.to_dict()
#   assertEqual(query, {'query': {'bool': {'filter': [{'bool': {'must': [{'range': {'timestamp': {
#                    'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}},
#                    'aggs': {'2': {'terms': {'field': 'Product', 'size': 0}}}})
#   result = s.execute()
#   assertEqual(result.aggregations['2'].buckets, [
#                    {u'key': u'a', u'doc_count': 5}, {u'key': u'b', u'doc_count': 5}])

# def test_A2():
#   q = [elasticSearchDB._Q('range', timestamp={'lte': 1423631917911, 'gte': 1423399451544})]
#   s = elasticSearchDB._Search(index_name)
#   s = s.filter('bool', must=q)
#   a1 = elasticSearchDB._A('terms', field='Product', size=0)
#   a1.metric('total_quantity', 'sum', field='quantity')
#   s.aggs.bucket('2', a1)
#   query = s.to_dict()
#   assertEqual(
#       query, {
#           'query': {
#               'bool': {
#                   'filter': [
#                       {
#                           'bool': {
#                               'must': [
#                                   {
#                                       'range': {
#                                           'timestamp': {
#                                               'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}}, 'aggs': {
#               '2': {
#                   'terms': {
#                       'field': 'Product', 'size': 0}, 'aggs': {
#                       'total_quantity': {
#                           'sum': {
#                               'field': 'quantity'}}}}}})
#   result = s.execute()
#   assertEqual(result.aggregations['2'].buckets,
#   [{u'total_quantity': {u'value': 5.0}, u'key': u'a', u'doc_count': 5}, {
#                    u'total_quantity': {u'value': 8.0}, u'key': u'b', u'doc_count': 5}])

# def test_piplineaggregation():
#   q = [elasticSearchDB._Q('range', timestamp={'lte': 1423631917911, 'gte': 1423399451544})]
#   s = elasticSearchDB._Search(index_name)
#   s = s.filter('bool', must=q)
#   a1 = elasticSearchDB._A('terms', field='Product', size=0)
#   a2 = elasticSearchDB._A('terms', field='timestamp')
#   a2.metric('total_quantity', 'sum', field='quantity')
#   a1.bucket(
#       'end_data',
#       'date_histogram',
#       field='timestamp',
#       interval='3600000ms').metric(
#       'tt',
#       a2).pipeline(
#       'avg_buckets',
#       'avg_bucket',
#       buckets_path='tt>total_quantity',
#       gap_policy='insert_zeros')
#   s.aggs.bucket('2', a1)
#   query = s.to_dict()
#   assertEqual(
#       query, {
#           'query': {
#               'bool': {
#                   'filter': [
#                       {
#                           'bool': {
#                               'must': [
#                                   {
#                                       'range': {
#                                           'timestamp': {
#                                               'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}}, 'aggs': {
#               '2': {
#                   'terms': {
#                       'field': 'Product', 'size': 0}, 'aggs': {
#                       'end_data': {
#                           'date_histogram': {
#                               'field': 'timestamp', 'interval': '3600000ms'}, 'aggs': {
#                               'tt': {
#                                   'terms': {
#                                       'field': 'timestamp'}, 'aggs': {
#                                       'total_quantity': {
#                                           'sum': {
#                                               'field': 'quantity'}}}}, 'avg_buckets': {
#                                   'avg_bucket': {
#                                       'buckets_path': 'tt>total_quantity', 'gap_policy': 'insert_zeros'}}}}}}}})
#   result = s.execute()
#   assertEqual(len(result.aggregations['2'].buckets), 2)
#   assertEqual(result.aggregations['2'].buckets[0].key, u'a')
#   assertEqual(result.aggregations['2'].buckets[1].key, u'b')
#   assertEqual(result.aggregations['2'].buckets[0]['end_data'].buckets[0].avg_buckets, {u'value': 2.5})
#   assertEqual(result.aggregations['2'].buckets[1]['end_data'].buckets[0].avg_buckets, {u'value': 4})
@pytest.fixture
def setUpAndTearDown():
    result = elasticSearchDB.createIndex("my-index", {})
    assert result["OK"]
    result = elasticSearchDB.index(
        indexName="my-index", body={"quantity": 1, "Product": "a", "timestamp": 1458226213000}, docID=1
    )
    assert result["OK"]

    yield setUpAndTearDown

    elasticSearchDB.deleteIndex("my-index")


def test_getAndExistsDoc(setUpAndTearDown):
    res = elasticSearchDB.existsDoc(index="my-index", docID="1")
    assert res
    res = elasticSearchDB.getDoc(index="my-index", docID="1")
    assert res["Value"] == {"quantity": 1, "Product": "a", "timestamp": 1458226213000}


def test_updateDoc(setUpAndTearDown):
    res = elasticSearchDB.updateDoc(index="my-index", docID="1", body={"doc": {"quantity": 2, "Product": "b"}})
    print(res)
    assert res["OK"]
    res = elasticSearchDB.getDoc(index="my-index", docID="1")
    assert res["Value"] == {"quantity": 2, "Product": "b", "timestamp": 1458226213000}


def test_deleteDoc(setUpAndTearDown):
    res = elasticSearchDB.deleteDoc(index="my-index", docID="1")
    assert res["OK"]
    res = elasticSearchDB.getDoc(index="my-index", docID="1")
    assert res["OK"]
    assert res["Value"] == {}
