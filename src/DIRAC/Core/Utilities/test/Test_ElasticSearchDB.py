from types import SimpleNamespace
from unittest.mock import MagicMock

import DIRAC.Core.Utilities.ElasticSearchDB as elastic_module
from DIRAC.Core.Utilities.ElasticSearchDB import ElasticSearchDB


def _get_db(prefix="dirac-"):
    db = ElasticSearchDB.__new__(ElasticSearchDB)
    db._connected = True
    db.client = MagicMock()
    db.globalIndexPrefix = prefix
    return db


def _arg_or_kwarg(call, name):
    return call.kwargs.get(name, call.args[0] if call.args else None)


def test_global_prefix_normalization_and_token_handling():
    db = _get_db()
    db.globalIndexPrefix = "  LHCB- "
    assert db.globalIndexPrefix == "lhcb-"

    assert db._withGlobalPrefix("jobs,-logs,_all,lhcb-ready") == "lhcb-jobs,-lhcb-logs,lhcb-*,lhcb-ready"


def test_query_and_document_operations_use_prefixed_index_names():
    db = _get_db("prefix-")
    db.client.search.return_value = {}
    db.client.update_by_query.return_value = {}
    db.client.index.return_value = {"result": "updated"}
    db.client.get.return_value = {"_source": {"a": 1}}
    db.client.delete.return_value = {"result": "deleted"}
    db.client.exists.return_value = True

    db.query("myindex", {"query": {"match_all": {}}})
    assert db.client.search.call_args.kwargs["index"] == "prefix-myindex"

    db.update("myindex", {"script": {}}, updateByQuery=True)
    assert db.client.update_by_query.call_args.kwargs["index"] == "prefix-myindex"

    db.update("myindex", {"a": 1}, updateByQuery=False, docID="42")
    assert db.client.index.call_args.kwargs["index"] == "prefix-myindex"

    db.getDoc("myindex", "1")
    assert _arg_or_kwarg(db.client.get.call_args, "index") == "prefix-myindex"

    db.updateDoc("myindex", "1", {"doc": {"a": 2}})
    assert _arg_or_kwarg(db.client.update.call_args, "index") == "prefix-myindex"

    db.deleteDoc("myindex", "1")
    assert _arg_or_kwarg(db.client.delete.call_args, "index") == "prefix-myindex"

    db.existsDoc("myindex", "1")
    assert _arg_or_kwarg(db.client.exists.call_args, "index") == "prefix-myindex"

    db.index("myindex", {"a": 1}, docID="5")
    assert db.client.index.call_args.kwargs["index"] == "prefix-myindex"

    db.deleteByQuery("myindex", {"query": {"match_all": {}}})
    assert db.client.delete_by_query.call_args.kwargs["index"] == "prefix-myindex"


def test_index_management_and_template_operations_use_prefixed_index_names():
    db = _get_db("prefix-")
    db.client.indices.put_index_template.return_value = {"acknowledged": True}
    db.client.indices.get_alias.return_value = {"prefix-myindex": {}}
    db.client.indices.get_mapping.return_value = {
        "prefix-myindex": {"mappings": {"properties": {"a": {"type": "keyword"}}}}
    }
    db.client.indices.exists.return_value = True
    db.client.indices.create.return_value = {"acknowledged": True}
    db.client.indices.delete.return_value = {"acknowledged": True}

    db.addIndexTemplate("my-template", ["myindex-*", "prefix-already-*"], mapping={})
    body = db.client.indices.put_index_template.call_args.kwargs["body"]
    assert body["index_patterns"] == ["prefix-myindex-*", "prefix-already-*"]

    db.getIndexes("myindex")
    assert _arg_or_kwarg(db.client.indices.get_alias.call_args, "index") == "prefix-myindex*"

    db.getIndexes()
    assert _arg_or_kwarg(db.client.indices.get_alias.call_args, "index") == "prefix-*"

    db.getDocTypes("myindex")
    assert _arg_or_kwarg(db.client.indices.get_mapping.call_args, "index") == "prefix-myindex"

    db.existingIndex("myindex")
    assert _arg_or_kwarg(db.client.indices.exists.call_args, "index") == "prefix-myindex"

    db.createIndex("myindex", mapping={}, period=None)
    assert db.client.indices.create.call_args.kwargs["index"] == "prefix-myindex"

    db.deleteIndex("myindex")
    assert _arg_or_kwarg(db.client.indices.delete.call_args, "index") == "prefix-myindex"


def test_get_indexes_without_global_prefix_lists_all_indexes():
    db = _get_db("")
    db.client.indices.get_alias.return_value = {}

    db.getIndexes()
    assert _arg_or_kwarg(db.client.indices.get_alias.call_args, "index") == "*"


def test_get_docs_and_search_builder_use_prefixed_index_names(monkeypatch):
    db = _get_db("prefix-")
    db.client.mget.return_value = {"docs": [{"_id": "7", "found": True, "_source": {"v": 1}}]}

    db.getDocs(lambda _doc_id, _vo: "logs", ["7"], "lhcb")
    mget_body = _arg_or_kwarg(db.client.mget.call_args, "body")
    assert mget_body["docs"][0]["_index"] == "prefix-logs"

    captured = {}

    def _fake_search(*, using, index):
        captured["using"] = using
        captured["index"] = index
        return "search-object"

    monkeypatch.setattr(elastic_module, "Search", _fake_search)
    assert db._Search("myindex") == "search-object"
    assert captured["using"] is db.client
    assert captured["index"] == "prefix-myindex"


def test_get_unique_value_leaves_prefixing_to_search_builder():
    db = _get_db("prefix-")
    query = MagicMock()
    query.filter.return_value = query
    query.extra.return_value = query
    query.execute.return_value = SimpleNamespace(
        aggregations={"quantity": SimpleNamespace(buckets=[{"key": 1}, {"key": 2}])}
    )
    db._Search = MagicMock(return_value=query)

    result = db.getUniqueValue("myindex", "quantity")

    assert result["OK"]
    assert result["Value"] == [1, 2]
    db._Search.assert_called_once_with("myindex")


def test_bulk_index_prefixes_once_but_keeps_internal_checks_unprefixed(monkeypatch):
    db = _get_db("prefix-")
    db.existingIndex = MagicMock(return_value={"OK": True, "Value": False})
    db.createIndex = MagicMock(return_value={"OK": True, "Value": "created"})

    seen = {}

    def _fake_bulk(*, client, index, actions):
        seen["client"] = client
        seen["index"] = index
        seen["actions"] = list(actions)
        return (len(seen["actions"]), [])

    monkeypatch.setattr(elastic_module, "bulk", _fake_bulk)
    res = db.bulk_index("myindex", data=[{"a": 1}, {"a": 2}], mapping=None, period=None, withTimeStamp=False)

    assert res["OK"]
    assert res["Value"] == 2
    db.existingIndex.assert_called_once_with("myindex")
    db.createIndex.assert_called_once_with("myindex", {}, None)
    assert seen["client"] is db.client
    assert seen["index"] == "prefix-myindex"


def test_create_index_with_period_uses_generated_name_and_global_prefix():
    db = _get_db("prefix-")
    db.client.indices.create.return_value = {"acknowledged": True}
    db.generateFullIndexName = MagicMock(return_value="myindex-2026-04-25")

    res = db.createIndex("myindex", mapping={"a": {"type": "keyword"}}, period="day")

    assert res["OK"]
    assert res["Value"] == "prefix-myindex-2026-04-25"
    db.generateFullIndexName.assert_called_once_with("myindex", "day")
    assert db.client.indices.create.call_args.kwargs["index"] == "prefix-myindex-2026-04-25"


def test_bulk_index_with_period_uses_prefixed_generated_name_once(monkeypatch):
    db = _get_db("prefix-")
    db.generateFullIndexName = MagicMock(return_value="myindex-2026-04-25")
    db.existingIndex = MagicMock(return_value={"OK": True, "Value": False})
    db.createIndex = MagicMock(return_value={"OK": True, "Value": "created"})

    seen = {}

    def _fake_bulk(*, client, index, actions):
        seen["client"] = client
        seen["index"] = index
        seen["actions"] = list(actions)
        return (len(seen["actions"]), [])

    monkeypatch.setattr(elastic_module, "bulk", _fake_bulk)
    res = db.bulk_index("myindex", data=[{"a": 1}], mapping={}, period="day", withTimeStamp=False)

    assert res["OK"]
    assert res["Value"] == 1
    db.generateFullIndexName.assert_called_once_with("myindex", "day")
    db.existingIndex.assert_called_once_with("myindex-2026-04-25")
    db.createIndex.assert_called_once_with("myindex", {}, "day")
    assert seen["client"] is db.client
    assert seen["index"] == "prefix-myindex-2026-04-25"
