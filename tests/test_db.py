from db import DB


db = DB("/tmp/testing")


def test_set_get():
    db.drop()

    db.set("int", 2**10)
    db.set("str", "abcd")
    db.set("none", None)
    db.set("json", {"a": 1, "b": {"c": "d"}})

    assert db.get("int") == 2**10
    assert db.get("str") == "abcd"
    assert db.get("none") is None
    assert db.get("json") == {"a": 1, "b": {"c": "d"}}


def test_transactions():
    db.drop()

    db.set("int", 124)
    ts_1 = db.begin()

    ts_1.set("int", 123)
    assert ts_1.get("int") == 124

    ts_2 = ts_1.begin()
    ts_2.set("rollbacking", 123)
    assert db.get("rollbacking", True) is None

    ts_3 = ts_2.begin()
    ts_3.set("commiting", 123)

    ts_3.commit()
    ts_2.rollback()
    ts_1.commit()
  
    assert db.get("int") == 123
    assert db.get("rollbacking", True) is None
    assert db.get("commiting") == 123


def test_select():
    db.drop()

    db.set("key_1", "samevalue")
    db.set("key_2", "samevalue")
    db.set("key_3", "samevalue")

    t = db.begin()

    t.set("key_1", {"same": "json"})
    t.set("key_2", {"same": "json"})
    t.set("key_3", {"same": "json"})

    assert db.select_keys("samevalue") == ["key_1", "key_2", "key_3"]

    t.commit()

    assert db.select_keys({"same": "json"}) == ["key_1", "key_2", "key_3"]