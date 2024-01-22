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

    db.set("int", 2**10)
    ts_1 = db.begin()

    ts_1.set("int", 123)
    assert ts_1.get("int") == 2**10

    ts_2 = ts_1.begin()
    ts_2.set("rollbacking", 123)

    ts_3 = ts_2.begin()
    ts_3.set("commiting", 123)

    ts_3.commit()
    ts_2.rollback()

    ts_1.commit()
  
    assert db.get("int") == 123
    assert db.get("rollbacking", True) is None
    assert db.get("commiting") == 123
