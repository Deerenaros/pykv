def test_integrated():
    import time, requests
    time.sleep(3)

    ITEM = {"a": "b", "c": {"a": 1}}

    resp = requests.put("http://pykv/api/v1/item/key123", json=ITEM)
    assert resp.json() == {"status": "ok"}


    resp = requests.get("http://pykv/api/v1/item/key123")
    assert resp.json().get("content") == ITEM


    resp = requests.post("http://pykv/api/v1/begin")
    assert "passkey" in resp.json()
    assert resp.json().get("deep") == 1
    passkey = resp.json().get("passkey")


    resp = requests.post("http://pykv/api/v1/begin", params={"passkey": passkey})
    assert "passkey" in resp.json()
    assert resp.json().get("deep") == 2
    passkey = resp.json().get("passkey")


    resp = requests.put("http://pykv/api/v1/item/key123",
                        params={"passkey": passkey},
                        json=ITEM | {"x": "y"})
    assert resp.json() == {"status": "ok"}


    resp = requests.get("http://pykv/api/v1/item/key123")
    assert resp.json().get("content") == ITEM


    resp = requests.post("http://pykv/api/v1/commit", params={"passkey": passkey})
    assert "passkey" in resp.json()
    assert resp.json().get("deep") == 1
    passkey = resp.json().get("passkey")


    resp = requests.get("http://pykv/api/v1/item/key123")
    assert resp.json().get("content") != ITEM


    resp = requests.post("http://pykv/api/v1/rollback", params={"passkey": "this is wrong passkey"})
    assert "error" in resp.json()


    resp = requests.post("http://pykv/api/v1/rollback", params={"passkey": passkey})
    assert "passkey" in resp.json()
    assert resp.json().get("deep") == 0
    passkey = resp.json().get("passkey")
    assert passkey == ""


    resp = requests.post("http://pykv/api/v1/begin", params={"passkey": passkey})
    passkey = resp.json().get("passkey")

    resp = requests.put("http://pykv/api/v1/item/key1", json=ITEM | {"x": "y"}, params={"passkey": passkey})
    assert resp.json() == {"status": "ok"}
    resp = requests.put("http://pykv/api/v1/item/key2", json=ITEM | {"x": "y"}, params={"passkey": passkey})
    assert resp.json() == {"status": "ok"}
    resp = requests.put("http://pykv/api/v1/item/key3", json=ITEM | {"x": "y"}, params={"passkey": passkey})
    assert resp.json() == {"status": "ok"}

    resp = requests.post("http://pykv/api/v1/begin", params={"passkey": passkey})
    passkey = resp.json().get("passkey")

    resp = requests.delete("http://pykv/api/v1/item/key123", params={"passkey": passkey})
    assert resp.json() == {"status": "ok"}

    resp = requests.get("http://pykv/api/v1/item/key123")
    assert resp.json().get("status") == "ok"

    resp = requests.post("http://pykv/api/v1/commitall", params={"passkey": passkey})
    assert resp.json() == {'status': 'ok', 'deep': 0, 'passkey': ''}


    resp = requests.get("http://pykv/api/v1/selectkeys", json=ITEM | {"x": "y"})
    assert resp.json() == {"status": "ok", "keys": ["key1", "key2", "key3"]}