import requests
import time
import logging
import pytest
import sys

if ret := pytest.main():
    sys.exit(ret)

time.sleep(3)

ITEM = {"a": "b", "c": {"a": 1}}

logging.getLogger().setLevel(logging.INFO)

resp = requests.put("http://pykv/api/v1/item/key123", json=ITEM)
logging.info(resp.json())
assert resp.json() == {"status": "ok"}


resp = requests.get("http://pykv/api/v1/item/key123")
logging.info(resp.json())
assert resp.json().get("content") == ITEM


resp = requests.post("http://pykv/api/v1/begin")
logging.info(resp.json())
assert "passkey" in resp.json()
assert resp.json().get("deep") == 1
passkey = resp.json().get("passkey")


resp = requests.post("http://pykv/api/v1/begin", params={"passkey": passkey})
logging.info(resp.json())
assert "passkey" in resp.json()
assert resp.json().get("deep") == 2
passkey = resp.json().get("passkey")


resp = requests.put("http://pykv/api/v1/item/key123",
                    params={"passkey": passkey},
                    json=ITEM | {"x": "y"})
logging.info(resp.json())
assert resp.json() == {"status": "ok"}


resp = requests.get("http://pykv/api/v1/item/key123")
logging.info(resp.json())
assert resp.json().get("content") == ITEM


resp = requests.post("http://pykv/api/v1/commit", params={"passkey": passkey})
logging.info(resp.json())
assert "passkey" in resp.json()
assert resp.json().get("deep") == 1
passkey = resp.json().get("passkey")


resp = requests.get("http://pykv/api/v1/item/key123")
logging.info(resp.json())
assert resp.json().get("content") != ITEM


resp = requests.post("http://pykv/api/v1/rollback", params={"passkey": "this is wrong passkey"})
assert "error" in resp.json()


resp = requests.post("http://pykv/api/v1/rollback", params={"passkey": passkey})
logging.info(resp.json())
assert "passkey" in resp.json()
assert resp.json().get("deep") == 0
passkey = resp.json().get("passkey")
assert passkey == ""


resp = requests.put("http://pykv/api/v1/item/key1", json=ITEM | {"x": "y"})
logging.info(resp.json())
assert resp.json() == {"status": "ok"}
resp = requests.put("http://pykv/api/v1/item/key2", json=ITEM | {"x": "y"})
logging.info(resp.json())
assert resp.json() == {"status": "ok"}
resp = requests.put("http://pykv/api/v1/item/key3", json=ITEM | {"x": "y"})
logging.info(resp.json())
assert resp.json() == {"status": "ok"}

resp = requests.delete("http://pykv/api/v1/item/key123")
logging.info(resp.json())
assert resp.json() == {"status": "ok"}

resp = requests.get("http://pykv/api/v1/selectkeys", json=ITEM | {"x": "y"})
logging.info(resp.json())
assert resp.json() == {"status": "ok", "keys": ["key1", "key2", "key3"]}