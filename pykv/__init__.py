__version__ = "0.1"

import fastapi
import typing
import logging
import os
import json

from .db import DB
DBPATH = os.environ.get("DBPATH", DB.__init__.__defaults__[0])

app = fastapi.FastAPI()
db = DB(dbpath=DBPATH)


@app.get("/api/v1/item/{key}")
def get(key: str):
    try:
        return {
            "status": "ok",
            "content": db.get(key)
        }
    except Exception as e:
        logging.exception("error")
        return {"status": "error", "error": str(e)}


@app.put("/api/v1/item/{key}")
def set(key: str, value: dict[str, typing.Any], passkey: str = ""):
    try:
        if db.login(passkey):
            db.set(key, value)
    except Exception as e:
        logging.exception("error")
        return {"status": "error", "error": str(e)}
    return {"status": "ok"}


@app.get("/api/v1/selectkeys")
def selectkeys(value: dict[str, typing.Any]):
    result = []
    try:
        # value = json.loads(value)
        result.extend(db.select_keys(value))
    except Exception as e:
        logging.exception("error")
        return {"status": "error", "error": str(e)}
    return {"status": "ok", "keys": result}


@app.delete("/api/v1/item/{key}")
def delete(key: str, passkey: str = ""):
    try:
        if db.login(passkey):
            db.remove(key)
    except Exception as e:
        logging.exception("error")
        return {"status": "error", "error": str(e)}
    return {"status": "ok"}


@app.post("/api/v1/begin")
def begin(passkey: str = ""):
    global db
    try:
        if db.login(passkey):
            db = db.begin()
    except Exception as e:
        logging.exception("error")
        return {"status": "error", "error": str(e)}
    return {"status": "ok", "deep": db.count, "passkey": db.passkey}


@app.post("/api/v1/commit")
def commit(passkey: str = ""):
    global db
    try:
        if db.login(passkey):
            db = db.commit()
    except Exception as e:
        logging.exception("error")
        return {"status": "error", "error": str(e)}
    return {"status": "ok", "deep": db.count, "passkey": db.passkey}


@app.post("/api/v1/rollback")
def rollback(passkey: str = ""):
    global db
    try:
        if db.login(passkey):
            db = db.rollback()
    except Exception as e:
        logging.exception("error")
        return {"status": "error", "error": str(e)}
    return {"status": "ok", "deep": db.count, "passkey": db.passkey}
