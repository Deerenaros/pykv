from logging import getLogger, DEBUG
from os import environ
from uvicorn import run

from . import app

getLogger().setLevel(DEBUG)
run(app, host="0.0.0.0", port=int(environ.get("PORT", 80)), log_level=DEBUG)