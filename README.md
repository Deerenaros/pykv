# Brief

Python-pure kv-database relying on filesystem with support of nested transactions in way, where commited nested transaction will be commited independently on parent. Also supports getting value by key, setting value by key, removing value by key, begining transaction and commit or rollback one.

Also providen webserver which repeats internal API but could contains little limitations on what key can content due to url-encode specifics.

# Run

To run locally with tests just use compose.yaml. This bundles FastAPI/unicorn web-server as first container and integrated part of tests as second container.

    docker compose up -d

After launching, `tests/__main__.py` will run pytests content than, after around 3 seconds, run integration smoke-test for webserver. If all tests run without errors - exit-code of container will be zero, which can be used on production setup.

# Setup

By default `8887` port will be used for webserver forwarding to 80 and `./data` will be used as mount for `/data` as DBPATH defaults.