source credentials.env && ./virtual-env/bin/gunicorn testwsapp.ws_app --reload -b 0.0.0.0:8080 --worker-class aiohttp.worker.GunicornWebWorker
