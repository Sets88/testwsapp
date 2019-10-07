source credentials.env && ./virtual-env/bin/gunicorn testwsapp.ws_app:get_app --reload -b 0.0.0.0:8080 -t 9999 --worker-class aiohttp.worker.GunicornWebWorker
