import asyncio
from decimal import Decimal
import time
import os
import traceback
from urllib import request

import asyncpg
import asyncio_redis
import simplejson as json


class AssetsUpdate():
    def __init__(self):
        self.redis = None
        self.db = None
        self.assets = dict()

    async def redis_connect(self):
        """!Connecting to redis server"""
        if self.redis is None:
            redis_host = os.environ.get('REDIS_HOST', 'localhost')
            self.redis = await asyncio_redis.Connection.create(host=redis_host, port=6379, db=1)

    async def db_connect(self):
        """!Connecting to database"""
        if self.db is None:
            self.db = await asyncpg.connect(user=os.environ['POSTGRES_DB_USER'],
                                            password=os.environ['POSTGRES_DB_PASS'],
                                            database=os.environ['POSTGRES_DB_NAME'],
                                            host=os.environ['POSTGRES_DB_HOST'],
                                            port=os.environ['POSTGRES_DB_PORT'])

    async def update_assets(self):
        """!Updating assets from db"""
        res = await self.db.fetch('SELECT id, symbol FROM assets')
        self.assets = {x['symbol']: x['id'] for x in res}

    async def add_db_asset_history(self, asset_id, timestamp, value):
        """!Adding asset history into database
        @param asset_id id of asset
        @param timestamp unixtime of when value received
        @param value decimal value of current asset"""
        await self.db.execute('''INSERT INTO asset_history (asset_id, timestamp, value) VALUES ($1, $2, $3)''',
                              asset_id, timestamp, value)

    async def publish_update(self, asset_name, asset_id, timestamp, value):
        """!Publishing record about asset into current asset redis channel
        @param asset_name symbol of asset
        @param asset_id id of asset
        @param timestamp unixtime of when value received
        @param value decimal value of current asset"""
        redis_prefix = os.environ.get('REDIS_PREFIX', '')
        data = json.dumps(dict(assetName=asset_name, time=timestamp, assetId=asset_id, value=value))
        await self.redis.publish('%sasset_data_%s' % (redis_prefix, asset_id), data)

    def get_assets_data(self):
        """!Loading assed data from forex trading
        @return parsed data containing information about all assets"""
        data = request.urlopen('https://ratesjson.fxcm.com/DataDisplayer').read().strip()
        asset_data = json.loads(data.replace(b',}', b'}')[5:-2])
        return asset_data

    async def parse_data(self, asset_data, timestamp):
        """Filtering assets data to take only reqired assets, calculating it and sending into assets channels
        @param asset_data parsed assets data received from forex trading
        @param timestamp unixtime of when assets data received"""
        for item in asset_data['Rates']:
            if item['Symbol'] in self.assets.keys():
                value = (Decimal(item['Bid']) + Decimal(item['Ask'])) / 2
                await self.publish_update(item['Symbol'], self.assets[item['Symbol']], timestamp, value)
                await self.add_db_asset_history(self.assets[item['Symbol']], timestamp, value)

    async def run(self):
        await self.redis_connect()
        await self.db_connect()
        timestamp = None
        while True:
            try:
                if int(time.time()) == timestamp:
                    continue
                timestamp = int(time.time())
                await self.update_assets()
                asset_data = self.get_assets_data()
                await self.parse_data(asset_data, timestamp)
                await asyncio.sleep(0.1)
            except Exception:
                traceback.print_exc()


if __name__ == '__main__':
    app = AssetsUpdate()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.run())
    loop.close()
