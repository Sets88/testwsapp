import asyncio
from datetime import datetime
from datetime import timedelta
import os
import traceback
import weakref

import asyncio_redis
import asyncpg
from aiohttp import web
from aiohttp import WSCloseCode
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.web_exceptions import HTTPMethodNotAllowed
import simplejson as json


class Subscriber():
    def __init__(self, asset_id):
        self.asset_id = int(asset_id)
        self.future = asyncio.Future()
        self.redis = None

    async def redis_connect(self):
        redis_host = os.environ.get('REDIS_HOST', 'localhost')
        self.redis = await asyncio_redis.Connection.create(host=redis_host, port=6379, db=1)

    def next_published(self):
        if self.future.cancelled():
            self.future = asyncio.Future()
        return self.future

    async def run(self):
        redis_prefix = os.environ.get('REDIS_PREFIX', '')
        await self.redis_connect()
        subscriber = await self.redis.start_subscribe()
        await subscriber.subscribe(['%sasset_data_%s' % (redis_prefix, self.asset_id)])
        while True:
            msg = await subscriber.next_published()
            self.future.set_result(msg)
            self.future = asyncio.Future()


class DataCollector():
    def __init__(self, db):
        self.db = db
        self.assets = dict()
        self.subscribers = dict()

    async def load_assets(self):
        async with self.db.acquire() as conn:
            res = await conn.fetch('SELECT id, symbol as name FROM assets')
            return [dict(x) for x in res]

    def get_subscriber(self, asset_id):
        return self.subscribers[asset_id]

    async def run(self):
        redis_prefix = os.environ.get('REDIS_PREFIX', '')
        loop = asyncio.get_event_loop()
        tasks = []
        for asset in await self.load_assets():
            subscriber = Subscriber(asset['id'])
            task = loop.create_task(subscriber.run())
            self.subscribers[int(asset['id'])] = subscriber
            tasks.append(task)
        await asyncio.gather(*tasks)


class WSApp():
    def __init__(self, request):
        self.request = request
        self.ws = web.WebSocketResponse()
        self.subscriber = None
        self.current_asset = None

    async def send_error(self, data, error):
        """!Sending error message into websocket
        @param data initial request received from websocket
        @param error error message to send into websocket"""
        await self.send_json(data['action'], status='error', error=error)

    async def send_json(self, action, status='success', **kwargs):
        """!Sending response message into websocket
        @param action current action message related to
        @param status status message which should be attached to final message
        @param **kwargs any passed parameter will update final dict message"""
        new_data = dict(status=status, action=action)
        new_data.update(kwargs)
        await self.ws.send_str(json.dumps(new_data))

    async def channel_subscribe(self, asset_id):
        """!Subscribing to asset redis channel
        @param asset_id id of asset to subscribe to"""
        self.subscriber = self.request.app['data'].get_subscriber(asset_id)

    async def check_input_subscribe(self, data):
        """!Checking input data of subscribe action received from websocket
        @param data data received from websocket
        @return Error message if worng data passed or None if no errors found"""
        if 'assetId' not in data:
            return 'assetId not passed'
        if not str(data['assetId']).isdigit():
            return 'Wrong assetId format'

    async def get_recent_data_by_asset_id(self, asset_id, period=30):
        """!Getting asset history of specified asset for specified period
        @param asset_id id of asset to get history about
        @param period period of history in minutes
        @return list of dicts with entire data of asset for specified period"""
        async with self.request.app['dbpool'].acquire() as conn:
            timestamp = int((datetime.now() - timedelta(minutes=period)).strftime('%s'))
            res = await conn.fetch('''SELECT asset_id as assetId, timestamp as time, value, $1 as assetName
                                      FROM asset_history
                                      WHERE asset_id = $2 AND timestamp > $3''',
                                   self.current_asset['symbol'], asset_id, timestamp)
            points = [dict(x) for x in res]
            return points

    async def get_asset_by_id(self, asset_id):
        """!Trying to find asset by assed_id
        @param asset_id id of asset to search by
        @retrun dict containing id and name of asset"""
        async with self.request.app['dbpool'].acquire() as conn:
            res = await conn.fetchrow('SELECT id, symbol FROM assets WHERE id = $1', int(asset_id))
            if res:
                return dict(res)

    async def action_assets(self, data):
        """!Loading all assets and sends it into websocket
        @param data request data received from websocket"""
        async with self.request.app['dbpool'].acquire() as conn:
            res = await conn.fetch('SELECT id, symbol as name FROM assets')
            assets = [dict(x) for x in res]
            await self.send_json(data['action'], message=dict(assets=assets))

    async def action_subscribe(self, data):
        """!Subscribing to asset redis channel and sending asset history for last 30 minutes
        @param data request data received from websocket"""
        res = await self.get_asset_by_id(data['assetId'])
        if not res:
            await self.send_error(data, 'Asset was not found')
            return
        await self.channel_subscribe(data['assetId'])
        self.current_asset = res
        points = await self.get_recent_data_by_asset_id(data['assetId'], 30)
        await self.send_json(data['action'], message=dict(points=points))

    async def process_actions(self):
        """!Processing requests received from websocket"""
        async for msg in self.ws:
            data = msg.json()
            if 'action' in data and hasattr(self, 'action_%s' % data['action']):
                if hasattr(self, 'check_input_%s' % data['action']):
                    error = await getattr(self, 'check_input_%s' % data['action'])(data)
                    if error:
                        await self.send_error(data, error)
                        continue
                await getattr(self, 'action_%s' % data['action'])(data)

    async def process_updates(self):
        """!Processing data received from asset redis channel"""
        while  True:
            try:
                if self.subscriber is not None:
                    subscriber = self.subscriber
                    msg = await asyncio.wait_for(asyncio.shield(subscriber.next_published()), 1.5)
                    if subscriber.asset_id != self.subscriber.asset_id:
                        continue
                    data = json.loads(msg.value)
                    await self.send_json('point', message=data)
            except (asyncio.TimeoutError):
                pass
            except Exception:
                traceback.print_exc();
                break
            await asyncio.sleep(0.1)

    async def run(self):
        try:
            await self.ws.prepare(self.request)
            while True:
                task = self.request.app.loop.create_task(self.process_updates())
                await self.process_actions()
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return self.ws
        except (HTTPBadRequest, HTTPMethodNotAllowed):
            resp = web.Response(status=404)
            await resp.prepare(self.request)
            return resp
        except Exception:
            traceback.print_exc()
            return self.ws
        finally:
            task.cancel()
            try:
                await self.ws.close()
            except RuntimeError:
                pass
        return self.ws


async def db_connect():
    return await asyncpg.create_pool(user=os.environ['POSTGRES_DB_USER'],
                                     password=os.environ['POSTGRES_DB_PASS'],
                                     database=os.environ['POSTGRES_DB_NAME'],
                                     host=os.environ['POSTGRES_DB_HOST'],
                                     port=os.environ['POSTGRES_DB_PORT'])


async def ws_handler(request):
    return await WSApp(request).run()


async def on_shutdown(app):
    for ws in set(app['websockets']):
        await ws.close(code=WSCloseCode.GOING_AWAY, message='Server shutdown')


async def get_app():
    application = web.Application()
    application['websockets'] = weakref.WeakSet()
    application['dbpool'] = await db_connect()
    application['data'] = DataCollector(application['dbpool'])
    asyncio.create_task(application['data'].run())
    application.router.add_get('/', ws_handler)
    application.on_shutdown.append(on_shutdown)
    return application
