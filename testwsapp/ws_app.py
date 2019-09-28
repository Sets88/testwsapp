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


class WSApp(object):
    def __init__(self, request):
        self.request = request
        self.ws = web.WebSocketResponse()
        self.redis = None
        self.db = None
        self.subscriber = None
        self.current_asset = None

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
        redis_prefix = os.environ.get('REDIS_PREFIX', '')
        if self.subscriber is None:
            self.subscriber = await self.redis.start_subscribe()
        else:
            await self.subscriber.unsubscribe(['%sasset_data_%s' % (redis_prefix, self.current_asset['id'])])
        await self.subscriber.subscribe(['%sasset_data_%s' % (redis_prefix, asset_id)])

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
        timestamp = int((datetime.now() - timedelta(minutes=period)).strftime('%s'))
        res = await self.db.fetch('''SELECT asset_id as assetId, timestamp as time, value, $1 as assetName 
                                     FROM asset_history
                                     WHERE asset_id = $2 AND timestamp > $3''',
                                  self.current_asset['symbol'], asset_id, timestamp)
        points = [dict(x) for x in res]
        return points

    async def get_asset_by_id(self, asset_id):
        """!Trying to find asset by assed_id
        @param asset_id id of asset to search by
        @retrun dict containing id and name of asset"""
        res = await self.db.fetchrow('SELECT id, symbol FROM assets WHERE id = $1', int(asset_id))
        if res:
            return dict(res)

    async def action_assets(self, data):
        """!Loading all assets and sends it into websocket
        @param data request data received from websocket"""
        res = await self.db.fetch('SELECT id, symbol as name FROM assets')
        assets = [dict(x) for x in res]
        await self.send_json(data['action'], message=dict(assets=assets))

    async def action_subscribe(self, data):
        """!Subscribing to asset redis channel and sending asset history for last 30 minutes
        @param data request data received from websocket"""
        res = await self.get_asset_by_id(data['assetId'])
        if not res:
            self.send_error(data, 'Asset was not found')
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
            if self.subscriber is not None:
                msg = await self.subscriber.next_published()
                data = json.loads(msg.value)
                await self.send_json('point', message=data)
            await asyncio.sleep(0.1)

    async def run(self):
        try:
            await self.ws.prepare(self.request)
            await self.redis_connect()
            await self.db_connect()
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
            if self.redis is not None:
                self.redis.close()
            try:
                await self.ws.close()
            except RuntimeError:
                pass
        return self.ws


async def ws_handler(request):
    return await WSApp(request).run()


async def on_shutdown(app):
    for ws in set(app['websockets']):
        await ws.close(code=WSCloseCode.GOING_AWAY, message='Server shutdown')


application = web.Application()
application['websockets'] = weakref.WeakSet()
application.router.add_get('/', ws_handler)
application.on_shutdown.append(on_shutdown)
