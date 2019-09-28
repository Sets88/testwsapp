import asyncio
from datetime import datetime
from datetime import timedelta
import os
from unittest import mock

import pytest
import asyncio_redis
import asyncpg

from testwsapp.ws_app import application
from testwsapp.ws_app import WSApp
import simplejson as json


async def async_magic():
    pass


class AMock(mock.Mock):
    def __await__(self):
        return async_magic().__await__()


async def redis_connect():
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    return await asyncio_redis.Connection.create(host=redis_host, port=6379, db=1)


async def db_connect():
        return await asyncpg.connect(user=os.environ['POSTGRES_DB_USER'],
                                     password=os.environ['POSTGRES_DB_PASS'],
                                     database=os.environ['POSTGRES_DB_NAME'],
                                     host=os.environ['POSTGRES_DB_HOST'],
                                     port=os.environ['POSTGRES_DB_PORT'])


async def test_redis_connect():
    wsapp = WSApp(None)
    await wsapp.redis_connect()
    await wsapp.redis.set('1', '1')
    res = await wsapp.redis.get('1')
    assert res == '1'


async def test_db_connect():
    wsapp = WSApp(None)
    await wsapp.db_connect()
    res = await wsapp.db.fetch('SELECT 1;')
    assert res[0][0] == 1


async def test_send_error():
    ws = AMock()

    wsapp = WSApp(None)
    wsapp.ws = ws
    await wsapp.send_error(dict(action='test'), error='testerror')
    assert json.loads(ws.send_str.call_args[0][0]) == {'status': 'error', 'action': 'test', 'error': 'testerror'}


async def test_send_json():
    ws = AMock()

    wsapp = WSApp(None)
    wsapp.ws = ws
    await wsapp.send_json('test', status='teststat', test='rewritetest')
    assert json.loads(ws.send_str.call_args[0][0]) == {'action': 'test', 'test': 'rewritetest', 'status': 'teststat'}


async def test_channel_subscribe():
    redis_pref = os.environ.get('REDIS_PREFIX', '')
    wsapp = WSApp(None)
    redis_conn = await redis_connect()
    await wsapp.redis_connect()
    await wsapp.channel_subscribe(2)
    await redis_conn.publish('%sasset_data_%s' % (redis_pref, 2), "testmsg")
    msg = await asyncio.wait_for(wsapp.subscriber.next_published(), timeout=3)
    assert msg.value == 'testmsg'


async def test_check_input_subsribe():
    wsapp = WSApp(None)
    assert await wsapp.check_input_subscribe(dict(test='test')) == 'assetId not passed'
    assert await wsapp.check_input_subscribe(dict(assetId='test')) == 'Wrong assetId format'
    assert await wsapp.check_input_subscribe(dict(assetId='1')) == None

async def test_get_recent_data_by_asset_id():
    wsapp = WSApp(None)
    wsapp.current_asset = dict(symbol='EURUSD')
    await wsapp.db_connect()
    db = await db_connect()
    res = await wsapp.get_recent_data_by_asset_id(1)
    assert len(res) == 0
    timestamp = int((datetime.now() - timedelta(minutes=35)).strftime('%s'))
    await db.execute('''INSERT INTO asset_history (asset_id, timestamp, value) VALUES ($1, $2, $3)''',
                     1, timestamp, 10)
    res = await wsapp.get_recent_data_by_asset_id(1)
    assert len(res) == 0
    timestamp = int((datetime.now() - timedelta(minutes=25)).strftime('%s'))
    await db.execute('''INSERT INTO asset_history (asset_id, timestamp, value) VALUES ($1, $2, $3)''',
                     1, timestamp, 10)
    res = await wsapp.get_recent_data_by_asset_id(1)
    assert len(res) == 1
    assert res[0]['assetid'] == 1
    assert res[0]['value'] == 10
    assert res[0]['assetname'] == 'EURUSD'


async def test_get_asset_by_id():
    wsapp = WSApp(None)
    wsapp.current_asset = dict(symbol='EURUSD')
    await wsapp.db_connect()
    db = await db_connect()
    res = await wsapp.get_asset_by_id(1)
    assert res
    assert res['id'] == 1
    assert res['symbol'] == 'EURUSD'


async def test_action_assets():
    send_json = AMock()
    wsapp = WSApp(None)
    wsapp.send_json = send_json
    await wsapp.db_connect()
    await wsapp.action_assets(dict(action='test'))
    send_json.assert_called_with('test',
                                  message={'assets': [{'id': 1, 'name': 'EURUSD'},
                                                      {'id': 2, 'name': 'USDJPY'},
                                                      {'id': 3, 'name': 'GBPUSD'},
                                                      {'id': 4, 'name': 'AUDUSD'},
                                                      {'id': 5, 'name': 'USDCAD'}]})

async def test_action_subscribe():
    send_error = AMock()
    send_json = AMock()
    wsapp = WSApp(None)
    wsapp.send_json = send_json
    wsapp.send_error = send_error
    wsapp.channel_subscribe = AMock()
    db = await db_connect()
    await wsapp.db_connect()
    await wsapp.action_subscribe(dict(action='subscribe', assetId=10))
    assert send_error.called
    send_error = AMock()
    wsapp.send_error = send_error
    timestamp = int((datetime.now() - timedelta(minutes=25)).strftime('%s'))
    await db.execute('''INSERT INTO asset_history (asset_id, timestamp, value) VALUES ($1, $2, $3)''',
                     1, timestamp, 10)
    await wsapp.action_subscribe(dict(action='subscribe', assetId=1))
    assert not send_error.called
    assert send_json.called
    assert send_json.call_args[0][0] == 'subscribe'
    assert send_json.call_args[1]['message']['points'][0]['assetid'] == 1
    assert send_json.call_args[1]['message']['points'][0]['value'] == 10
    assert send_json.call_args[1]['message']['points'][0]['assetname'] == 'EURUSD'


async def test_process_actions():
    class AsyncWS():
        data = [dict(action='test'),
                dict(action='assets', aa='1'),
                dict(action='subscribe', aa='2'),
                dict(action='subscribe', assetId='2')]

        def __aiter__(self):
            return self

        def json(self):
            return self.data.pop()

        async def __anext__(self):
            if len(self.data) > 0:
                return self
            raise StopAsyncIteration

    wsapp = WSApp(None)
    wsapp.ws = AsyncWS()
    action_assets = AMock()
    action_subscribe = AMock()
    send_error = AMock()
    send_json = AMock()
    wsapp.action_assets = action_assets
    wsapp.action_subscribe = action_subscribe
    wsapp.send_json = send_json
    wsapp.send_error = send_error
    await wsapp.process_actions()
    action_assets.assert_called_once_with(dict(action='assets', aa='1'))
    action_subscribe.assert_called_once_with(dict(action='subscribe', assetId='2'))
    send_error.assert_called_once_with(dict(action='subscribe', aa='2'), 'assetId not passed')


async def test_intergration(test_client):
    db = await db_connect()
    redis_conn = await redis_connect()
    redis_prefix = os.environ.get('REDIS_PREFIX', '')

    timestamp = int((datetime.now() - timedelta(minutes=35)).strftime('%s'))
    await db.execute('''INSERT INTO asset_history (asset_id, timestamp, value) VALUES ($1, $2, $3)''',
                     1, timestamp, 10)
    timestamp = int((datetime.now() - timedelta(minutes=25)).strftime('%s'))
    await db.execute('''INSERT INTO asset_history (asset_id, timestamp, value) VALUES ($1, $2, $3)''',
                     1, timestamp, 10)


    client = await test_client(application)
    async with client.ws_connect('/') as ws:
        await ws.send_json(dict(action='assets'))
        res = await asyncio.wait_for(ws.receive(), timeout=3)
        res.json() == {'status': 'success',
                       'action': 'assets',
                       'message': {'assets': [{'id': 1, 'name': 'EURUSD'},
                                              {'id': 2, 'name': 'USDJPY'},
                                              {'id': 3, 'name': 'GBPUSD'},
                                              {'id': 4, 'name': 'AUDUSD'},
                                              {'id': 5, 'name': 'USDCAD'}]}}

        # Subscribing to asset 1
        await ws.send_json(dict(action='subscribe', assetId=1))
        res = await asyncio.wait_for(ws.receive(), timeout=1)
        res = json.loads(res.data)
        assert res['status'] == 'success'
        assert res['action'] == 'subscribe'
        assert res['action'] == 'subscribe'
        assert len(res['message']['points']) == 1
        assert res['message']['points'][0]['assetid'] == 1
        assert res['message']['points'][0]['value'] == 10
        assert res['message']['points'][0]['assetname'] == 'EURUSD'
        # Trying to get data shouldn't be
        try:
            await asyncio.wait_for(ws.receive(), timeout=0.3)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            res = None
        assert res == None
        # Sending data into redis channel
        data = json.dumps(dict(assetName='EURUSD', time=100, assetId=1, value=20))
        await redis_conn.publish('%sasset_data_%s' % (redis_prefix, 1), data)
        res = await asyncio.wait_for(ws.receive(), timeout=1)
        res = json.loads(res.data)
        assert res['message'] == json.loads(data)
        assert res['status'] == 'success'
        assert res['action'] == 'point'
        # Subscribing to asset 2
        await ws.send_json(dict(action='subscribe', assetId=2))
        res = await asyncio.wait_for(ws.receive(), timeout=1)
        res = json.loads(res.data)
        assert len(res['message']['points']) == 0

        # Sending data into redis channel for previous asset
        await redis_conn.publish('%sasset_data_%s' % (redis_prefix, 1), data)
        try:
            await asyncio.wait_for(ws.receive(), timeout=0.3)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            res = None
        assert res == None

        # Sending data into correct redis channel
        data = json.dumps(dict(assetName='USDJPY', time=200, assetId=2, value=30))
        await redis_conn.publish('%sasset_data_%s' % (redis_prefix, 2), data)
        res = await asyncio.wait_for(ws.receive(), timeout=1)
        res = json.loads(res.data)
        assert res['message'] == json.loads(data)
        assert res['status'] == 'success'
        assert res['action'] == 'point'

        # Sending data into correct redis channel once more time
        data = json.dumps(dict(assetName='USDJPY', time=300, assetId=2, value=40))
        await redis_conn.publish('%sasset_data_%s' % (redis_prefix, 2), data)
        res = await asyncio.wait_for(ws.receive(), timeout=1)
        res = json.loads(res.data)
        assert res['message'] == json.loads(data)
        assert res['status'] == 'success'
        assert res['action'] == 'point'
