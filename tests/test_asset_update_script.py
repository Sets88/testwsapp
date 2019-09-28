from decimal import Decimal
import os
from unittest import mock

import pytest
import asyncio_redis
import asyncpg

from testwsapp.asset_update_script import AssetsUpdate


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
    assertupdate = AssetsUpdate()
    await assertupdate.redis_connect()
    await assertupdate.redis.set('1', '1')
    res = await assertupdate.redis.get('1')
    assert res == '1'


async def test_db_connect():
    assertupdate = AssetsUpdate()
    await assertupdate.db_connect()
    res = await assertupdate.db.fetch('SELECT 1;')
    assert res[0][0] == 1


async def test_update_assets():
    assertupdate = AssetsUpdate()
    await assertupdate.db_connect()
    await assertupdate.update_assets()
    assert assertupdate.assets == {'EURUSD': 1, 'USDJPY': 2, 'GBPUSD': 3, 'AUDUSD': 4, 'USDCAD': 5}


async def test_add_db_asset_history():
    db = AMock()
    assertupdate = AssetsUpdate()
    assertupdate.db = db
    await assertupdate.add_db_asset_history(1, 100, 1.0)
    db.execute.assert_called_with('INSERT INTO asset_history (asset_id, timestamp, value) VALUES ($1, $2, $3)',
                                  1, 100, 1.0)


async def test_publish_update():
    redis = AMock()
    assertupdate = AssetsUpdate()
    assertupdate.redis = redis
    await assertupdate.publish_update('EURUSD', 1, 100, 1.0)
    redis.publish.assert_called_once_with('%sasset_data_1' % os.environ.get('REDIS_PREFIX', ''),
                                          '{"assetName": "EURUSD", "time": 100, "assetId": 1, "value": 1.0}')

@mock.patch('urllib.request.urlopen')
def test_get_assets_data(mock_urlopen):
    test_data = b'null({"Rates":[{"Symbol":"EURUSD","Bid":"1.09393","Ask":"1.09395","Spread":"0.20","ProductType":"1",}]});\n'
    assertupdate = AssetsUpdate()
    mock_read = mock.Mock()
    mock_urlopen.return_value = mock_read
    mock_read.read.return_value = test_data
    res = assertupdate.get_assets_data()
    assert res['Rates'][0]['Symbol'] == 'EURUSD'
    assert res['Rates'][0]['Bid'] == '1.09393'
    assert res['Rates'][0]['Ask'] == '1.09395'
    assert res['Rates'][0]['Spread'] == '0.20'
    assert res['Rates'][0]['ProductType'] == '1'

async def test_parse_data():
    data = dict(Rates=[dict(Symbol='WRONG'), dict(Symbol='EURUSD', Bid='1.1', Ask='1.2')])
    publish_update = AMock()
    add_db_asset_history = AMock()
    assertupdate = AssetsUpdate()
    assertupdate.assets = {'EURUSD': 1}
    assertupdate.publish_update = publish_update
    assertupdate.add_db_asset_history = add_db_asset_history
    await assertupdate.parse_data(data, 100)
    publish_update.assert_called_once_with('EURUSD', 1, 100, Decimal('1.15'))
    add_db_asset_history.assert_called_once_with(1, 100, Decimal('1.15'))
