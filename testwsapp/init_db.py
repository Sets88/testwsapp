import asyncio
from datetime import datetime
import os

import asyncpg


async def db_connect(db_name):
    db_conn = await asyncpg.connect(user=os.environ['POSTGRES_DB_USER'],
                                    password=os.environ['POSTGRES_DB_PASS'],
                                    database=db_name,
                                    host=os.environ['POSTGRES_DB_HOST'],
                                    port=os.environ['POSTGRES_DB_PORT'])
    return db_conn

async def create_tables(db_name, db_conn=None):
    if db_conn is None:
        db_conn = await db_connect(db_name)
    await db_conn.execute('''CREATE TABLE assets (
                                id serial PRIMARY KEY,
                                symbol VARCHAR(10));''')
    await db_conn.execute('''CREATE TABLE asset_history (
                             asset_id INTEGER REFERENCES assets (id),
                             timestamp INTEGER,
                             value DECIMAL);''')
    await db_conn.execute('CREATE INDEX ix_asset_history_timestamp ON asset_history (timestamp);')


async def populate(db_name, db_conn=None):
    if db_conn is None:
        db_conn = await db_connect(db_name)
    await db_conn.execute('''INSERT INTO assets (id, symbol) VALUES
                             (1, 'EURUSD'),
                             (2, 'USDJPY'),
                             (3, 'GBPUSD'),
                             (4, 'AUDUSD'),
                             (5, 'USDCAD');''')


async def init_db(db_name):
    db_conn = await db_connect(db_name)
    await create_tables(db_name, db_conn=db_conn)
    await populate(db_name, db_conn=db_conn)
    await db_conn.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db(os.environ['POSTGRES_DB_NAME']))
