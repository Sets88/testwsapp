import asyncio
import re
import os
import subprocess
import sys
import time
import uuid

import pytest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from testwsapp.init_db import init_db


TEMPLATE_DB = None


def source_environment():
    credentials_file = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'testing_credentials.env')

    command = ['bash', '-c', 'source %s && env' % credentials_file]

    proc = subprocess.Popen(command, stdout=subprocess.PIPE)

    for line in proc.stdout:
        line = line.decode('utf-8')
        (key, _, value) = line.partition("=")
        os.environ[key] = value.strip()


def filter_schemas(item):
    return re.match(r'%s_[a-z0-9A-Z]' % os.environ['POSTGRES_DB_NAME'], item[0]) is not None


def drop_schemas():
    params = dict(host=os.environ['POSTGRES_DB_HOST'],
                  database=os.environ['POSTGRES_DB_NAME'],
                  port=os.environ.get('POSTGRES_DB_PORT', '5432'),
                  user=os.environ['POSTGRES_DB_USER'],
                  password=os.environ['POSTGRES_DB_PASS'])
    conn = psycopg2.connect(**params)
    try:
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE pg_catalog.pg_get_userbyid(datdba)='%s';" %
                    os.environ['POSTGRES_DB_USER'])
        schema_names = [x[0] for x in filter(filter_schemas, cur.fetchall())]
    finally:
        conn.close()

    for schema_name in schema_names:
        try:
            conn = psycopg2.connect(**params)
            conn.set_isolation_level(0)
            cur = conn.cursor()
            try:
                cur.execute('DROP DATABASE IF EXISTS %s;' % schema_name)
            finally:
                conn.close()
        except Exception:
            continue

def recreate_template_db():
    drop_template_sql = "DROP DATABASE IF EXISTS %s;" % TEMPLATE_DB
    create_template_sql = "CREATE DATABASE %s" % TEMPLATE_DB
    params = dict(host=os.environ['POSTGRES_DB_HOST'],
                  database=os.environ['POSTGRES_DB_NAME'],
                  port=os.environ.get('POSTGRES_DB_PORT', '5432'),
                  user=os.environ['POSTGRES_DB_USER'],
                  password=os.environ['POSTGRES_DB_PASS'])
    conn = psycopg2.connect(**params)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    try:
        cursor.execute(drop_template_sql)
        cursor.execute(create_template_sql)
        cursor.execute("GRANT ALL PRIVILEGES ON DATABASE %s TO %s" % (TEMPLATE_DB, os.environ['POSTGRES_DB_USER']))
    finally:
        conn.close()


def generate_template_db():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db(TEMPLATE_DB))


def generate_test_database(db_uuid):
    db_name = "%s_%s" % (os.environ['POSTGRES_DB_NAME'], db_uuid)
    drop_sql = "DROP DATABASE IF EXISTS %s;" % db_name
    create_sql = "CREATE DATABASE %s TEMPLATE %s;" % (db_name, TEMPLATE_DB)
    params = dict(host=os.environ['POSTGRES_DB_HOST'],
                  database=os.environ['POSTGRES_DB_NAME'],
                  port=os.environ.get('POSTGRES_DB_PORT', '5432'),
                  user=os.environ['POSTGRES_DB_USER'],
                  password=os.environ['POSTGRES_DB_PASS'])
    conn = psycopg2.connect(**params)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    try:
        cursor.execute(drop_sql)
        cursor.execute(create_sql)
        cursor.execute("GRANT ALL PRIVILEGES ON DATABASE %s TO %s" % (db_name, os.environ['POSTGRES_DB_USER']))
    finally:
        conn.close()

    return db_name



@pytest.fixture(scope="session", autouse=True)
def drop_old_databases_and_create_template():
    global TEMPLATE_DB
    source_environment()
    TEMPLATE_DB = "%s_template" % (os.environ['POSTGRES_DB_NAME'], )
    print("Removing old databases and creating template one")
    drop_schemas()
    recreate_template_db()
    generate_template_db()


@pytest.fixture(autouse=True)
def create_test_database():
    db_uuid = uuid.uuid4().hex
    if not os.environ.get('POSTGRES_ORIGINAL_DB_NAME'):
        os.environ['POSTGRES_ORIGINAL_DB_NAME'] = os.environ['POSTGRES_DB_NAME']
    else:
        os.environ['POSTGRES_DB_NAME'] = os.environ['POSTGRES_ORIGINAL_DB_NAME']
    os.environ['POSTGRES_DB_NAME'] = generate_test_database(db_uuid)
    os.environ['REDIS_PREFIX'] = db_uuid
