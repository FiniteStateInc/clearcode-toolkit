#!/usr/bin/env python3
# Set up a SQLAlchemy session into a PostgreSQL database

import atexit
import inspect
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

logger = logging.getLogger(__name__)


def setup_session(pg_url=None, echo_statements=False, search_path='public', app_name=None, dev=False, pgbouncer=False, time_zone='UTC', null_pool=True, connection_args={}):

    # Pick the top of the stack if the application name is not explicitly set.
    if not app_name:
        app_name = inspect.stack()[-1][1]

    # Set up a session with the database:
    if null_pool:
        pg_engine = create_engine(pg_url, echo=echo_statements, poolclass=NullPool, connect_args=connection_args)
    else:
        pg_engine = create_engine(pg_url, echo=echo_statements, connect_args=connection_args)

    PgSession = sessionmaker(bind=pg_engine)
    pg_session = PgSession()
    pg_session.execute('set search_path to %s' % search_path)
    pg_session.execute('set time zone %s' % time_zone)

    atexit.register(pg_session.close)

    return pg_session
