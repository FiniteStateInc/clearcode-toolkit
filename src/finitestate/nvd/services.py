#!/usr/bin/env python

from contextlib import contextmanager
import json
import logging
import boto3
import psycopg2


logging.basicConfig(level=logging.CRITICAL)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SimpleAWSSecretService(object):
    def __init__(self, **kwargs):

        if not kwargs.get('aws_region'):
            raise Exception('"aws_region" is a required keyword argument for SimpleAWSSecretService.')

        region = kwargs['aws_region']
        profile = kwargs.get('profile', 'default')

        if profile == 'default':
            logger.debug('creating boto3 session with no profile spec...')
            b3session = boto3.session.Session()
        else:
            logger.debug('creating boto3 session with profile "%s"...' % profile)
            b3session = boto3.session.Session(profile_name=profile)

        self.asm_client = b3session.client('secretsmanager', region_name=region)


    def get_secret(self, secret_name):
        secret_value = self.asm_client.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value['SecretString'])


PSYCOPG_SVC_PARAM_NAMES = [
    'dbname',
    'user',
    'password',
    'host',
    'port'
]

class PostgresPsycopgService(object):
    def __init__(self, **kwargs):
        raw_params = kwargs

        self.db_connection_params = {}
        for name in PSYCOPG_SVC_PARAM_NAMES:
            self.db_connection_params[name] = raw_params[name]
        self.db_connection_params['connect_timeout'] = 3


    def open_connection(self):
        return psycopg2.connect(**self.db_connection_params)


    @contextmanager
    def connect(self):
        connection = None
        try:
            connection = psycopg2.connect(**self.db_connection_params)
            yield connection
            connection.commit()
        except:
            if connection is not None:
                connection.rollback()
            raise
        finally:
            if connection is not None:
                connection.close()
