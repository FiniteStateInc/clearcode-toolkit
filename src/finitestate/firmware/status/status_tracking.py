"""
  status_tracking.py

  Common utilities and enumerations used for tracking the process
  of firmware unpack and analysis.
"""

import boto3
import datetime
import json
import logging
import os
import uuid
import urllib.parse
from enum import Enum, unique, auto

from .analysis_dag import get_file_unpacked_plugins, get_file_unpacked_at_path_plugins, get_firmware_unpacked_plugins, get_rollup_plugins
from .fspluginoutputresult import FSPluginOutputResult

logger = logging.getLogger(__name__)

EVENT_TOPIC_ARN = os.environ.get('EVENT_TOPIC_ARN')

sns = None


def __get_sns():
    global sns
    if sns is None:
        sns = boto3.client('sns')
    return sns


# Most plugins have a unique name, introducing similar concept for all UNPACK-related code
@unique
class Stage(Enum):
    """ Possible master stages for all phases of firmware and file unpack and analysis """
    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value

    UNPACK = 'unpack'
    ANALYSIS = 'analysis'
    RISK = 'risk'


UNPACK_COMPONENT_NAME = str(Stage.UNPACK)


@unique
class State(Enum):
    """ Possible states for all phases of firmware and file unpack and analysis """
    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    @property
    def terminal(self):
        return self in [State.DONE, State.ERROR, State.DUPLICATE]

    ERROR = auto()
    ERROR_NON_FATAL = auto()
    QUEUED = auto()
    PROCESSING = auto()
    QUEUED_RETRY = auto()
    PROCESSING_RETRY = auto()
    DONE = auto()
    DUPLICATE = auto()


def generate_fwan_process_id() -> str:
    """ Generates a new Firmware Analysis Process ID """
    return str(uuid.uuid4())


def update_status(fwan_process_id: str,
                  component: str,
                  event_code: State,
                  file_id: str,
                  occurred_at=None,
                  event_details=None,
                  result_code: FSPluginOutputResult = FSPluginOutputResult.NOT_SPECIFIED):
    """ Publishes a status message to the SNS topic for status_tracker """
    event = {
        'fwan_process_id': fwan_process_id,
        # Escaping unicode pathnames that the DB can't handle through SQLAlchemy. Safest way is just to "replace" characters it can't handle
        'object_hash_id': file_id.encode('utf8', 'surrogateescape').decode('utf-8', 'replace'),
        'component_name': component,
        'event_code': event_code,
        'occurred_at': occurred_at or datetime.datetime.utcnow().timestamp(),
        'event_details': event_details or {},
        'result_code': result_code,
    }
    logger.debug(f'EVENT_TOPIC_ARN in update_status is {EVENT_TOPIC_ARN}')

    if EVENT_TOPIC_ARN:
        sns_client = __get_sns()
        sns_client.publish(TopicArn=EVENT_TOPIC_ARN, Message=json.dumps(event, default=str).strip())
    else:
        logger.warning(f'Event topic is not configured - notification suppressed - {json.dumps(event, default=str).strip()}')


def get_db_session(connect_timeout=5):
    """ Returns a db_session for current status_tracker PostgreSQL database, default connection timeout 5 seconds """
    from finitestate.common.aws.secrets import get_secret
    from finitestate.common.db.get_pg_session import setup_session

    REGION = os.environ['AWS_DEFAULT_REGION']
    ENVIRONMENT = os.environ['ENVIRONMENT']
    INSTANCE = os.getenv('INSTANCE', '')
    database_secret = get_secret(f'fwan-status/{ENVIRONMENT}{INSTANCE}/firmware', REGION)
    database_proxy_host = f'statustrackerproxy{INSTANCE}.{ENVIRONMENT}.fstate.ninja'
    url_encoded_password = urllib.parse.quote(database_secret.get('password'))
    
    db_url = f"postgresql://{database_secret.get('username')}:{url_encoded_password}@{database_proxy_host}:{database_secret.get('port')}/postgres"
    return setup_session(pg_url=db_url, app_name='status_tracker', connection_args={'connect_timeout': connect_timeout})


def init_file_status(fwan_process_id: str, file_id: str, db_session=None):
    """ Write all file 'to be done' plugin rows to active_component table
        :param str fwan_process_id: The process id for this proposed firmware
        :param str file_id: Unique identifier, usually either firmware_hash or file_hash
        :param sqlalchemy.Session db_session: if None, a new session object is created for this function and committed.
                                              if not None, operations will be performed with the session object and not committed
                                                (commit is responsibility of caller).
    """
    from finitestate.firmware.status.postgres.model import ActiveComponent
    null_session = False
    if db_session is None:
        db_session = get_db_session()
        null_session = True

    # Query DAG for all file plugins to add
    for plugin in get_file_unpacked_plugins():
        db_session.merge(ActiveComponent(fwan_process_id=fwan_process_id, file_id=file_id, component_id=plugin))
    if null_session:
        db_session.commit()


def init_file_at_path_status(fwan_process_id: str, file_id: str, db_session=None):
    """ Write all file 'to be done' plugin rows to active_component table
        :param str fwan_process_id: The process id for this proposed firmware
        :param str file_id: Compound key (e.g. 'file_hash:full_path')
        :param sqlalchemy.Session db_session: if None, a new session object is created for this function and committed.
                                              if not None, operations will be performed with the session object and not committed
                                                (commit is responsibility of caller).
    """
    from finitestate.firmware.status.postgres.model import ActiveComponent
    null_session = False
    if db_session is None:
        db_session = get_db_session()
        null_session = True

    # Query DAG for all file plugins to add
    for plugin in get_file_unpacked_at_path_plugins():
        # Escaping unicode pathnames that the DB can't handle through SQLAlchemy. Safest way is just to "replace" characters it can't handle
        db_session.merge(
            ActiveComponent(fwan_process_id=fwan_process_id,
                            file_id=file_id.encode('utf8', 'surrogateescape').decode('utf-8', 'replace'),
                            component_id=plugin))
    if null_session:
        db_session.commit()


def init_firmware_status(fwan_process_id: str, firmware_id: str, db_session=None):
    """ Write all firmware 'to be done' plugin rows to active_component table
        :param str fwan_process_id: The process id for this proposed firmware
        :param str firmware_id: The SHA256 hash of the firmware
        :param sqlalchemy.Session db_session: if None, a new session object is created for this function and committed.
                                              if not None, operations will be performed with the session object and not committed
                                                (commit is responsibility of caller).
    """
    from finitestate.firmware.status.postgres.model import ActiveComponent
    null_session = False
    if db_session is None:
        db_session = get_db_session()
        null_session = True

    # Query DAG for all firmware plugins to add
    for plugin in get_firmware_unpacked_plugins():
        db_session.merge(ActiveComponent(fwan_process_id=fwan_process_id, file_id=firmware_id, component_id=plugin))
    if null_session:
        db_session.commit()


def init_rollup_status(fwan_process_id: str, firmware_id: str, db_session=None):
    """ Write all rollup 'to be done' plugin rows to active_component table
        :param str fwan_process_id: The process id for this proposed firmware
        :param str firmware_id: The SHA256 hash of the firmware
        :param sqlalchemy.Session db_session: if None, a new session object is created for this function and committed.
                                              if not None, operations will be performed with the session object and not committed
                                                (commit is responsibility of caller).
    """
    from finitestate.firmware.status.postgres.model import ActiveComponent
    null_session = False
    if db_session is None:
        db_session = get_db_session()
        null_session = True

    # Query DAG for all rollup plugins to add
    for plugin in get_rollup_plugins():
        db_session.merge(ActiveComponent(fwan_process_id=fwan_process_id, file_id=firmware_id, component_id=plugin))
    if null_session:
        db_session.commit()
