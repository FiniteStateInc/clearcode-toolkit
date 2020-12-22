"""
base.py

Base class definitions for all message formats in Finite State firmware system.

"""
import json
from abc import abstractmethod
from uuid import UUID

from finitestate.common.json_utils import JsonClassSerializable


class FSPluginMessageBase(JsonClassSerializable):
    """Base class for all messages on the Finite State messaging systems.

    Args:
        fwan_process_id: Used for all related status tracking events. Defaults to uuid4().
        trigger_downstream_plugins: If set to False does not send a message to trigger
            normal downstream events. Defaults to True.
    """
    def __init__(self, fwan_process_id: UUID, trigger_downstream_plugins: bool = True):
        # Use UUID to verify valid format, but store as string (consistent with message formats)
        self.fwan_process_id = UUID(str(fwan_process_id)).hex
        self.trigger_downstream_plugins = trigger_downstream_plugins

    @property
    @abstractmethod
    def storage_id(self) -> str:
        """str: Returns the 'storage_id' provided by the message type.
        This is generally used if we need to know the 'primary key' that the message
        operates on, such as a file hash or a firmware hash.  This is used under
        the hood to load overrides and custom configurations.
        """
        return NotImplementedError()

    @property
    @abstractmethod
    def status_id(self) -> str:
        """str: Returns the 'status_id' provided by the message type.
        This is generally used if we need to know the 'primary key' that the message
        operates on, such as a file hash or a firmware hash, can be paired with path, etc.
        This is used under the hood for the status_tracker.
        """
        return NotImplementedError()

    @staticmethod
    @abstractmethod
    def deserialize(json_string: str):
        # __init__ does all the verification, no need to add any verifiers unless values needs bounds checks
        return FSPluginMessageBase(**json.loads(json_string))
