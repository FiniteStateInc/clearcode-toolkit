"""
analysis_complete_message.py

Class definitions for 'Analysis Complete' messages used to kick off rollups.
"""
import json
from uuid import UUID

from .base import FSPluginMessageBase


class FSAnalysisCompleteMessage(FSPluginMessageBase):
    """Class for 'Analysis Complete' messages used to kick off rollups, serializable as JSON string.

    Args:
        firmware_id: SHA256 hash of source firmware to process.
        fwan_process_id: Used for all related status tracking events. Must be a UUID.
        trigger_downstream_plugins: If set to False does not send a message to trigger
            normal downstream events. Defaults to True.
    """

    def __init__(self, firmware_hash: str, fwan_process_id: UUID, trigger_downstream_plugins: bool = True):
        super().__init__(fwan_process_id, trigger_downstream_plugins)
        self.firmware_hash = firmware_hash

    @property
    def storage_id(self) -> str:
        return self.firmware_hash

    @property
    def status_id(self) -> str:
        return self.firmware_hash

    @staticmethod
    def deserialize(json_string: str):
        return FSAnalysisCompleteMessage(**json.loads(json_string))
