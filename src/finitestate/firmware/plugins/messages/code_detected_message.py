"""
code_detected_message.py

Class definition for 'source code detected' event notifications
"""
import json
from uuid import UUID

from .base import FSPluginMessageBase


class FSCodeDetectedMessage(FSPluginMessageBase):
    """Class for all 'source code detected'event messages code_detect plugin, serializable as JSON string.

    Args:
        file_id: SHA256 hash of source file to process.
        mime_type: Mime type of the given file
        language: Language of the source code that was detected
        fwan_process_id: Used for all related status tracking events. Defaults to uuid4().
        trigger_downstream_plugins: If set to False does not send a message to trigger
            normal downstream events. Defaults to True.
    """
    def __init__(self,
                 file_id: str,
                 mime_type: str,
                 language: str,
                 fwan_process_id: UUID,
                 trigger_downstream_plugins: bool = True):
        super().__init__(fwan_process_id, trigger_downstream_plugins)
        self.file_id = file_id
        self.mime_type = mime_type
        self.language = language

    @property
    def storage_id(self) -> str:
        return self.file_id

    @property
    def status_id(self) -> str:
        return self.file_id

    @staticmethod
    def deserialize(json_string: str):
        return FSCodeDetectedMessage(**json.loads(json_string))
