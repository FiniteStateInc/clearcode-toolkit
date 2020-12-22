from finitestate.common.aws.s3 import slurp_object_from_uri
import json

class Event:
    def __init__(self, event_id, event_type, organization_id, aggregate_id, occurred_at):
        self.event_id        = event_id
        self.event_type      = event_type
        self.organization_id = organization_id
        self.aggregate_id    = aggregate_id
        self.occurred_at     = occurred_at

    @classmethod
    def from_payload_uri(cls, uri):
        obj = json.loads(slurp_object_from_uri(uri))
        event_metadata = obj["payload"]["event_metadata"]

        return Event(
            event_metadata["event_id"],
            event_metadata["event_type"],
            event_metadata["organization_id"],
            event_metadata["aggregate_id"],
            event_metadata["occurred_at"]
        )
