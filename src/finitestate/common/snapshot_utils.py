import copy
import json


def snapshot_is_different(existing_document, new_document, fields_to_ignore=None):
    """ Return true if different, false if the same """
    # remove any fields to ignore (e.g. timestamps, metadata)
    if fields_to_ignore is None:
        fields_to_ignore = []

    existing = copy.deepcopy(existing_document)
    if existing:
        for field_to_ignore in fields_to_ignore:
            if field_to_ignore in existing.keys():
                del existing[field_to_ignore]
    else:
        existing = {}

    new = copy.deepcopy(new_document)
    if new:
        for field_to_ignore in fields_to_ignore:
            if field_to_ignore in new.keys():
                del new[field_to_ignore]
    else:
        new = {}

    if json.dumps(existing, sort_keys=True) != json.dumps(new, sort_keys=True):
        return True
    return False
