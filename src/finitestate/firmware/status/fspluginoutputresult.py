import enum


@enum.unique
class FSPluginOutputResult(enum.Enum):
    """Final status of successful plugin. All errors should be raised, not returned.

    Values:
        EXECUTED_WITH_RESULTS: Successful run and output data generated.
        EXECUTED_WITHOUT_RESULTS: Successful run, but no output data generated.
        NOT_EXECUTED_NOT_APPLICABLE: Successful run, but didn't execute because of some
            pre-condition (not applicable because of MIME type, etc).
        EXECUTED_WITH_OVERRIDES: Successful run, but overrides existed and written instead
        ERROR: Should only be returned by base class in the case of error in the plugin
        TIMEOUT: Should only be returned by base class in the case of plugin timeout
    """
    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    EXECUTED_WITH_RESULTS = enum.auto()
    EXECUTED_WITHOUT_RESULTS = enum.auto()
    NOT_EXECUTED_NOT_APPLICABLE = enum.auto()
    EXECUTED_WITH_OVERRIDES = enum.auto()
    # Status normally handled by the base class. Derived
    # classes shouldn't need to specificy these manually.
    ERROR = enum.auto()
    TIMEOUT = enum.auto()
    # Placeholder for any misbehaving process that does not supply a result, and/or results that aren't final
    NOT_SPECIFIED = enum.auto()
