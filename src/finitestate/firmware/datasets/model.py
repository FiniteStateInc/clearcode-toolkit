import attr
from enum import auto, unique

from finitestate.common.enum import AutoName


@unique
class Granularity(AutoName):
    FIRMWARE = auto()
    FILE = auto()
    FIRMWARE_FILE = auto()


@unique
class Format(AutoName):
    JSON = auto()
    JSONL = auto()
    TARGZ = auto()


@unique
class Density(AutoName):
    LOW = auto()
    HIGH = auto()


@attr.dataclass
class Dataset:
    name: str
    granularity: Granularity
    format: Format
    density: Density

    @classmethod
    def default(cls, name: str) -> 'Dataset':
        return Dataset(name=name, granularity=Granularity.FILE, format=Format.JSONL, density=Density.LOW)
