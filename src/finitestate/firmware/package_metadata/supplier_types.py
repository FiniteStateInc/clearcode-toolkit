from enum import Enum


class SupplierType(Enum):
    rpm = 'rpm'
    npm = 'npm'
    python = 'python'
    debian = 'debian'

    def __str__(self):
        return self.name
