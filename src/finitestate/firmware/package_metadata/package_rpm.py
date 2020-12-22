import json

from .model import FSPackageMetadata
from .supplier_types import SupplierType


def standardize(file_id, rpm_data, package_supplier_info):
    # TODO: Tranlation
    return FSPackageMetadata(id=file_id,
                             name=rpm_data['name'],
                             version=rpm_data['version'],
                             release=rpm_data['release'],
                             file_name=package_supplier_info['file_name'],
                             supplier_name=package_supplier_info['supplier_name'],
                             supplier_type=str(SupplierType.rpm),
                             supplier_url=package_supplier_info['supplier_base_url'],
                             download_location=package_supplier_info['download_location'],
                             home_page=rpm_data['url'],
                             source_information=json.dumps(package_supplier_info['additional_metadata']),
                             declared_license=rpm_data['copyright'],
                             summary_description=rpm_data['summary'],
                             detailed_description=rpm_data['description'])
