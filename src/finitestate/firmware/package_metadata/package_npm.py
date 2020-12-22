import json
from .model import FSPackageMetadata
from .supplier_types import SupplierType


def standardize(file_id, npm_data, package_supplier_info):
    # TODO: Tranlation
    return FSPackageMetadata(id=file_id,
                             name=npm_data['name'],
                             version=npm_data['version'],
                             release=None,  # Perhaps this is None
                             file_name=package_supplier_info['file_name'],
                             supplier_name=package_supplier_info['supplier_name'],
                             supplier_type=str(SupplierType.npm),
                             supplier_url=package_supplier_info['supplier_base_url'],
                             download_location=package_supplier_info['download_location'],
                             home_page=npm_data.get('homepage'),
                             source_information=json.dumps(
                                 {
                                     'download_date': package_supplier_info['download_date'],
                                     'author': npm_data.get('author'),
                                     'source_repository': npm_data.get('repository'),
                                     'license_data': npm_data.get('licenses')
                                 }
                             ),
                             # Licenses may or may not be there. We need to handle the case where they're not. In that case,
                             # we should return None. Otherwise, we should return a sorted list of comma separated strings.
                             declared_license=", ".join(sorted(x.get("type", str()) for x in npm_data.get('licenses', [{}]))) or None,
                             summary_description=npm_data.get('description'),
                             detailed_description=None)
