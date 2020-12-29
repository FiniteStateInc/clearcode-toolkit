import attr


@attr.s
class FSPackageMetadata():
    id: str = attr.ib(kw_only=True)
    name: str = attr.ib(kw_only=True)
    version: str = attr.ib(kw_only=True)
    release: str = attr.ib(kw_only=True)
    file_name: str = attr.ib(kw_only=True)
    supplier_name: str = attr.ib(kw_only=True)
    supplier_type: str = attr.ib(kw_only=True)
    supplier_url: str = attr.ib(kw_only=True)
    source_information: str = attr.ib(kw_only=True)
    file_name: str = attr.ib(kw_only=True)
    download_location: str = attr.ib(kw_only=True)
    home_page: str = attr.ib(kw_only=True)
    declared_license: str = attr.ib(kw_only=True)
    summary_description: str = attr.ib(kw_only=True)
    detailed_description: str = attr.ib(kw_only=True)
