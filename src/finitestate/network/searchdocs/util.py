import json
import typing

IT_CATEGORIES = [
    "Desktop Computer/Server",
    "Laptop",
    "Workstation",
    "Tablet",
    "Server",
    "Smartphone"
]


def get_device_product_display_name(doc):
    """
    Returns the canonical product display name of a device, using the information from a search document (Algolia).

    :arg typing.Union[typing.Dict[str, typing.Any], str] doc: A dictionary or JSON string
    :rtype: str

    >>> get_device_product_display_name({ 'product_manufacturer': {'name': 'Cisco'}, 'product': {'name': 'Cisco Router'} })
    'Cisco Router'
    >>> get_device_product_display_name({ 'product_manufacturer': {'name': 'Cisco'}, 'product': {'name': 'Router'} })
    'Cisco Router'
    >>> get_device_product_display_name({ 'product_manufacturer': {'name': 'Netgear'}, 'product_family': {'name': 'Wireless Router'} })
    'Netgear Wireless Router'
    >>> get_device_product_display_name({ 'product_manufacturer': {'name': 'Netgear'}, 'product_family': {'name': 'Netgear Wireless Router'} })
    'Netgear Wireless Router'
    >>> get_device_product_display_name({ 'product_manufacturer': {'name': 'Google'}, 'product_category': {'name': 'Smartphone'}, 'operating_system': {'name': 'Android'} })
    'Google Android Smartphone'
    >>> get_device_product_display_name({ 'product_manufacturer': {'name': 'Google'}, 'product_category': {'name': 'Smartphone'}, 'operating_system': {'name': 'Google Android'} })
    'Google Android Smartphone'
    >>> get_device_product_display_name({ 'product_manufacturer': {'name': 'Nest'}, 'product_category': {'name': 'Smart Thermostat'}, 'operating_system': {'name': 'Google Android'} })
    'Nest Smart Thermostat'
    >>> get_device_product_display_name({ 'product_manufacturer': {'name': 'Nest'}, 'operating_system': {'name': 'Linux'} })
    'Nest Linux device'
    >>> get_device_product_display_name({'product_category': {'name': 'Smartphone'}, 'operating_system': {'name': 'iOS'} })
    'iOS Smartphone'
    >>> get_device_product_display_name({'product_category': {'name': 'Smartphone'} })
    'Smartphone'
    >>> get_device_product_display_name({'oui': {'vendor_name': 'Intel'}, 'operating_system': {'name': 'Linux'}})
    'Intel Linux device'
    >>> get_device_product_display_name({'oui': {'vendor_name': 'Intel'}})
    'Intel device'
    >>> get_device_product_display_name('{"oui": {"vendor_name": "Intel"}}')
    'Intel device'
    """

    if isinstance(doc, str):
        doc = json.loads(doc)

    search_doc = (doc or {})

    brand = (search_doc.get('product_manufacturer') or {}).get('name')
    model = (search_doc.get('product') or {}).get('name')
    family = (search_doc.get('product_family') or {}).get('name')
    category = (search_doc.get('product_category') or {}).get('name')
    os = (search_doc.get('operating_system') or {}).get('name')
    oui = (search_doc.get('oui') or {}).get('vendor_name')

    if model and brand:
        if brand.lower() in model.lower():
            return model
        else:
            return '{brand} {model}'.format(brand=brand, model=model)
    elif brand and family:
        if brand.lower() in family.lower():
            return family
        else:
            return '{brand} {family}'.format(brand=brand, family=family)
    elif os and brand and category and category in IT_CATEGORIES:
        if brand.lower() in os.lower():
            return '{os} {category}'.format(os=os, category=category)
        else:
            return '{brand} {os} {category}'.format(brand=brand, os=os, category=category)
    elif brand and category and category not in IT_CATEGORIES:
        return '{brand} {category}'.format(brand=brand, category=category)
    elif os and brand:
        if brand.lower() in os.lower():
            return '{os} device'.format(os=os)
        else:
            return '{brand} {os} device'.format(brand=brand, os=os)
    elif brand:
        return '{brand} device'.format(brand=brand)
    elif os and category and category in IT_CATEGORIES:
        return '{os} {category}'.format(os=os, category=category)
    elif category and category in IT_CATEGORIES:
        return category
    elif oui and os:
        return '{oui} {os} device'.format(oui=oui, os=os)
    elif oui:
        return '{oui} device'.format(oui=oui)
    elif os:
        return '{os} device'.format(os=os)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
