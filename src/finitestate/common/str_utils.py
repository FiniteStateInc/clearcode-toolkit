import base64


def deep_bytes_to_str(e, encoding: str = 'utf-8'):
    """ Replaces bytes with their str equivalents """
    if isinstance(e, bytes):
        try:
            return e.decode(encoding)
        except:
            return base64.b64encode(e).decode(encoding)
    if isinstance(e, dict):
        for k, v in e.items():
            e[k] = deep_bytes_to_str(v)
    if isinstance(e, list):
        for n in range(len(e)):
            e[n] = deep_bytes_to_str(e[n])
    return e