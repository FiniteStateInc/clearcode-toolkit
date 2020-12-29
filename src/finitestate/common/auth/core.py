from abc import ABC


class HttpAuthorizationProvider(ABC):
    def get(self) -> str:
        """
        Returns an authorization value that can be used in an HTTP request header
        See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Authorization

        Returns: An authorization string formatted like "Authorization: <type> <credentials>"
        """
        raise NotImplementedError()
