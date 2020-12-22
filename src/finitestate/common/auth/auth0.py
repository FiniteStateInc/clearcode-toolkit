import datetime
import http.client
import json

from finitestate.common.aws.secrets import get_secret
from finitestate.common.dateutil import utcnow
from .core import HttpAuthorizationProvider


def get_client_credentials(*, auth0_domain: str = 'finitestate.auth0.com', api_identifier: str, auth0_client_id: str, auth0_client_secret: str) -> dict:
    """
    Retrieves credentials for an Auth0 client

    Args:
        auth0_domain: The Auth0 domain
        api_identifier: The Auth0 API identifier
        auth0_client_id: The client id
        auth0_client_secret: The client secret

    Returns:
        A dictionary with the access_token, token_type, and expires_in fields.
    """
    conn = http.client.HTTPSConnection(auth0_domain)

    payload = {
        'client_id': auth0_client_id,
        'client_secret': auth0_client_secret,
        'audience': api_identifier,
        'grant_type': 'client_credentials',
    }

    headers = {
        'content-type': 'application/json'
    }

    conn.request('POST', '/oauth/token', json.dumps(payload), headers)

    res = conn.getresponse()

    return json.loads(res.read())


def get_client_credentials_from_secret(*, secret_name: str, auth0_domain: str = 'finitestate.auth0.com',  api_identifier: str) -> dict:
    """
    Retrieves credentials for an Auth0 client using an AWS secret

    Args:
        secret_name: The name of the AWS secret that contains the Auth0 client id and secret.
        auth0_domain: The Auth0 domain
        api_identifier: The Auth0 API identifier

    Returns:
        A dictionary with the access_token, token_type, and expires_in fields.
    """
    secret = get_secret(secret_name=secret_name)

    return get_client_credentials(
        api_identifier=api_identifier,
        auth0_client_id=secret['client_id'],
        auth0_client_secret=secret['client_secret']
    )


def format_auth_header_from_credentials(credentials: dict) -> str:
    return "{token_type} {access_token}".format(**credentials)


class AwsSecretAuth0BearerTokenAuthorizationProvider(HttpAuthorizationProvider):
    def __init__(self, *, aws_secret_name: str, auth0_api_identifier: str):
        self.aws_secret_name = aws_secret_name
        self.auth0_api_identifier = auth0_api_identifier

    def _get_credentials(self) -> dict:
        return get_client_credentials_from_secret(secret_name=self.aws_secret_name, api_identifier=self.auth0_api_identifier)

    def get(self) -> str:
        return format_auth_header_from_credentials(self._get_credentials())


class CachingAwsSecretAuth0BearerTokenAuthorizationProvider(AwsSecretAuth0BearerTokenAuthorizationProvider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.creds: dict = None
        self.expiry: datetime.datetime = None

    def get(self) -> str:
        if not self.creds or (self.expiry is None or self.expiry <= utcnow()):
            request_time = utcnow()
            self.creds = self._get_credentials()
            self.expiry = request_time + datetime.timedelta(seconds=self.creds['expires_in'])

        return format_auth_header_from_credentials(self.creds)

