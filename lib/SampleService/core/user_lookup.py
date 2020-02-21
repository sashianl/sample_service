''' Look up user names in the KBase Auth service to determine if they represent existing users. '''

# this is tested in the integration tests to avoid starting up the auth server again, as
# it takes a few seconds

import logging
import requests
from typing import List, Sequence, Tuple

from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import not_falsy_in_iterable as _no_falsy_in_iterable
from SampleService.core.acls import AdminPermission


class KBaseUserLookup:
    ''' A client for contacting the KBase authentication server to verify user names. '''

    def __init__(
            self,
            auth_url: str,
            auth_token: str,
            full_admin_roles: List[str] = None,
            read_admin_roles: List[str] = None):
        '''
        Create the client.
        :param auth_url: The root url of the authentication service.
        :param auth_token: A valid token for the authentication service.
        :raises InvalidTokenError: if the token is invalid
        '''
        self._url = _not_falsy(auth_url, 'auth_url')
        if not self._url.endswith('/'):
            self._url += '/'
        self._user_url = self._url + 'api/V2/users?list='
        self._me_url = self._url + 'api/V2/me'
        self._token = _not_falsy(auth_token, 'auth_token')
        self._full_roles = set(full_admin_roles) if full_admin_roles else set()
        self._read_roles = set(read_admin_roles) if read_admin_roles else set()

        # Auth 0.4.1 needs to be deployed before this will work
        # r = requests.get(self._url, headers={'Accept': 'application/json'})
        # self._check_error(r)
        # if r.json().get('servicename') != 'Authentication Service':
        #     raise IOError(f'The service at {self._url} does not appear to be the KBase ' +
        #                   'Authentication Service')

        # could use the server time to adjust for clock skew, probably not worth the trouble

        # check token is valid
        r = requests.get(
            self._user_url, headers={'Accept': 'application/json', 'authorization': self._token})
        self._check_error(r)
        # need to test this with a mock. YAGNI for now.
        # if r.json() != {}:
        #    raise ValueError(f'Invalid auth url, expected empty map, got {r.text}')

    def _check_error(self, r):
        if r.status_code != 200:
            try:
                j = r.json()
            except Exception:
                err = ('Non-JSON response from KBase auth server, status code: ' +
                       str(r.status_code))
                logging.getLogger(__name__).info('%s, response:\n%s', err, r.text)
                raise IOError(err)
            # assume that if we get json then at least this is the auth server and we can
            # rely on the error structure.
            if j['error'].get('appcode') == 10020:  # Invalid token
                raise InvalidTokenError('KBase auth server reported token is invalid.')
            if j['error'].get('appcode') == 30010:  # Invalid username
                raise InvalidUserError(
                    'The KBase auth server is being very assertive about ' +
                    'one of the usernames being illegal: ' + j['error']['message'])
            # don't really see any other error codes we need to worry about - maybe disabled?
            # worry about it later.
            raise IOError('Error from KBase auth server: ' + j['error']['message'])

    def are_valid_users(self, usernames: Sequence[str]) -> List[str]:
        '''
        Check whether users exist in the authentication service.

        :param users: the users to check.
        :returns: A list of users that have valid usernames but do not exist in the authentication
            service.
        :raises InvalidTokenError: if the token has expired
        :raises InvalidUserError: if any of the user names are invalid user names.
        '''
        if usernames is None:
            raise ValueError('usernames cannot be None')
        if not usernames:
            return []
        _no_falsy_in_iterable(usernames, 'usernames')

        r = requests.get(self._user_url + ','.join(usernames),
                         headers={'Authorization': self._token})
        self._check_error(r)
        good_users = r.json()
        # TODO ACL cache
        return [u for u in usernames if u not in good_users]

    def is_admin(self, token: str) -> Tuple[AdminPermission, str]:
        '''
        Check whether a user is a service administrator.

        :param token: The user's token.
        :returns: A tuple consisting of an enum indicating the user's administration permissions,
          if any, and the username.
        '''
        # TODO ACL cache admin users
        # TODO CODE should regex the token to check for \n etc., but the SDK has already checked it
        _not_falsy(token, 'token')
        r = requests.get(self._me_url, headers={'Authorization': token})
        self._check_error(r)
        j = r.json()
        return self._get_role(j['customroles']), j['user']

    def _get_role(self, roles):
        r = set(roles)
        if r & self._full_roles:
            return AdminPermission.FULL
        if r & self._read_roles:
            return AdminPermission.READ
        return AdminPermission.NONE


class AuthenticationError(Exception):
    ''' An error thrown from the authentication service. '''


class InvalidTokenError(AuthenticationError):
    ''' An error thrown when a token is invalid. '''


class InvalidUserError(AuthenticationError):
    ''' An error thrown when a user name is invalid. '''
