'''
Methods for accessing workspace data.
'''

from enum import IntEnum
from typing import List

from installed_clients.WorkspaceClient import Workspace
from installed_clients.baseclient import ServerError as _ServerError
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.errors import IllegalParameterError as _IllegalParameterError
from SampleService.core.errors import UnauthorizedError as _UnauthorizedError
from SampleService.core.errors import NoSuchWorkspaceDataError as _NoSuchWorkspaceDataError
from SampleService.core.errors import NoSuchUserError as _NoSuchUserError


class WorkspaceAccessType(IntEnum):
    '''
    The different levels of workspace service access.
    '''
    READ = 2
    WRITE = 3
    ADMIN = 4


_PERM_TO_PERM_SET = {WorkspaceAccessType.READ: {'r', 'w', 'a'},
                     WorkspaceAccessType.WRITE: {'w', 'a'},
                     WorkspaceAccessType.ADMIN: {'a'}}

_PERM_TO_PERM_TEXT = {WorkspaceAccessType.READ: 'read',
                      WorkspaceAccessType.WRITE: 'write to',
                      WorkspaceAccessType.ADMIN: 'administrate'}


class WS:
    '''
    The workspace class.
    '''

    def __init__(self, client: Workspace):
        '''
        Create the workspace class.

        Attempts to contact the endpoint of the workspace in administration mode and does not
        catch any exceptions encountered.

        :param client: An SDK workspace client with administrator permissions.
        '''
        self.ws = _not_falsy(client, 'client')
        # check token is a valid admin token
        self.ws.administer({'command': 'listModRequests'})

    def has_permissions(
            self,
            user: str,
            perm: WorkspaceAccessType,
            workspace_id: int = None,
            upa: str = None):
        '''
        Check if a user can read a workspace resource. Exactly one of workspace_id or upa must
        be supplied - if both are supplied workspace_id takes precedence.

        The user is not checked for existence.

        :param user: The user's user name.
        :param perm: The requested permission
        :param workspace_id: The ID of the workspace.
        :param upa: a workspace service UPA.
        :raises IllegalParameterError: if the parameters are incorrect, such as a missing user
            or improper UPA.
        :raises UnauthorizedError: if the user doesn't have the requested permission.
        :raises NoSuchWorkspaceDataError: if the workspace or UPA doesn't exist.
        '''
        _check_string(user, 'user')
        _not_falsy(perm, 'perm')
        if workspace_id is not None:
            wsid = workspace_id
            name = 'workspace'
            target = str(workspace_id)
            upa = None
        elif upa:
            wsid = self._check_upa(upa)
            name = 'upa'
            target = upa
        else:
            raise ValueError('Either an UPA or a workpace ID must be supplied')
        if wsid < 1:
            raise _IllegalParameterError(f'{wsid} is not a valid workspace ID')

        try:
            p = self.ws.administer({'command': 'getPermissionsMass',
                                    'params': {'workspaces': [{'id': wsid}]}})
        except _ServerError as se:
            # this is pretty ugly, need error codes
            if 'No workspace' in se.args[0] or 'is deleted' in se.args[0]:
                raise _NoSuchWorkspaceDataError(se.args[0]) from se
            else:
                raise
        if p['perms'][0].get(user) not in _PERM_TO_PERM_SET[perm]:
            raise _UnauthorizedError(
                f'User {user} cannot {_PERM_TO_PERM_TEXT[perm]} {name} {target}')
        if upa:
            # Allow any server errors to percolate upwards
            # theoretically the workspace could've been deleted between the last call and this
            # one, but that'll just result in a different error and is extremely unlikely to
            # happen, so don't worry about it
            ret = self.ws.administer({'command': 'getObjectInfo',
                                      'params': {'objects': [{'ref': upa}],
                                                 'ignoreErrors': 1}
                                      })
            if not ret['infos'][0]:
                raise _NoSuchWorkspaceDataError(f'Object {upa} does not exist')

    # returns ws id
    def _check_upa(self, upa):
        wsidstr = upa.split('/')
        if len(wsidstr) != 3:
            raise _IllegalParameterError(f'{upa} is not a valid UPA')
        self._get_ws_num(wsidstr[2], upa)
        self._get_ws_num(wsidstr[1], upa)
        return self._get_ws_num(wsidstr[0], upa)

    def _get_ws_num(self, int_: str, upa):
        try:
            i = int(int_)
            if i < 1:
                raise _IllegalParameterError(f'{upa} is not a valid UPA')
            return i
        except ValueError:
            raise _IllegalParameterError(f'{upa} is not a valid UPA')

    def get_user_workspaces(self, user) -> List[int]:
        '''
        Get a list of IDs of workspaces a user can read, including public workspaces.

        :param user: The username of the user whose workspaces will be returned.
        :returns: A list of workspace IDs.
        :raises IllegalParameterError: if the parameters are incorrect, such as a missing user.
        :raises NoSuchUserError: if the user does not exist.
        '''
        # May also want write / admin / no public ws
        _check_string(user, 'user')
        try:
            ids = self.ws.administer({'command': 'listWorkspaceIDs',
                                      'user': user,
                                      'params': {'perm': 'r', 'excludeGlobal': 0}})
        except _ServerError as se:
            # this is pretty ugly, need error codes
            if 'not a valid user' in se.args[0]:
                raise _NoSuchUserError(se.args[0]) from se
            else:
                raise
        return sorted(ids['workspaces'] + ids['pub'])
