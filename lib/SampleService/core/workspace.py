'''
Methods for accessing workspace data.
'''

from enum import IntEnum

from installed_clients.WorkspaceClient import Workspace
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.errors import IllegalParameterError as _IllegalParameterError
from SampleService.core.errors import UnauthorizedError as _UnauthorizedError


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

        :param user: The user's user name.
        :param perm: The requested permission
        :param workspace_id: The ID of the workspace.
        :param upa: a workspace service UPA.
        '''
        _check_string(user, 'user')
        _not_falsy(perm, 'perm')
        if workspace_id is not None:
            wsid = workspace_id
            name = 'workspace'
            target = str(workspace_id)
        elif upa:
            wsid = self._check_upa(upa)
            name = 'upa'
            target = upa
        else:
            raise ValueError('Either an UPA or a workpace ID must be supplied')
        if wsid < 1:
            raise _IllegalParameterError(f'{wsid} is not a valid workspace ID')

        # TODO handle no such / deleted workspace, should throw IllegalParameter
        # easier to do in integration test
        p = self.ws.administer({'command': 'getPermissionsMass',
                                'params': {'workspaces': [{'id': wsid}]}})
        if p['perms'][0].get(user) not in _PERM_TO_PERM_SET[perm]:
            raise _UnauthorizedError(
                f'User {user} cannot {_PERM_TO_PERM_TEXT[perm]} {name} {target}')

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
