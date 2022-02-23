'''
Methods for accessing workspace data.
'''

from enum import IntEnum
from typing import List, Optional, Dict as _Dict, Set as _Set

from installed_clients.WorkspaceClient import Workspace
from installed_clients.baseclient import ServerError as _ServerError
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.errors import IllegalParameterError as _IllegalParameterError
from SampleService.core.errors import UnauthorizedError as _UnauthorizedError
from SampleService.core.errors import NoSuchWorkspaceDataError as _NoSuchWorkspaceDataError
from SampleService.core.errors import NoSuchUserError as _NoSuchUserError
from SampleService.core.user import UserID


class WorkspaceAccessType(IntEnum):
    '''
    The different levels of workspace service access.
    '''
    NONE = 1
    READ = 2
    WRITE = 3
    ADMIN = 4


_PERM_TO_PERM_SET: _Dict[WorkspaceAccessType, _Set[str]] = {
    WorkspaceAccessType.NONE: set(),
    WorkspaceAccessType.READ: {'r', 'w', 'a'},
    WorkspaceAccessType.WRITE: {'w', 'a'},
    WorkspaceAccessType.ADMIN: {'a'}
    }

_PERM_TO_PERM_TEXT = {WorkspaceAccessType.READ: 'read',
                      WorkspaceAccessType.WRITE: 'write to',
                      WorkspaceAccessType.ADMIN: 'administrate'
                      }


class UPA:
    '''
    A Unique Permanent Address for a workspace object, consisting of the string 'X/Y/Z' where
    X, Y and Z are integers greater than 0 and respectively the workspace ID, the object ID,
    and the object version of the object.

    str(upa) returns the X/Y/Z form of the UPA.

    :ivar wsid: The workspace ID.
    :ivar objid: The object ID.
    :ivar version: The object version.
    '''

    def __init__(self, upa: str = None, wsid: int = None, objid: int = None, version: int = None):
        '''
        Create the UPA. Requires either the upa parameter or all of the wsid, objid, and version
        parameters. If upa is supplied the other arguments are ignored.

        :param upa: The UPA as a string.
        :param wsid: The workspace ID.
        :param objid: The object ID.
        :param version: The object version.
        :raises IllegalParameterError: if the UPA is invalid.
        '''
        if upa:
            self.wsid, self.objid, self.version = self._check_upa(upa)
        else:
            for num, name in (
                    (wsid, 'workspace ID'),
                    (objid, 'object ID'),
                    (version, 'object version')):
                if not num or num < 1:
                    raise _IllegalParameterError(f'Illegal {name}: {num}')
            self.wsid = wsid
            self.objid = objid
            self.version = version

    def _check_upa(self, upa):
        upastr = upa.split('/')
        if len(upastr) != 3:
            raise _IllegalParameterError(f'{upa} is not a valid UPA')
        return (self._get_ws_num(upastr[0], upa),
                self._get_ws_num(upastr[1], upa),
                self._get_ws_num(upastr[2], upa))

    def _get_ws_num(self, int_: str, upa):
        try:
            i = int(int_)
            if i < 1:
                raise _IllegalParameterError(f'{upa} is not a valid UPA')
            return i
        except ValueError:
            raise _IllegalParameterError(f'{upa} is not a valid UPA')

    def __str__(self) -> str:
        return f'{self.wsid}/{self.objid}/{self.version}'

    def __eq__(self, other) -> bool:
        if type(self) is type(other):
            return (self.wsid, self.objid, self.version) == (
                other.wsid, other.objid, other.version)
        return False

    def __hash__(self):
        return hash((self.wsid, self.objid, self.version))


class DataUnitID:
    '''
    Represents a unit of data in the workspace, which may be a subpart of a workspace object.
    A single workspace object may have many data units.

    The DUID consists of an UPA and an arbitrary ID that denotes a portion of the object. If the
    ID is None, the data unit consists of the entire object.

    :ivar upa: The object UPA.
    :ivar dataid: The ID of the data within the object, if any.
    '''

    def __init__(self, upa: UPA, dataid: str = None):
        '''
        Create the DUID.

        :param upa: The workspace object's UPA.
        :param dataid: The id of the data within the object that this DUID references with a
            maximum of 256 characters. None if the data unit is the entire object.
        '''
        self.upa = _not_falsy(upa, 'upa')
        self.dataid = _check_string(dataid, 'dataid', max_len=256, optional=True)

    def __str__(self):
        if self.dataid:
            return f'{self.upa}:{self.dataid}'
        else:
            return str(self.upa)

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.upa, self.dataid) == (other.upa, other.dataid)
        return False

    def __hash__(self):
        return hash((self.upa, self.dataid))


class WS:
    '''
    The workspace class.
    '''

    def __init__(self, client: Workspace):
        '''
        Create the workspace class.

        Attempts to contact the endpoint of the workspace in administration mode and does not
        catch any exceptions encountered.

        :param client: An SDK workspace client with administrator read permissions.
        '''
        self._ws = _not_falsy(client, 'client')
        # check token is a valid admin token
        self._ws.administer({'command': 'listModRequests'})

    def has_permission(
            self,
            user: Optional[UserID],
            perm: WorkspaceAccessType,
            workspace_id: int = None,
            upa: UPA = None):
        '''
        Check if a user can access a workspace resource. Exactly one of workspace_id or upa must
        be supplied - if both are supplied workspace_id takes precedence.

        Beware - passing a NONE permission will not throw errors unless the object or workspace
        does not exist.

        The user is not checked for existence.

        :param user: The user's user name, or None for an anonymous user.
        :param perm: The requested permission
        :param workspace_id: The ID of the workspace.
        :param upa: a workspace service UPA.
        :raises IllegalParameterError: if the wsid is illegal.
        :raises UnauthorizedError: if the user doesn't have the requested permission.
        :raises NoSuchWorkspaceDataError: if the workspace or UPA doesn't exist.
        '''
        _not_falsy(perm, 'perm')
        if workspace_id is not None:
            wsid = workspace_id
            name = 'workspace'
            target = str(workspace_id)
            upa = None
        elif upa:
            wsid = upa.wsid
            name = 'upa'
            target = str(upa)
        else:
            raise ValueError('Either an UPA or a workpace ID must be supplied')
        if wsid < 1:
            raise _IllegalParameterError(f'{wsid} is not a valid workspace ID')

        try:
            p = self._ws.administer({'command': 'getPermissionsMass',
                                     'params': {'workspaces': [{'id': wsid}]}
                                     }
                                    )['perms'][0]
        except _ServerError as se:
            # this is pretty ugly, need error codes
            if 'No workspace' in se.args[0] or 'is deleted' in se.args[0]:
                raise _NoSuchWorkspaceDataError(se.args[0]) from se
            else:
                raise
        publicaccess = p.get('*') == 'r' and perm == WorkspaceAccessType.READ
        hasaccess = p.get(user.id) in _PERM_TO_PERM_SET[perm] if user else False
        # could optimize a bit if NONE and upa but not worth the code complication most likely
        if (perm != WorkspaceAccessType.NONE and not hasaccess and not publicaccess):
            u = f'User {user}' if user else 'Anonymous users'
            raise _UnauthorizedError(f'{u} cannot {_PERM_TO_PERM_TEXT[perm]} {name} {target}')
        if upa:
            # Allow any server errors to percolate upwards
            # theoretically the workspace could've been deleted between the last call and this
            # one, but that'll just result in a different error and is extremely unlikely to
            # happen, so don't worry about it
            ret = self._ws.administer({'command': 'getObjectInfo',
                                       'params': {'objects': [{'ref': str(upa)}],
                                                  'ignoreErrors': 1}
                                       })
            if not ret['infos'][0]:
                raise _NoSuchWorkspaceDataError(f'Object {upa} does not exist')

    def get_user_workspaces(self, user: Optional[UserID]) -> List[int]:
        '''
        Get a list of IDs of workspaces a user can read, including public workspaces.

        :param user: The username of the user whose workspaces will be returned, or null for an
            anonymous user.
        :returns: A list of workspace IDs.
        :raises NoSuchUserError: if the user does not exist.
        '''
        # May also want write / admin / no public ws
        try:
            if user:
                ids = self._ws.administer({'command': 'listWorkspaceIDs',
                                           'user': user.id,
                                           'params': {'perm': 'r', 'excludeGlobal': 0}})
            else:
                ids = self._ws.list_workspace_ids({'onlyGlobal': 1})
        except _ServerError as se:
            # this is pretty ugly, need error codes
            if 'not a valid user' in se.args[0]:
                raise _NoSuchUserError(se.args[0]) from se
            else:
                raise
        return sorted(ids['workspaces'] + ids['pub'])
