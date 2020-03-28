from pytest import raises
from unittest.mock import create_autospec

from installed_clients.WorkspaceClient import Workspace
from installed_clients.baseclient import ServerError
from SampleService.core.workspace import WS, WorkspaceAccessType, UPA
from core.test_utils import assert_exception_correct
from SampleService.core.errors import UnauthorizedError
from SampleService.core.errors import MissingParameterError
from SampleService.core.errors import IllegalParameterError
from SampleService.core.errors import NoSuchWorkspaceDataError
from SampleService.core.errors import NoSuchUserError

# these tests mock the workspace client, so integration tests are important to check for
# incompatible changes in the workspace api


def test_upa_init():
    _upa_init_str('1/1/1', 1, 1, 1, '1/1/1')
    _upa_init_str('64/790/17895101', 64, 790, 17895101, '64/790/17895101')

    _upa_init_int(1, 1, 1, 1, 1, 1, '1/1/1')
    _upa_init_int(89, 356, 2, 89, 356, 2, '89/356/2')


def test_upa_init_all_args():
    u = UPA(upa='6/3/9', wsid=7, objid=4, version=10)

    assert u.wsid == 6
    assert u.objid == 3
    assert u.version == 9
    assert str(u) == '6/3/9'


def _upa_init_str(str_, wsid, objid, ver, outstr):
    u = UPA(str_)

    assert u.wsid == wsid
    assert u.objid == objid
    assert u.version == ver
    assert str(u) == outstr


def _upa_init_int(wsid, objid, ver, wside, objide, vere, outstr):
    u = UPA(wsid=wsid, objid=objid, version=ver)

    assert u.wsid == wside
    assert u.objid == objide
    assert u.version == vere
    assert str(u) == outstr


def test_upa_init_fail():
    with raises(Exception) as got:
        UPA()
    assert_exception_correct(got.value, IllegalParameterError('Illegal workspace ID: None'))

    _upa_init_str_fail('1', IllegalParameterError('1 is not a valid UPA'))
    _upa_init_str_fail('1/2', IllegalParameterError('1/2 is not a valid UPA'))
    _upa_init_str_fail('1/2/3/5', IllegalParameterError('1/2/3/5 is not a valid UPA'))
    _upa_init_str_fail('1/2/3/', IllegalParameterError('1/2/3/ is not a valid UPA'))
    _upa_init_str_fail('/1/2/3', IllegalParameterError('/1/2/3 is not a valid UPA'))
    _upa_init_str_fail('f/2/3', IllegalParameterError('f/2/3 is not a valid UPA'))
    _upa_init_str_fail('1/f/3', IllegalParameterError('1/f/3 is not a valid UPA'))
    _upa_init_str_fail('1/2/f', IllegalParameterError('1/2/f is not a valid UPA'))
    _upa_init_str_fail('0/2/3', IllegalParameterError('0/2/3 is not a valid UPA'))
    _upa_init_str_fail('1/0/3', IllegalParameterError('1/0/3 is not a valid UPA'))
    _upa_init_str_fail('1/2/0', IllegalParameterError('1/2/0 is not a valid UPA'))
    _upa_init_str_fail('-24/2/3', IllegalParameterError('-24/2/3 is not a valid UPA'))
    _upa_init_str_fail('1/-42/3', IllegalParameterError('1/-42/3 is not a valid UPA'))
    _upa_init_str_fail('1/2/-10677810', IllegalParameterError('1/2/-10677810 is not a valid UPA'))

    _upa_init_int_fail(None, 2, 3, IllegalParameterError('Illegal workspace ID: None'))
    _upa_init_int_fail(1, None, 3, IllegalParameterError('Illegal object ID: None'))
    _upa_init_int_fail(1, 2, None, IllegalParameterError('Illegal object version: None'))
    _upa_init_int_fail(0, 2, 3, IllegalParameterError('Illegal workspace ID: 0'))
    _upa_init_int_fail(1, 0, 3, IllegalParameterError('Illegal object ID: 0'))
    _upa_init_int_fail(1, 2, 0, IllegalParameterError('Illegal object version: 0'))
    _upa_init_int_fail(-98, 2, 3, IllegalParameterError('Illegal workspace ID: -98'))
    _upa_init_int_fail(1, -6, 3, IllegalParameterError('Illegal object ID: -6'))
    _upa_init_int_fail(1, 2, -87501, IllegalParameterError('Illegal object version: -87501'))


def _upa_init_str_fail(upa, expected):
    with raises(Exception) as got:
        UPA(upa)
    assert_exception_correct(got.value, expected)


def _upa_init_int_fail(wsid, objid, ver, expected):
    with raises(Exception) as got:
        UPA(wsid=wsid, objid=objid, version=ver)
    assert_exception_correct(got.value, expected)


def test_init_fail():
    _init_fail(None, ValueError('client cannot be a value that evaluates to false'))

    wsc = create_autospec(Workspace, spec_set=True, instance=True)
    wsc.administer.side_effect = ServerError('jsonrpcerror', 24, 'poopoo')
    _init_fail(wsc, ServerError('jsonrpcerror', 24, 'poopoo'))


def _init_fail(wsc, expected):
    with raises(Exception) as got:
        WS(wsc)
    assert_exception_correct(got.value, expected)


def test_has_permission():
    _has_permission('a', None, UPA('42/65/3'), WorkspaceAccessType.READ, 42)
    _has_permission('b', 24, UPA('67/2/92'), WorkspaceAccessType.READ, 24)
    _has_permission('c', 1, None, WorkspaceAccessType.READ, 1)
    _has_permission('a', None, UPA('7/45/789'), WorkspaceAccessType.WRITE, 7)
    _has_permission('c', None, UPA('1/1/1'), WorkspaceAccessType.WRITE, 1)
    _has_permission('c', 301, None, WorkspaceAccessType.ADMIN, 301)


def test_has_permission_fail_bad_input():
    r = WorkspaceAccessType.READ
    _has_permission_fail(None, 1, None, r, MissingParameterError('user'))
    _has_permission_fail('', 1, None, r, MissingParameterError('user'))
    _has_permission_fail('b', None, None, r, ValueError(
        'Either an UPA or a workpace ID must be supplied'))
    _has_permission_fail('b', 0, None, r, IllegalParameterError('0 is not a valid workspace ID'))
    _has_permission_fail('b', 1, None, None, ValueError(
        'perm cannot be a value that evaluates to false'))


def test_has_permission_fail_unauthorized():
    r = WorkspaceAccessType.READ
    w = WorkspaceAccessType.WRITE
    a = WorkspaceAccessType.ADMIN
    _has_permission_fail('d', 1, None, r, UnauthorizedError('User d cannot read workspace 1'))
    _has_permission_fail('d', 34, None, w, UnauthorizedError(
        'User d cannot write to workspace 34'))
    _has_permission_fail('b', None, UPA('6/7/8'), w, UnauthorizedError(
        'User b cannot write to upa 6/7/8'))
    _has_permission_fail('d', 6, None, a, UnauthorizedError(
        'User d cannot administrate workspace 6'))
    _has_permission_fail('b', 74, None, a, UnauthorizedError(
        'User b cannot administrate workspace 74'))
    _has_permission_fail('a', None, UPA('890/44/1'), a, UnauthorizedError(
        'User a cannot administrate upa 890/44/1'))


def test_has_permission_fail_on_get_perms_no_workspace():
    _has_permission_fail_ws_exception(
        ServerError('JSONRPCError', -32500, 'No workspace with id 22 exists'),
        NoSuchWorkspaceDataError('No workspace with id 22 exists')
    )


def test_has_permission_fail_on_get_perms_deleted_workspace():
    _has_permission_fail_ws_exception(
        ServerError('JSONRPCError', -32500, 'Workspace 22 is deleted'),
        NoSuchWorkspaceDataError('Workspace 22 is deleted')
    )


def test_has_permission_fail_on_get_perms_server_error():
    _has_permission_fail_ws_exception(
        ServerError('JSONRPCError', -32500, "Things is f'up"),
        ServerError('JSONRPCError', -32500, "Things is f'up")
    )


def test_has_permission_fail_no_object():
    wsc = create_autospec(Workspace, spec_set=True, instance=True)

    ws = WS(wsc)
    wsc.administer.assert_called_once_with({'command': 'listModRequests'})

    wsc.administer.side_effect = [
        {'perms': [{'a': 'w', 'b': 'r', 'c': 'a'}]},
        {'infos': [None]}]

    with raises(Exception) as got:
        ws.has_permissions('b', WorkspaceAccessType.READ, upa=UPA('67/8/90'))
    assert_exception_correct(got.value, NoSuchWorkspaceDataError('Object 67/8/90 does not exist'))


def test_has_permission_fail_on_get_info_server_error():
    wsc = create_autospec(Workspace, spec_set=True, instance=True)

    ws = WS(wsc)
    wsc.administer.assert_called_once_with({'command': 'listModRequests'})

    wsc.administer.side_effect = [
        {'perms': [{'a': 'w', 'b': 'r', 'c': 'a'}]},
        ServerError('JSONRPCError', -32500, 'Thanks Obama')]

    with raises(Exception) as got:
        ws.has_permissions('b', WorkspaceAccessType.READ, upa=UPA('67/8/90'))
    assert_exception_correct(got.value, ServerError('JSONRPCError', -32500, 'Thanks Obama'))


def _has_permission(user, wsid, upa, perm, expected_wsid):
    wsc = create_autospec(Workspace, spec_set=True, instance=True)

    ws = WS(wsc)
    wsc.administer.assert_called_once_with({'command': 'listModRequests'})
    retperms = {'perms': [{'a': 'w', 'b': 'r', 'c': 'a'}]}

    if wsid:
        wsc.administer.return_value = retperms
    else:
        wsc.administer.side_effect = [retperms, {'infos': [['objinfo goes here']]}]

    ws.has_permissions(user, perm, wsid, upa)

    getperms = {'command': 'getPermissionsMass', 'params': {'workspaces': [{'id': expected_wsid}]}}
    if wsid:
        wsc.administer.assert_called_with(getperms)
    else:
        wsc.administer.assert_any_call(getperms)
        wsc.administer.assert_called_with({'command': 'getObjectInfo',
                                           'params': {'objects': [{'ref': str(upa)}],
                                                      'ignoreErrors': 1}})

    assert wsc.administer.call_count == 2 if wsid else 3


def _has_permission_fail(user, wsid, upa, perm, expected):
    with raises(Exception) as got:
        _has_permission(user, wsid, upa, perm, None)
    assert_exception_correct(got.value, expected)


def _has_permission_fail_ws_exception(ws_exception, expected):
    wsc = create_autospec(Workspace, spec_set=True, instance=True)

    ws = WS(wsc)

    wsc.administer.assert_called_once_with({'command': 'listModRequests'})

    wsc.administer.side_effect = ws_exception

    with raises(Exception) as got:
        ws.has_permissions('foo', WorkspaceAccessType.READ, 22)
    assert_exception_correct(got.value, expected)


def test_get_user_workspaces():
    _get_user_workspaces([], [], [])
    _get_user_workspaces([8, 89], [], [8, 89])
    _get_user_workspaces([], [4, 7], [4, 7])
    _get_user_workspaces([4, 66, 90, 104], [1, 45, 89], [1, 4, 45, 66, 89, 90, 104])


def _get_user_workspaces(workspaces, pub, expected):
    wsc = create_autospec(Workspace, spec_set=True, instance=True)

    ws = WS(wsc)

    wsc.administer.assert_called_once_with({'command': 'listModRequests'})

    wsc.administer.return_value = {'workspaces': workspaces, 'pub': pub}

    assert ws.get_user_workspaces('usera') == expected

    wsc.administer.assert_called_with({'command': 'listWorkspaceIDs',
                                       'user': 'usera',
                                       'params': {'perm': 'r', 'excludeGlobal': 0}})

    assert wsc.administer.call_count == 2


def test_get_user_workspaces_fail_bad_input():
    _get_user_workspaces_fail(None, MissingParameterError('user'))
    _get_user_workspaces_fail('', MissingParameterError('user'))


def _get_user_workspaces_fail(user, expected):
    wsc = create_autospec(Workspace, spec_set=True, instance=True)

    ws = WS(wsc)

    wsc.administer.assert_called_once_with({'command': 'listModRequests'})

    with raises(Exception) as got:
        ws.get_user_workspaces(user)
    assert_exception_correct(got.value, expected)


def test_get_user_workspaces_fail_no_user():
    _get_user_workspaces_fail_ws_exception(
        ServerError('JSONRPCError', -32500, 'User foo is not a valid user'),
        NoSuchUserError('User foo is not a valid user')
    )


def test_get_user_workspaces_fail_server_error():
    _get_user_workspaces_fail_ws_exception(
        ServerError('JSONRPCError', -32500, 'aw crapadoodles'),
        ServerError('JSONRPCError', -32500, 'aw crapadoodles')
    )


def _get_user_workspaces_fail_ws_exception(ws_exception, expected):
    wsc = create_autospec(Workspace, spec_set=True, instance=True)

    ws = WS(wsc)

    wsc.administer.assert_called_once_with({'command': 'listModRequests'})

    wsc.administer.side_effect = ws_exception

    with raises(Exception) as got:
        ws.get_user_workspaces('foo')
    assert_exception_correct(got.value, expected)
