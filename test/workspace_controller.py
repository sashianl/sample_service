'''
Q&D Utility to run a Workspace server for the purposes of testing.

Initializes a GridFS backend and does not support handles.
'''

import os as _os
import shutil as _shutil
import subprocess as _subprocess
import tempfile as _tempfile
import time as _time
from pathlib import Path as _Path

import requests as _requests

from configparser import ConfigParser as _ConfigParser
from installed_clients.WorkspaceClient import Workspace as _Workspace
from installed_clients.baseclient import ServerError as _ServerError

from core import test_utils as _test_utils
from core.test_utils import TestException as _TestException
from mongo_controller import MongoController as _MongoController

_WS_CLASS = 'us.kbase.workspace.WorkspaceServer'
_JARS_FILE = _Path(__file__).resolve().parent.joinpath('wsjars')


class WorkspaceController:
    """
    The main Workspace controller class. The Workspace will allow users with the KBase Auth
    service WS_READ_ADMIN role to use read-only administration methods and WS_FULL_ADMIN role
    to use all administration methods.

    Attributes:
    version - the version of the Workspace service
    port - the port for the Workspace service.
    temp_dir - the location of the Workspace data and logs.
    """

    # TODO This code likely belongs somewhere else. Not quite sure where though, maybe in WS repo.
    # TODO This code is similar to the auth controller code, DRY it up?

    def __init__(
            self,
            jars_dir: _Path,
            mongo_controller: _MongoController,
            mongo_db: str,
            mongo_type_db: str,
            auth_url: str,
            root_temp_dir: _Path):
        '''
        Create and start a new Workspace service. An unused port will be selected for the server.

        :param jars_dir: The path to the lib/jars dir of the KBase Jars repo
            (https://github.com/kbase/jars), e.g /path_to_repo/lib/jars.
        :param mongo_controller: A MongoDB controller.
        :param mongo_db: The database in which to store Workspace data.
        :param mongo_type_db: The database in which to store Workspace type specifications.
        :param auth_url: The root url of an instance of the KBase auth service.
        :param root_temp_dir: A temporary directory in which to store Auth data and log files.
            The files will be stored inside a child directory that is unique per invocation.
        '''
        if not jars_dir or not _os.access(jars_dir, _os.X_OK):
            raise _TestException('jars_dir {} does not exist or is not executable.'
                                 .format(jars_dir))
        if not mongo_controller:
            raise _TestException('mongo_controller must be provided')
        if not mongo_db:
            raise _TestException('mongo_db must be provided')
        if not mongo_type_db:
            raise _TestException('mongo_type_db must be provided')
        if not auth_url:
            raise _TestException('auth_url must be provided')
        if not root_temp_dir:
            raise _TestException('root_temp_dir is None')

        self._mongo = mongo_controller
        self._db = mongo_db
        jars_dir = jars_dir.resolve()
        class_path = self._get_class_path(jars_dir)

        # make temp dirs
        root_temp_dir = root_temp_dir.absolute()
        _os.makedirs(root_temp_dir, exist_ok=True)
        self.temp_dir = _Path(
            _tempfile.mkdtemp(prefix='WorkspaceController-', dir=str(root_temp_dir)))
        ws_temp_dir = self.temp_dir.joinpath('temp_files')
        _os.makedirs(ws_temp_dir)

        configfile = self._create_deploy_cfg(
            self.temp_dir,
            ws_temp_dir,
            f'localhost:{self._mongo.port}',
            mongo_db,
            mongo_type_db,
            auth_url)
        newenv = _os.environ.copy()
        newenv['KB_DEPLOYMENT_CONFIG'] = configfile

        self.port = _test_utils.find_free_port()

        command = ['java', '-classpath', class_path, _WS_CLASS, str(self.port)]

        self._wslog = self.temp_dir / 'ws.log'
        self._outfile = open(self._wslog, 'w')

        self._proc = _subprocess.Popen(
            command, stdout=self._outfile, stderr=_subprocess.STDOUT, env=newenv)

        ws = _Workspace(f'http://localhost:{self.port}')
        for count in range(40):
            err = None
            _time.sleep(1)  # wait for server to start
            try:
                self.version = ws.ver()
                break
            except (_ServerError, _requests.exceptions.ConnectionError) as se:
                err = _TestException(se.args[0])
                err.__cause__ = se
        if err:
            print('Error starting workspace service. Dumping logs and throwing error')
            self._print_ws_logs()
            raise err
        self.startup_count = count + 1

    def _get_class_path(self, jars_dir: _Path):
        cp = []
        with open(_JARS_FILE) as jf:
            for l in jf:
                if l.strip() and not l.startswith('#'):
                    p = jars_dir.joinpath(l.strip())
                    if not p.is_file():
                        raise _TestException(f'Required jar does not exist: {p}')
                    cp.append(str(p))
        return ':'.join(cp)

    def _create_deploy_cfg(
            self,
            temp_dir,
            ws_temp_dir,
            mongo_host,
            mongo_db,
            mongo_type_db,
            auth_url):
        cp = _ConfigParser()
        cp['Workspace'] = {
            'mongodb-host': mongo_host,
            'mongodb-database': mongo_db,
            'mongodb-type-database': mongo_type_db,
            'backend-type': 'GridFS',
            'auth-service-url': auth_url + '/api/legacy/KBase',
            'auth-service-url-allow-insecure': 'true',
            'auth2-service-url': auth_url + '/',  # TODO WS should not be necessary
            'temp-dir': str(ws_temp_dir),
            'ignore-handle-service': 'true',
            'auth2-ws-admin-read-only-roles': 'WS_READ_ADMIN',
            'auth2-ws-admin-full-roles': 'WS_FULL_ADMIN'
        }
        f = temp_dir / 'test.cfg'
        with open(f, 'w') as inifile:
            cp.write(inifile)
        return f

    def clear_db(self):
        '''
        Remove all data, but not indexes, from the database. Do not remove any installed types.
        '''
        self._mongo.clear_database(self._db)

    def destroy(self, delete_temp_files: bool = True, dump_logs_to_stdout: bool = True):
        '''
        Shut down the server and optionally delete any files generated.

        :param delete_temp_files: if true, delete all the temporary files generated as part of
            running the server.
        :param dump_logs_to_stdout: Write the contents of the workspace log file to stdout.
            This is useful in the context of 3rd party CI services, where the log file is not
            necessarily accessible.
        '''
        if self._proc:
            self._proc.terminate()
        self._print_ws_logs(dump_logs_to_stdout=dump_logs_to_stdout)
        if delete_temp_files and self.temp_dir:
            _shutil.rmtree(self.temp_dir)

    # closes logfile
    def _print_ws_logs(self, dump_logs_to_stdout=True):
        if self._outfile:
            self._outfile.close()
            if dump_logs_to_stdout:
                with open(self._wslog) as f:
                    for l in f:
                        print(l)
