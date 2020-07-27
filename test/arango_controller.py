"""
A controller for ArangoDB useful for running tests.

Production use is not recommended.
"""
import arango
from pathlib import Path
from core.test_utils import TestException, find_free_port
import os
import tempfile
import subprocess
import time
import shutil


class ArangoController:
    """
    The main ArangoDB controller class.

    Attributes:
    port - the port for the ArangoDB service.
    temp_dir - the location of the ArangoDB data and logs.
    client - a python-arango client pointed at the server.
    """

    def __init__(self, arangoexe: Path, arangojs: Path, root_temp_dir: Path) -> None:
        '''
        Create and start a new ArangoDB database. An unused port will be selected for the server.

        :param arangoexe: The path to the ArangoDB server executable (e.g. arangod) to run.
        :param arangojs: The path to the ArangoDB javascript files
          (e.g. --javascript.startup-directory).
        :param root_temp_dir: A temporary directory in which to store ArangoDB data and log files.
            The files will be stored inside a child directory that is unique per invocation.
        '''
        arangoexe = Path(os.path.expanduser(arangoexe))
        arangojs = Path(os.path.expanduser(arangojs))
        if not arangoexe or not os.access(arangoexe, os.X_OK):
            raise TestException('arangodb executable path {} does not exist or is not executable.'
                                .format(arangoexe))
        if not arangojs or not os.path.isdir(arangojs):
            raise TestException('arangodb javascript path {} does not exist or is not a directory.'
                                .format(arangoexe))
        if not root_temp_dir:
            raise ValueError('root_temp_dir is None')

        # make temp dirs
        root_temp_dir = root_temp_dir.absolute()
        os.makedirs(root_temp_dir, exist_ok=True)
        self.temp_dir = Path(tempfile.mkdtemp(prefix='ArangoController-', dir=str(root_temp_dir)))
        data_dir = self.temp_dir.joinpath('data')
        os.makedirs(data_dir)

        self.port = find_free_port()

        command = [
            str(arangoexe),
            '--server.endpoint', f'tcp://localhost:{self.port}',
            '--configuration', 'none',
            '--database.directory', str(data_dir),
            '--javascript.startup-directory', str(arangojs),
            '--javascript.app-path', str(data_dir / 'apps'),
            '--log.file', str(self.temp_dir / 'arango.log')
            ]

        self._outfile = open(self.temp_dir.joinpath('arango.out'), 'w')

        self._proc = subprocess.Popen(command, stdout=self._outfile, stderr=subprocess.STDOUT)
        time.sleep(3)  # wait for server to start up
        self.client = arango.ArangoClient(hosts=f'http://localhost:{self.port}')
        self.client.db(verify=True)  # connect to the _system db with default creds

    def destroy(self, delete_temp_files: bool) -> None:
        """
        Shut down the ArangoDB server.

        :param delete_temp_files: delete all the ArangoDB data files and logs generated during the
            test.
        """
        if self._proc:
            self._proc.terminate()
        if self._outfile:
            self._outfile.close()
        if delete_temp_files and self.temp_dir:
            shutil.rmtree(self.temp_dir)

    def clear_database(self, db_name, drop_indexes=False):
        '''
        Remove all data from a database.

        :param db_name: the name of the db to clear.
        :param drop_indexes: drop all indexes if true, retain indexes (which will be empty) if
            false.
        '''
        if drop_indexes:
            self.client.db().delete_database(db_name)
        else:
            db = self.client.db(db_name)
            for c in db.collections():
                if not c['name'].startswith('_'):
                    # don't drop collection since that drops indexes
                    db.collection(c['name']).delete_match({})


def main():
    arangoexe = Path('~/arango/3.5.0/usr/sbin/arangod')
    arangojs = Path('~/arango/3.5.0/usr/share/arangodb3/js/')

    ac = ArangoController(arangoexe, arangojs, Path('./test_temp_can_delete'))
    print('port: ' + str(ac.port))
    print('temp_dir: ' + str(ac.temp_dir))
    db = ac.client.db()  # _system db
    db.create_database('foo')
    db = ac.client.db('foo')
    db.create_collection('bar')
    ac.clear_database('foo', True)
    input('press enter to shut down')
    ac.destroy(True)


if __name__ == '__main__':
    main()
