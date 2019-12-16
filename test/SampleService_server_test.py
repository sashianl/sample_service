# -*- coding: utf-8 -*-
import os
import time
import unittest
from configparser import ConfigParser

from SampleService.SampleServiceImpl import SampleService
from SampleService.SampleServiceServer import MethodContext
from SampleService.authclient import KBaseAuth as _KBaseAuth

from installed_clients.WorkspaceClient import Workspace


class SampleServiceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        token = os.environ.get('KB_AUTH_TOKEN', None)
        config_file = os.environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('SampleService'):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        authServiceUrl = cls.cfg['auth-service-url']
        auth_client = _KBaseAuth(authServiceUrl)
        user_id = auth_client.get_user(token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.ctx = MethodContext(None)
        cls.ctx.update({'token': token,
                        'user_id': user_id,
                        'provenance': [
                            {'service': 'SampleService',
                             'method': 'please_never_use_it_in_production',
                             'method_params': []
                             }],
                        'authenticated': 1})
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = Workspace(cls.wsURL)
        # cls.serviceImpl = SampleService(cls.cfg)
        # suffix = int(time.time() * 1000)
        # cls.wsName = "test_SampleService_" + str(suffix)
        # ret = cls.wsClient.create_workspace({'workspace': cls.wsName})  # noqa

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    def test_create_sample(self):
        # ret = self.serviceImpl.create_sample(self.ctx, {})
        # assert ret == [{'id': 'foo', 'version': 1}]
        pass
