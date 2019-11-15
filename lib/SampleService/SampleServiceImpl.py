# -*- coding: utf-8 -*-
#BEGIN_HEADER
import logging
import os

from installed_clients.KBaseReportClient import KBaseReport
#END_HEADER


class SampleService:
    '''
    Module Name:
    SampleService

    Module Description:
    A KBase module: SampleService

Handles creating, updating, retriving samples and linking data to samples.
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa
    VERSION = "0.0.1"
    GIT_URL = "https://github.com/mrcreosote/sample_service.git"
    GIT_COMMIT_HASH = "c63c061ffbc6f2aef594a7fbdcd07badd8bf4c2a"

    #BEGIN_CLASS_HEADER
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.callback_url = os.environ['SDK_CALLBACK_URL']
        self.shared_folder = config['scratch']
        logging.basicConfig(format='%(created)s %(levelname)s: %(message)s',
                            level=logging.INFO)
        #END_CONSTRUCTOR
        pass


    def create_sample(self, ctx, sample):
        """
        Create a new sample or a sample version.
        If Sample.id is null, a new Sample is created along with a new ID.
        Otherwise, a new version of Sample.id is created. If Sample.id does not exist, an error
          is returned.
        :param sample: instance of type "Sample" (A Sample, consisting of a
           tree of subsamples and replicates. id - the ID of the sample.
           single_node - the node in a single node tree.) -> structure:
           parameter "id" of type "sample_id" (A Sample ID. Must be globally
           unique. Always assigned by the Sample service.), parameter
           "single_node" of type "SampleNode" (A node in a sample tree. id -
           the ID of the node. meta_controlled - metadata restricted by the
           sample controlled vocabulary and validators. meta_user -
           unrestricted metadata.) -> structure: parameter "id" of type
           "node_id" (A SampleNode ID. Must be unique within a Sample,
           contain only the characters [a-zA-Z0-9_], and be less than 255
           characters.), parameter "meta_controlled" of type "metadata"
           (Metadata attached to a sample.) -> mapping from type
           "metadata_key" (A key in a metadata key/value pair. Less than 1000
           unicode characters.) to tuple of size 2: type "metadata_value" (A
           value in a metadata key/value pair. Less than 1000 unicode
           characters.), type "units" (Units for a quantity, e.g. km, mol/g,
           ppm, etc. 'None' for unitless quantities. Less than 50 unicode
           characters.), parameter "meta_user" of type "metadata" (Metadata
           attached to a sample.) -> mapping from type "metadata_key" (A key
           in a metadata key/value pair. Less than 1000 unicode characters.)
           to tuple of size 2: type "metadata_value" (A value in a metadata
           key/value pair. Less than 1000 unicode characters.), type "units"
           (Units for a quantity, e.g. km, mol/g, ppm, etc. 'None' for
           unitless quantities. Less than 50 unicode characters.)
        :returns: instance of type "SampleAddress" (A Sample ID and version.
           id - the ID of the sample. version - the version of the sample.)
           -> structure: parameter "id" of type "sample_id" (A Sample ID.
           Must be globally unique. Always assigned by the Sample service.),
           parameter "version" of type "version" (The version of a sample.
           Always > 0.)
        """
        # ctx is the context object
        # return variables are: address
        #BEGIN create_sample
        #END create_sample

        # At some point might do deeper type checking...
        if not isinstance(address, dict):
            raise ValueError('Method create_sample return value ' +
                             'address is not type dict as required.')
        # return the results
        return [address]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
