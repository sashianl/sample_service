# -*- coding: utf-8 -*-
#BEGIN_HEADER
# TODO TESTS
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
    VERSION = "0.1.0-alpha1"
    GIT_URL = "https://github.com/mrcreosote/sample_service.git"
    GIT_COMMIT_HASH = "ab5442e49f1447457426d07dad1b076c29d9cdf7"

    #BEGIN_CLASS_HEADER
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        
        #END_CONSTRUCTOR
        pass


    def create_sample(self, ctx, params):
        """
        Create a new sample or a sample version.
        :param params: instance of type "CreateSampleParams" (Parameters for
           creating a sample. If Sample.id is null, a new Sample is created
           along with a new ID. Otherwise, a new version of Sample.id is
           created. If Sample.id does not exist, an error is returned. Any
           incoming version or timestamp in the incoming sample is ignored.
           sample - the sample to save. prior_version - if non-null, ensures
           that no other sample version is saved between prior_version and
           the version that is created by this save. If this is not the case,
           the sample will fail to save.) -> structure: parameter "sample" of
           type "Sample" (A Sample, consisting of a tree of subsamples and
           replicates. id - the ID of the sample. node_tree - the tree(s) of
           sample nodes in the sample. The the roots of all trees must be
           BioReplicate nodes. All the BioReplicate nodes must be at the
           start of the list, and all child nodes must occur before their
           parents in the list. name - the name of the sample. Must be less
           than 255 characters. save_date - the date the sample version was
           saved. version - the version of the sample.) -> structure:
           parameter "id" of type "sample_id" (A Sample ID. Must be globally
           unique. Always assigned by the Sample service.), parameter
           "node_tree" of list of type "SampleNode" (A node in a sample tree.
           id - the ID of the node. parent - the id of the parent node for
           the current node. BioReplicate nodes, and only BioReplicate nodes,
           do not have a parent. type - the type of the node. meta_controlled
           - metadata restricted by the sample controlled vocabulary and
           validators. meta_user - unrestricted metadata.) -> structure:
           parameter "id" of type "node_id" (A SampleNode ID. Must be unique
           within a Sample and be less than 255 characters.), parameter
           "parent" of type "node_id" (A SampleNode ID. Must be unique within
           a Sample and be less than 255 characters.), parameter "type" of
           type "samplenode_type" (The type of a sample node. One of:
           BioReplicate - a biological replicate. Always at the top of the
           sample tree. TechReplicate - a technical replicate. SubSample - a
           sub sample that is not a technical replicate.), parameter
           "meta_controlled" of type "metadata" (Metadata attached to a
           sample. The UnspecifiedObject map values MUST be a primitive type
           - either int, float, or string.) -> mapping from type
           "metadata_key" (A key in a metadata key/value pair. Less than 1000
           unicode characters.) to mapping from type "metadata_value_key" (A
           key for a value associated with a piece of metadata. Less than
           1000 unicode characters. Examples: units, value, species) to
           unspecified object, parameter "meta_user" of type "metadata"
           (Metadata attached to a sample. The UnspecifiedObject map values
           MUST be a primitive type - either int, float, or string.) ->
           mapping from type "metadata_key" (A key in a metadata key/value
           pair. Less than 1000 unicode characters.) to mapping from type
           "metadata_value_key" (A key for a value associated with a piece of
           metadata. Less than 1000 unicode characters. Examples: units,
           value, species) to unspecified object, parameter "name" of type
           "sample_name" (A sample name. Must be less than 255 characters.),
           parameter "save_date" of type "timestamp" (A timestamp in epoch
           milliseconds.), parameter "version" of type "version" (The version
           of a sample. Always > 0.), parameter "prior_version" of Long
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
        address = {'id': 'foo', 'version': 1}
        #END create_sample

        # At some point might do deeper type checking...
        if not isinstance(address, dict):
            raise ValueError('Method create_sample return value ' +
                             'address is not type dict as required.')
        # return the results
        return [address]

    def get_sample(self, ctx, params):
        """
        Get a sample. If the version is omitted the most recent sample is returned.
        :param params: instance of type "GetSampleParams" (GetSample
           parameters id - the ID of the sample to retrieve. version - the
           version of the sample to retrieve, or the most recent sample if
           omitted.) -> structure: parameter "id" of type "sample_id" (A
           Sample ID. Must be globally unique. Always assigned by the Sample
           service.), parameter "version" of type "version" (The version of a
           sample. Always > 0.)
        :returns: instance of type "Sample" (A Sample, consisting of a tree
           of subsamples and replicates. id - the ID of the sample. node_tree
           - the tree(s) of sample nodes in the sample. The the roots of all
           trees must be BioReplicate nodes. All the BioReplicate nodes must
           be at the start of the list, and all child nodes must occur before
           their parents in the list. name - the name of the sample. Must be
           less than 255 characters. save_date - the date the sample version
           was saved. version - the version of the sample.) -> structure:
           parameter "id" of type "sample_id" (A Sample ID. Must be globally
           unique. Always assigned by the Sample service.), parameter
           "node_tree" of list of type "SampleNode" (A node in a sample tree.
           id - the ID of the node. parent - the id of the parent node for
           the current node. BioReplicate nodes, and only BioReplicate nodes,
           do not have a parent. type - the type of the node. meta_controlled
           - metadata restricted by the sample controlled vocabulary and
           validators. meta_user - unrestricted metadata.) -> structure:
           parameter "id" of type "node_id" (A SampleNode ID. Must be unique
           within a Sample and be less than 255 characters.), parameter
           "parent" of type "node_id" (A SampleNode ID. Must be unique within
           a Sample and be less than 255 characters.), parameter "type" of
           type "samplenode_type" (The type of a sample node. One of:
           BioReplicate - a biological replicate. Always at the top of the
           sample tree. TechReplicate - a technical replicate. SubSample - a
           sub sample that is not a technical replicate.), parameter
           "meta_controlled" of type "metadata" (Metadata attached to a
           sample. The UnspecifiedObject map values MUST be a primitive type
           - either int, float, or string.) -> mapping from type
           "metadata_key" (A key in a metadata key/value pair. Less than 1000
           unicode characters.) to mapping from type "metadata_value_key" (A
           key for a value associated with a piece of metadata. Less than
           1000 unicode characters. Examples: units, value, species) to
           unspecified object, parameter "meta_user" of type "metadata"
           (Metadata attached to a sample. The UnspecifiedObject map values
           MUST be a primitive type - either int, float, or string.) ->
           mapping from type "metadata_key" (A key in a metadata key/value
           pair. Less than 1000 unicode characters.) to mapping from type
           "metadata_value_key" (A key for a value associated with a piece of
           metadata. Less than 1000 unicode characters. Examples: units,
           value, species) to unspecified object, parameter "name" of type
           "sample_name" (A sample name. Must be less than 255 characters.),
           parameter "save_date" of type "timestamp" (A timestamp in epoch
           milliseconds.), parameter "version" of type "version" (The version
           of a sample. Always > 0.)
        """
        # ctx is the context object
        # return variables are: sample
        #BEGIN get_sample
        #END get_sample

        # At some point might do deeper type checking...
        if not isinstance(sample, dict):
            raise ValueError('Method get_sample return value ' +
                             'sample is not type dict as required.')
        # return the results
        return [sample]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
