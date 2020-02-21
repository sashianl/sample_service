# -*- coding: utf-8 -*-
#BEGIN_HEADER

from SampleService.core.config import build_samples as _build_samples
from SampleService.core.api_translation import (get_sample_address_from_object as
                                                _get_sample_address_from_object)
from SampleService.core.api_translation import get_id_from_object as _get_id_from_object
from SampleService.core.api_translation import acls_from_dict as _acls_from_dict
from SampleService.core.api_translation import acls_to_dict as _acls_to_dict
from SampleService.core.api_translation import sample_to_dict as _sample_to_dict
from SampleService.core.api_translation import create_sample_params as _create_sample_params
from SampleService.core.api_translation import check_admin as _check_admin
from SampleService.core.acls import AdminPermission as _AdminPermission

_CTX_USER = 'user_id'
_CTX_TOKEN = 'token'
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
    VERSION = "0.1.0-alpha4"
    GIT_URL = "https://github.com/mrcreosote/sample_service.git"
    GIT_COMMIT_HASH = "f9bd1e9e000ce82fc2a582e91ca368d6478b6304"

    #BEGIN_CLASS_HEADER
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self._samples, self._user_lookup = _build_samples(config)
        #END_CONSTRUCTOR
        pass

    def create_sample(self, ctx, params):
        """
        Create a new sample or a sample version.
        :param params: instance of type "CreateSampleParams" (Parameters for
           creating a sample. If Sample.id is null, a new Sample is created
           along with a new ID. Otherwise, a new version of Sample.id is
           created. If Sample.id does not exist, an error is returned. Any
           incoming user, version or timestamp in the incoming sample is
           ignored. sample - the sample to save. prior_version - if non-null,
           ensures that no other sample version is saved between
           prior_version and the version that is created by this save. If
           this is not the case, the sample will fail to save.) -> structure:
           parameter "sample" of type "Sample" (A Sample, consisting of a
           tree of subsamples and replicates. id - the ID of the sample. user
           - the user that saved the sample. node_tree - the tree(s) of
           sample nodes in the sample. The the roots of all trees must be
           BioReplicate nodes. All the BioReplicate nodes must be at the
           start of the list, and all child nodes must occur after their
           parents in the list. name - the name of the sample. Must be less
           than 255 characters. save_date - the date the sample version was
           saved. version - the version of the sample.) -> structure:
           parameter "id" of type "sample_id" (A Sample ID. Must be globally
           unique. Always assigned by the Sample service.), parameter "user"
           of type "user" (A user's username.), parameter "node_tree" of list
           of type "SampleNode" (A node in a sample tree. id - the ID of the
           node. parent - the id of the parent node for the current node.
           BioReplicate nodes, and only BioReplicate nodes, do not have a
           parent. type - the type of the node. meta_controlled - metadata
           restricted by the sample controlled vocabulary and validators.
           meta_user - unrestricted metadata.) -> structure: parameter "id"
           of type "node_id" (A SampleNode ID. Must be unique within a Sample
           and be less than 255 characters.), parameter "parent" of type
           "node_id" (A SampleNode ID. Must be unique within a Sample and be
           less than 255 characters.), parameter "type" of type
           "samplenode_type" (The type of a sample node. One of: BioReplicate
           - a biological replicate. Always at the top of the sample tree.
           TechReplicate - a technical replicate. SubSample - a sub sample
           that is not a technical replicate.), parameter "meta_controlled"
           of type "metadata" (Metadata attached to a sample. The
           UnspecifiedObject map values MUST be a primitive type - either
           int, float, string, or equivalent typedefs.) -> mapping from type
           "metadata_key" (A key in a metadata key/value pair. Less than 1000
           unicode characters.) to mapping from type "metadata_value_key" (A
           key for a value associated with a piece of metadata. Less than
           1000 unicode characters. Examples: units, value, species) to
           unspecified object, parameter "meta_user" of type "metadata"
           (Metadata attached to a sample. The UnspecifiedObject map values
           MUST be a primitive type - either int, float, string, or
           equivalent typedefs.) -> mapping from type "metadata_key" (A key
           in a metadata key/value pair. Less than 1000 unicode characters.)
           to mapping from type "metadata_value_key" (A key for a value
           associated with a piece of metadata. Less than 1000 unicode
           characters. Examples: units, value, species) to unspecified
           object, parameter "name" of type "sample_name" (A sample name.
           Must be less than 255 characters.), parameter "save_date" of type
           "timestamp" (A timestamp in epoch milliseconds.), parameter
           "version" of type "version" (The version of a sample. Always >
           0.), parameter "prior_version" of Long
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
        s, id_, pv = _create_sample_params(params)
        ret = self._samples.save_sample(s, ctx[_CTX_USER], id_, pv)
        address = {'id': str(ret[0]), 'version': ret[1]}
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
        :param params: instance of type "GetSampleParams" (get_sample
           parameters. id - the ID of the sample to retrieve. version - the
           version of the sample to retrieve, or the most recent sample if
           omitted. as_admin - get the sample regardless of ACLs as long as
           the user has administration read permissions.) -> structure:
           parameter "id" of type "sample_id" (A Sample ID. Must be globally
           unique. Always assigned by the Sample service.), parameter
           "version" of type "version" (The version of a sample. Always >
           0.), parameter "as_admin" of type "boolean" (A boolean value, 0
           for false, 1 for true.)
        :returns: instance of type "Sample" (A Sample, consisting of a tree
           of subsamples and replicates. id - the ID of the sample. user -
           the user that saved the sample. node_tree - the tree(s) of sample
           nodes in the sample. The the roots of all trees must be
           BioReplicate nodes. All the BioReplicate nodes must be at the
           start of the list, and all child nodes must occur after their
           parents in the list. name - the name of the sample. Must be less
           than 255 characters. save_date - the date the sample version was
           saved. version - the version of the sample.) -> structure:
           parameter "id" of type "sample_id" (A Sample ID. Must be globally
           unique. Always assigned by the Sample service.), parameter "user"
           of type "user" (A user's username.), parameter "node_tree" of list
           of type "SampleNode" (A node in a sample tree. id - the ID of the
           node. parent - the id of the parent node for the current node.
           BioReplicate nodes, and only BioReplicate nodes, do not have a
           parent. type - the type of the node. meta_controlled - metadata
           restricted by the sample controlled vocabulary and validators.
           meta_user - unrestricted metadata.) -> structure: parameter "id"
           of type "node_id" (A SampleNode ID. Must be unique within a Sample
           and be less than 255 characters.), parameter "parent" of type
           "node_id" (A SampleNode ID. Must be unique within a Sample and be
           less than 255 characters.), parameter "type" of type
           "samplenode_type" (The type of a sample node. One of: BioReplicate
           - a biological replicate. Always at the top of the sample tree.
           TechReplicate - a technical replicate. SubSample - a sub sample
           that is not a technical replicate.), parameter "meta_controlled"
           of type "metadata" (Metadata attached to a sample. The
           UnspecifiedObject map values MUST be a primitive type - either
           int, float, string, or equivalent typedefs.) -> mapping from type
           "metadata_key" (A key in a metadata key/value pair. Less than 1000
           unicode characters.) to mapping from type "metadata_value_key" (A
           key for a value associated with a piece of metadata. Less than
           1000 unicode characters. Examples: units, value, species) to
           unspecified object, parameter "meta_user" of type "metadata"
           (Metadata attached to a sample. The UnspecifiedObject map values
           MUST be a primitive type - either int, float, string, or
           equivalent typedefs.) -> mapping from type "metadata_key" (A key
           in a metadata key/value pair. Less than 1000 unicode characters.)
           to mapping from type "metadata_value_key" (A key for a value
           associated with a piece of metadata. Less than 1000 unicode
           characters. Examples: units, value, species) to unspecified
           object, parameter "name" of type "sample_name" (A sample name.
           Must be less than 255 characters.), parameter "save_date" of type
           "timestamp" (A timestamp in epoch milliseconds.), parameter
           "version" of type "version" (The version of a sample. Always > 0.)
        """
        # ctx is the context object
        # return variables are: sample
        #BEGIN get_sample
        id_, ver = _get_sample_address_from_object(params)
        admin = _check_admin(self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.READ,
                             # pretty annoying to test ctx.log_info is working, do it manually
                             'get_sample', ctx.log_info, skip_check=not params.get('as_admin'))
        s = self._samples.get_sample(id_, ctx[_CTX_USER], ver, admin)
        sample = _sample_to_dict(s)
        #END get_sample

        # At some point might do deeper type checking...
        if not isinstance(sample, dict):
            raise ValueError('Method get_sample return value ' +
                             'sample is not type dict as required.')
        # return the results
        return [sample]

    def get_sample_acls(self, ctx, params):
        """
        Get a sample's ACLs.
        :param params: instance of type "GetSampleACLsParams"
           (get_sample_acls parameters.) -> structure: parameter "id" of type
           "sample_id" (A Sample ID. Must be globally unique. Always assigned
           by the Sample service.)
        :returns: instance of type "SampleACLs" (Access control lists for a
           sample. Access levels include the privileges of the lower access
           levels. owner - the user that created and owns the sample. admin -
           users that can administrate (e.g. alter ACLs) the sample. write -
           users that can write (e.g. create a new version) to the sample.
           read - users that can view the sample.) -> structure: parameter
           "owner" of type "user" (A user's username.), parameter "admin" of
           list of type "user" (A user's username.), parameter "write" of
           list of type "user" (A user's username.), parameter "read" of list
           of type "user" (A user's username.)
        """
        # ctx is the context object
        # return variables are: acls
        #BEGIN get_sample_acls
        id_ = _get_id_from_object(params, required=True)
        acls_ret = self._samples.get_sample_acls(id_, ctx[_CTX_USER])
        acls = _acls_to_dict(acls_ret)
        #END get_sample_acls

        # At some point might do deeper type checking...
        if not isinstance(acls, dict):
            raise ValueError('Method get_sample_acls return value ' +
                             'acls is not type dict as required.')
        # return the results
        return [acls]

    def replace_sample_acls(self, ctx, params):
        """
        Completely overwrite a sample's ACLs. Any current ACLs are replaced by the provided
        ACLs, even if empty, and gone forever.
        The sample owner cannot be changed via this method.
        :param params: instance of type "ReplaceSampleACLsParams"
           (replace_sample_acls parameters. id - the ID of the sample to
           modify. acls - the ACLs to set on the sample.) -> structure:
           parameter "id" of type "sample_id" (A Sample ID. Must be globally
           unique. Always assigned by the Sample service.), parameter "acls"
           of type "SampleACLs" (Access control lists for a sample. Access
           levels include the privileges of the lower access levels. owner -
           the user that created and owns the sample. admin - users that can
           administrate (e.g. alter ACLs) the sample. write - users that can
           write (e.g. create a new version) to the sample. read - users that
           can view the sample.) -> structure: parameter "owner" of type
           "user" (A user's username.), parameter "admin" of list of type
           "user" (A user's username.), parameter "write" of list of type
           "user" (A user's username.), parameter "read" of list of type
           "user" (A user's username.)
        """
        # ctx is the context object
        #BEGIN replace_sample_acls
        id_ = _get_id_from_object(params, required=True)
        acls = _acls_from_dict(params)
        self._samples.replace_sample_acls(id_, ctx[_CTX_USER], acls)
        #END replace_sample_acls
        pass

    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
