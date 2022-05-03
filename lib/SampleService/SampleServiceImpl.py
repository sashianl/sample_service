# -*- coding: utf-8 -*-
#BEGIN_HEADER

import datetime as _datetime
from collections import defaultdict

from SampleService.core.config import build_samples as _build_samples
from SampleService.core.api_translation import (get_sample_address_from_object as
                                                _get_sample_address_from_object)
from SampleService.core.api_translation import get_id_from_object as _get_id_from_object
from SampleService.core.api_translation import acls_from_dict as _acls_from_dict
from SampleService.core.api_translation import acls_to_dict as _acls_to_dict
from SampleService.core.api_translation import sample_to_dict as _sample_to_dict
from SampleService.core.api_translation import create_sample_params as _create_sample_params
from SampleService.core.api_translation import validate_samples_params as _validate_samples_params
from SampleService.core.api_translation import check_admin as _check_admin
from SampleService.core.api_translation import (
    get_static_key_metadata_params as _get_static_key_metadata_params,
    create_data_link_params as _create_data_link_params,
    get_datetime_from_epochmilliseconds_in_object as _get_datetime_from_epochmillseconds_in_object,
    links_to_dicts as _links_to_dicts,
    get_upa_from_object as _get_upa_from_object,
    get_data_unit_id_from_object as _get_data_unit_id_from_object,
    get_admin_request_from_object as _get_admin_request_from_object,
    datetime_to_epochmilliseconds as _datetime_to_epochmilliseconds,
    get_user_from_object as _get_user_from_object,
    acl_delta_from_dict as _acl_delta_from_dict,
)
from SampleService.core.acls import AdminPermission as _AdminPermission
from SampleService.core.sample import SampleAddress as _SampleAddress
from SampleService.core.user import UserID as _UserID
from SampleService.impl_methods import (
    update_samples_acls as _update_samples_acls
)

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

Note that usage of the administration flags will be logged by the service.
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa
    VERSION = "0.2.5"
    GIT_URL = "git@github.com:kbase/sample_service.git"
    GIT_COMMIT_HASH = "b7e68b5768795d77287d8ea6d67d32b21ae34cf9"

    #BEGIN_CLASS_HEADER
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self._samples, self._user_lookup, self._read_exempt_roles = _build_samples(config)
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
           this is not the case, the sample will fail to save. as_admin - run
           the method as a service administrator. The user must have full
           administration permissions. as_user - create the sample as a
           different user. Ignored if as_admin is not true. Neither the
           administrator nor the impersonated user need have permissions to
           the sample if a new version is saved.) -> structure: parameter
           "sample" of type "Sample" (A Sample, consisting of a tree of
           subsamples and replicates. id - the ID of the sample. user - the
           user that saved the sample. node_tree - the tree(s) of sample
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
           source_meta - the pre-transformation keys and values of the
           controlled metadata at the data source for controlled metadata
           keys. In some cases the source metadata may be transformed prior
           to ingestion by the Sample Service; the contents of this data
           structure allows for reconstructing the original representation.
           The metadata here is not validated other than basic size checks
           and is provided on an informational basis only. The metadata keys
           in the SourceMetadata data structure must be a subset of the
           meta_controlled mapping keys. meta_user - unrestricted metadata.)
           -> structure: parameter "id" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "parent" of type "node_id" (A SampleNode ID. Must be
           unique within a Sample and be less than 255 characters.),
           parameter "type" of type "samplenode_type" (The type of a sample
           node. One of: BioReplicate - a biological replicate. Always at the
           top of the sample tree. TechReplicate - a technical replicate.
           SubSample - a sub sample that is not a technical replicate.),
           parameter "meta_controlled" of type "metadata" (Metadata attached
           to a sample.) -> mapping from type "metadata_key" (A key in a
           metadata key/value pair. Less than 1000 unicode characters.) to
           type "metadata_value" (A metadata value, represented by a mapping
           of value keys to primitive values. An example for a location
           metadata key might be: { "name": "Castle Geyser", "lat":
           44.463816, "long": -110.836471 } "primitive values" means an int,
           float, string, or equivalent typedefs. Including any collection
           types is an error.) -> mapping from type "metadata_value_key" (A
           key for a value associated with a piece of metadata. Less than
           1000 unicode characters. Examples: units, value, species) to
           unspecified object, parameter "source_meta" of list of type
           "SourceMetadata" (Information about a metadata key as it appeared
           at the data source. The source key and value represents the
           original state of the metadata before it was tranformed for
           ingestion by the sample service. key - the metadata key. skey -
           the key as it appeared at the data source. svalue - the value as
           it appeared at the data source.) -> structure: parameter "key" of
           type "metadata_key" (A key in a metadata key/value pair. Less than
           1000 unicode characters.), parameter "skey" of type "metadata_key"
           (A key in a metadata key/value pair. Less than 1000 unicode
           characters.), parameter "svalue" of type "metadata_value" (A
           metadata value, represented by a mapping of value keys to
           primitive values. An example for a location metadata key might be:
           { "name": "Castle Geyser", "lat": 44.463816, "long": -110.836471 }
           "primitive values" means an int, float, string, or equivalent
           typedefs. Including any collection types is an error.) -> mapping
           from type "metadata_value_key" (A key for a value associated with
           a piece of metadata. Less than 1000 unicode characters. Examples:
           units, value, species) to unspecified object, parameter
           "meta_user" of type "metadata" (Metadata attached to a sample.) ->
           mapping from type "metadata_key" (A key in a metadata key/value
           pair. Less than 1000 unicode characters.) to type "metadata_value"
           (A metadata value, represented by a mapping of value keys to
           primitive values. An example for a location metadata key might be:
           { "name": "Castle Geyser", "lat": 44.463816, "long": -110.836471 }
           "primitive values" means an int, float, string, or equivalent
           typedefs. Including any collection types is an error.) -> mapping
           from type "metadata_value_key" (A key for a value associated with
           a piece of metadata. Less than 1000 unicode characters. Examples:
           units, value, species) to unspecified object, parameter "name" of
           type "sample_name" (A sample name. Must be less than 255
           characters.), parameter "save_date" of type "timestamp" (A
           timestamp in epoch milliseconds.), parameter "version" of type
           "version" (The version of a sample. Always > 0.), parameter
           "prior_version" of Long, parameter "as_admin" of type "boolean" (A
           boolean value, 0 for false, 1 for true.), parameter "as_user" of
           type "user" (A user's username.)
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
        sample, id_, prev_ver = _create_sample_params(params)
        as_admin, user = _get_admin_request_from_object(params, 'as_admin', 'as_user')
        _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.FULL,
            # pretty annoying to test ctx.log_info is working, do it manually
            'create_sample', ctx.log_info, as_user=user, skip_check=not as_admin)
        ret = self._samples.save_sample(
            sample, user if user else _UserID(ctx[_CTX_USER]), id_, prev_ver, as_admin=as_admin)
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
           source_meta - the pre-transformation keys and values of the
           controlled metadata at the data source for controlled metadata
           keys. In some cases the source metadata may be transformed prior
           to ingestion by the Sample Service; the contents of this data
           structure allows for reconstructing the original representation.
           The metadata here is not validated other than basic size checks
           and is provided on an informational basis only. The metadata keys
           in the SourceMetadata data structure must be a subset of the
           meta_controlled mapping keys. meta_user - unrestricted metadata.)
           -> structure: parameter "id" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "parent" of type "node_id" (A SampleNode ID. Must be
           unique within a Sample and be less than 255 characters.),
           parameter "type" of type "samplenode_type" (The type of a sample
           node. One of: BioReplicate - a biological replicate. Always at the
           top of the sample tree. TechReplicate - a technical replicate.
           SubSample - a sub sample that is not a technical replicate.),
           parameter "meta_controlled" of type "metadata" (Metadata attached
           to a sample.) -> mapping from type "metadata_key" (A key in a
           metadata key/value pair. Less than 1000 unicode characters.) to
           type "metadata_value" (A metadata value, represented by a mapping
           of value keys to primitive values. An example for a location
           metadata key might be: { "name": "Castle Geyser", "lat":
           44.463816, "long": -110.836471 } "primitive values" means an int,
           float, string, or equivalent typedefs. Including any collection
           types is an error.) -> mapping from type "metadata_value_key" (A
           key for a value associated with a piece of metadata. Less than
           1000 unicode characters. Examples: units, value, species) to
           unspecified object, parameter "source_meta" of list of type
           "SourceMetadata" (Information about a metadata key as it appeared
           at the data source. The source key and value represents the
           original state of the metadata before it was tranformed for
           ingestion by the sample service. key - the metadata key. skey -
           the key as it appeared at the data source. svalue - the value as
           it appeared at the data source.) -> structure: parameter "key" of
           type "metadata_key" (A key in a metadata key/value pair. Less than
           1000 unicode characters.), parameter "skey" of type "metadata_key"
           (A key in a metadata key/value pair. Less than 1000 unicode
           characters.), parameter "svalue" of type "metadata_value" (A
           metadata value, represented by a mapping of value keys to
           primitive values. An example for a location metadata key might be:
           { "name": "Castle Geyser", "lat": 44.463816, "long": -110.836471 }
           "primitive values" means an int, float, string, or equivalent
           typedefs. Including any collection types is an error.) -> mapping
           from type "metadata_value_key" (A key for a value associated with
           a piece of metadata. Less than 1000 unicode characters. Examples:
           units, value, species) to unspecified object, parameter
           "meta_user" of type "metadata" (Metadata attached to a sample.) ->
           mapping from type "metadata_key" (A key in a metadata key/value
           pair. Less than 1000 unicode characters.) to type "metadata_value"
           (A metadata value, represented by a mapping of value keys to
           primitive values. An example for a location metadata key might be:
           { "name": "Castle Geyser", "lat": 44.463816, "long": -110.836471 }
           "primitive values" means an int, float, string, or equivalent
           typedefs. Including any collection types is an error.) -> mapping
           from type "metadata_value_key" (A key for a value associated with
           a piece of metadata. Less than 1000 unicode characters. Examples:
           units, value, species) to unspecified object, parameter "name" of
           type "sample_name" (A sample name. Must be less than 255
           characters.), parameter "save_date" of type "timestamp" (A
           timestamp in epoch milliseconds.), parameter "version" of type
           "version" (The version of a sample. Always > 0.)
        """
        # ctx is the context object
        # return variables are: sample
        #BEGIN get_sample
        id_, ver = _get_sample_address_from_object(params)
        admin = _check_admin(self._user_lookup, ctx.get(_CTX_TOKEN), _AdminPermission.READ,
                             # pretty annoying to test ctx.log_info is working, do it manually
                             'get_sample', ctx.log_info, skip_check=not params.get('as_admin'))
        s = self._samples.get_sample(
            id_, _get_user_from_object(ctx, _CTX_USER), ver, as_admin=admin)
        sample = _sample_to_dict(s)
        #END get_sample

        # At some point might do deeper type checking...
        if not isinstance(sample, dict):
            raise ValueError('Method get_sample return value ' +
                             'sample is not type dict as required.')
        # return the results
        return [sample]

    def get_samples(self, ctx, params):
        """
        :param params: instance of type "GetSamplesParams" -> structure:
           parameter "samples" of list of type "SampleIdentifier" ->
           structure: parameter "id" of type "sample_id" (A Sample ID. Must
           be globally unique. Always assigned by the Sample service.),
           parameter "version" of type "version" (The version of a sample.
           Always > 0.), parameter "as_admin" of type "boolean" (A boolean
           value, 0 for false, 1 for true.)
        :returns: instance of list of type "Sample" (A Sample, consisting of
           a tree of subsamples and replicates. id - the ID of the sample.
           user - the user that saved the sample. node_tree - the tree(s) of
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
           source_meta - the pre-transformation keys and values of the
           controlled metadata at the data source for controlled metadata
           keys. In some cases the source metadata may be transformed prior
           to ingestion by the Sample Service; the contents of this data
           structure allows for reconstructing the original representation.
           The metadata here is not validated other than basic size checks
           and is provided on an informational basis only. The metadata keys
           in the SourceMetadata data structure must be a subset of the
           meta_controlled mapping keys. meta_user - unrestricted metadata.)
           -> structure: parameter "id" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "parent" of type "node_id" (A SampleNode ID. Must be
           unique within a Sample and be less than 255 characters.),
           parameter "type" of type "samplenode_type" (The type of a sample
           node. One of: BioReplicate - a biological replicate. Always at the
           top of the sample tree. TechReplicate - a technical replicate.
           SubSample - a sub sample that is not a technical replicate.),
           parameter "meta_controlled" of type "metadata" (Metadata attached
           to a sample.) -> mapping from type "metadata_key" (A key in a
           metadata key/value pair. Less than 1000 unicode characters.) to
           type "metadata_value" (A metadata value, represented by a mapping
           of value keys to primitive values. An example for a location
           metadata key might be: { "name": "Castle Geyser", "lat":
           44.463816, "long": -110.836471 } "primitive values" means an int,
           float, string, or equivalent typedefs. Including any collection
           types is an error.) -> mapping from type "metadata_value_key" (A
           key for a value associated with a piece of metadata. Less than
           1000 unicode characters. Examples: units, value, species) to
           unspecified object, parameter "source_meta" of list of type
           "SourceMetadata" (Information about a metadata key as it appeared
           at the data source. The source key and value represents the
           original state of the metadata before it was tranformed for
           ingestion by the sample service. key - the metadata key. skey -
           the key as it appeared at the data source. svalue - the value as
           it appeared at the data source.) -> structure: parameter "key" of
           type "metadata_key" (A key in a metadata key/value pair. Less than
           1000 unicode characters.), parameter "skey" of type "metadata_key"
           (A key in a metadata key/value pair. Less than 1000 unicode
           characters.), parameter "svalue" of type "metadata_value" (A
           metadata value, represented by a mapping of value keys to
           primitive values. An example for a location metadata key might be:
           { "name": "Castle Geyser", "lat": 44.463816, "long": -110.836471 }
           "primitive values" means an int, float, string, or equivalent
           typedefs. Including any collection types is an error.) -> mapping
           from type "metadata_value_key" (A key for a value associated with
           a piece of metadata. Less than 1000 unicode characters. Examples:
           units, value, species) to unspecified object, parameter
           "meta_user" of type "metadata" (Metadata attached to a sample.) ->
           mapping from type "metadata_key" (A key in a metadata key/value
           pair. Less than 1000 unicode characters.) to type "metadata_value"
           (A metadata value, represented by a mapping of value keys to
           primitive values. An example for a location metadata key might be:
           { "name": "Castle Geyser", "lat": 44.463816, "long": -110.836471 }
           "primitive values" means an int, float, string, or equivalent
           typedefs. Including any collection types is an error.) -> mapping
           from type "metadata_value_key" (A key for a value associated with
           a piece of metadata. Less than 1000 unicode characters. Examples:
           units, value, species) to unspecified object, parameter "name" of
           type "sample_name" (A sample name. Must be less than 255
           characters.), parameter "save_date" of type "timestamp" (A
           timestamp in epoch milliseconds.), parameter "version" of type
           "version" (The version of a sample. Always > 0.)
        """
        # ctx is the context object
        # return variables are: samples
        #BEGIN get_samples
        # if not params.get('samples'):
        #   raise ValueError(f"")
        if type(params.get('samples')) is not list:
            raise ValueError(
                'Missing or incorrect "samples" field - ' +
                'must provide a list of samples to retrieve.'
            )
        if len(params.get('samples')) == 0:
            raise ValueError(
                'Cannot provide empty list of samples - ' +
                'must provide at least one sample to retrieve.'
            )
        ids_ = []
        for samp_obj in params['samples']:
          id_, ver = _get_sample_address_from_object(samp_obj)
          ids_.append({'id': id_, 'version': ver})
        # ids_ = _get_sample_addresses_from_object(params)
        admin = _check_admin(self._user_lookup, ctx.get(_CTX_TOKEN), _AdminPermission.READ,
                             # pretty annoying to test ctx.log_info is working, do it manually
                             'get_sample', ctx.log_info, skip_check=not params.get('as_admin'))
        samples = self._samples.get_samples(
            ids_, _get_user_from_object(ctx, _CTX_USER), as_admin=admin)
        samples = [_sample_to_dict(s) for s in samples]
        #END get_samples

        # At some point might do deeper type checking...
        if not isinstance(samples, list):
            raise ValueError('Method get_samples return value ' +
                             'samples is not type list as required.')
        # return the results
        return [samples]

    def get_sample_acls(self, ctx, params):
        """
        Get a sample's ACLs.
        :param params: instance of type "GetSampleACLsParams"
           (get_sample_acls parameters. id - the ID of the sample to
           retrieve. as_admin - get the sample acls regardless of ACL
           contents as long as the user has administration read permissions.)
           -> structure: parameter "id" of type "sample_id" (A Sample ID.
           Must be globally unique. Always assigned by the Sample service.),
           parameter "as_admin" of type "boolean" (A boolean value, 0 for
           false, 1 for true.)
        :returns: instance of type "SampleACLs" (Access control lists for a
           sample. Access levels include the privileges of the lower access
           levels. owner - the user that created and owns the sample. admin -
           users that can administrate (e.g. alter ACLs) the sample. write -
           users that can write (e.g. create a new version) to the sample.
           read - users that can view the sample. public_read - whether any
           user can read the sample, regardless of permissions.) ->
           structure: parameter "owner" of type "user" (A user's username.),
           parameter "admin" of list of type "user" (A user's username.),
           parameter "write" of list of type "user" (A user's username.),
           parameter "read" of list of type "user" (A user's username.),
           parameter "public_read" of type "boolean" (A boolean value, 0 for
           false, 1 for true.)
        """
        # ctx is the context object
        # return variables are: acls
        #BEGIN get_sample_acls
        id_ = _get_id_from_object(params, 'id', required=True)
        admin = _check_admin(
            self._user_lookup, ctx.get(_CTX_TOKEN), _AdminPermission.READ,
            # pretty annoying to test ctx.log_info is working, do it manually
            'get_sample_acls', ctx.log_info, skip_check=not params.get('as_admin'))
        acls_ret = self._samples.get_sample_acls(
            id_, _get_user_from_object(ctx, _CTX_USER), as_admin=admin)
        acls = _acls_to_dict(acls_ret, read_exempt_roles=self._read_exempt_roles)
        #END get_sample_acls

        # At some point might do deeper type checking...
        if not isinstance(acls, dict):
            raise ValueError('Method get_sample_acls return value ' +
                             'acls is not type dict as required.')
        # return the results
        return [acls]

    def update_sample_acls(self, ctx, params):
        """
        Update a sample's ACLs.
        :param params: instance of type "UpdateSampleACLsParams"
           (update_sample_acls parameters. id - the ID of the sample to
           modify. admin - a list of users that will receive admin
           privileges. Default none. write - a list of users that will
           receive write privileges. Default none. read - a list of users
           that will receive read privileges. Default none. remove - a list
           of users that will have all privileges removed. Default none.
           public_read - an integer that determines whether the sample will
           be set to publicly readable: > 0: public read. 0: No change (the
           default). < 0: private. at_least - false, the default, indicates
           that the users should get the exact permissions as specified in
           the user lists, which may mean a reduction in permissions. If
           true, users that already exist in the sample ACLs will not have
           their permissions reduced as part of the ACL update unless they
           are in the remove list. E.g. if a user has write permissions and
           read permissions are specified in the update, no changes will be
           made to the user's permission. as_admin - update the sample acls
           regardless of sample ACL contents as long as the user has full
           service administration permissions.) -> structure: parameter "id"
           of type "sample_id" (A Sample ID. Must be globally unique. Always
           assigned by the Sample service.), parameter "admin" of list of
           type "user" (A user's username.), parameter "write" of list of
           type "user" (A user's username.), parameter "read" of list of type
           "user" (A user's username.), parameter "remove" of list of type
           "user" (A user's username.), parameter "public_read" of Long,
           parameter "at_least" of type "boolean" (A boolean value, 0 for
           false, 1 for true.), parameter "as_admin" of type "boolean" (A
           boolean value, 0 for false, 1 for true.)
        """
        # ctx is the context object
        #BEGIN update_sample_acls
        id_ = _get_id_from_object(params, 'id', required=True)
        acldelta = _acl_delta_from_dict(params)
        admin = _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.FULL,
            # pretty annoying to test ctx.log_info is working, do it manually
            'update_sample_acls', ctx.log_info, skip_check=not params.get('as_admin'))
        self._samples.update_sample_acls(id_, _UserID(ctx[_CTX_USER]), acldelta, as_admin=admin)
        #END update_sample_acls
        pass

    def update_samples_acls(self, ctx, params):
        """
        Update the ACLs of many samples.
        :param params: instance of type "UpdateSamplesACLsParams"
           (update_samples_acls parameters. These parameters are the same as
           update_sample_acls, except: ids - a list of IDs of samples to
           modify.) -> structure: parameter "ids" of list of type "sample_id"
           (A Sample ID. Must be globally unique. Always assigned by the
           Sample service.), parameter "admin" of list of type "user" (A
           user's username.), parameter "write" of list of type "user" (A
           user's username.), parameter "read" of list of type "user" (A
           user's username.), parameter "remove" of list of type "user" (A
           user's username.), parameter "public_read" of Long, parameter
           "at_least" of type "boolean" (A boolean value, 0 for false, 1 for
           true.), parameter "as_admin" of type "boolean" (A boolean value, 0
           for false, 1 for true.)
        """
        # ctx is the context object
        #BEGIN update_samples_acls
        _update_samples_acls(
            params,
            self._samples,
            self._user_lookup,
            ctx[_CTX_USER],
            ctx[_CTX_TOKEN],
            _AdminPermission.FULL,
            ctx.log_info,
        )
        #END update_samples_acls
        pass

    def replace_sample_acls(self, ctx, params):
        """
        Completely overwrite a sample's ACLs. Any current ACLs are replaced by the provided
        ACLs, even if empty, and gone forever.
        The sample owner cannot be changed via this method.
        :param params: instance of type "ReplaceSampleACLsParams"
           (replace_sample_acls parameters. id - the ID of the sample to
           modify. acls - the ACLs to set on the sample. as_admin - replace
           the sample acls regardless of sample ACL contents as long as the
           user has full service administration permissions.) -> structure:
           parameter "id" of type "sample_id" (A Sample ID. Must be globally
           unique. Always assigned by the Sample service.), parameter "acls"
           of type "SampleACLs" (Access control lists for a sample. Access
           levels include the privileges of the lower access levels. owner -
           the user that created and owns the sample. admin - users that can
           administrate (e.g. alter ACLs) the sample. write - users that can
           write (e.g. create a new version) to the sample. read - users that
           can view the sample. public_read - whether any user can read the
           sample, regardless of permissions.) -> structure: parameter
           "owner" of type "user" (A user's username.), parameter "admin" of
           list of type "user" (A user's username.), parameter "write" of
           list of type "user" (A user's username.), parameter "read" of list
           of type "user" (A user's username.), parameter "public_read" of
           type "boolean" (A boolean value, 0 for false, 1 for true.),
           parameter "as_admin" of type "boolean" (A boolean value, 0 for
           false, 1 for true.)
        """
        # ctx is the context object
        #BEGIN replace_sample_acls
        id_ = _get_id_from_object(params, 'id', required=True)
        acls = _acls_from_dict(params)
        admin = _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.FULL,
            # pretty annoying to test ctx.log_info is working, do it manually
            'replace_sample_acls', ctx.log_info, skip_check=not params.get('as_admin'))
        self._samples.replace_sample_acls(id_, _UserID(ctx[_CTX_USER]), acls, as_admin=admin)
        #END replace_sample_acls
        pass

    def get_metadata_key_static_metadata(self, ctx, params):
        """
        Get static metadata for one or more metadata keys.
                The static metadata for a metadata key is metadata *about* the key - e.g. it may
                define the key's semantics or denote that the key is linked to an ontological ID.
                The static metadata does not change without the service being restarted. Client caching is
                recommended to improve performance.
        :param params: instance of type "GetMetadataKeyStaticMetadataParams"
           (get_metadata_key_static_metadata parameters. keys - the list of
           metadata keys to interrogate. prefix - 0 (the default) to
           interrogate standard metadata keys. 1 to interrogate prefix
           metadata keys, but require an exact match to the prefix key. 2 to
           interrogate prefix metadata keys, but any keys which are a prefix
           of the provided keys will be included in the results.) ->
           structure: parameter "keys" of list of type "metadata_key" (A key
           in a metadata key/value pair. Less than 1000 unicode characters.),
           parameter "prefix" of Long
        :returns: instance of type "GetMetadataKeyStaticMetadataResults"
           (get_metadata_key_static_metadata results. static_metadata - the
           static metadata for the requested keys.) -> structure: parameter
           "static_metadata" of type "metadata" (Metadata attached to a
           sample.) -> mapping from type "metadata_key" (A key in a metadata
           key/value pair. Less than 1000 unicode characters.) to type
           "metadata_value" (A metadata value, represented by a mapping of
           value keys to primitive values. An example for a location metadata
           key might be: { "name": "Castle Geyser", "lat": 44.463816, "long":
           -110.836471 } "primitive values" means an int, float, string, or
           equivalent typedefs. Including any collection types is an error.)
           -> mapping from type "metadata_value_key" (A key for a value
           associated with a piece of metadata. Less than 1000 unicode
           characters. Examples: units, value, species) to unspecified object
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN get_metadata_key_static_metadata
        keys, prefix = _get_static_key_metadata_params(params)
        results = {'static_metadata': self._samples.get_key_static_metadata(keys, prefix=prefix)}
        #END get_metadata_key_static_metadata

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method get_metadata_key_static_metadata return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]

    def create_data_link(self, ctx, params):
        """
        Create a link from a KBase Workspace object to a sample.
                The user must have admin permissions for the sample and write permissions for the
                Workspace object.
        :param params: instance of type "CreateDataLinkParams"
           (create_data_link parameters. upa - the workspace UPA of the
           object to be linked. dataid - the dataid of the data to be linked,
           if any, within the object. If omitted the entire object is linked
           to the sample. id - the sample id. version - the sample version.
           node - the sample node. update - if false (the default), fail if a
           link already exists from the data unit (the combination of the UPA
           and dataid). if true, expire the old link and create the new link
           unless the link is already to the requested sample node, in which
           case the operation is a no-op. as_admin - run the method as a
           service administrator. The user must have full administration
           permissions. as_user - create the link as a different user.
           Ignored if as_admin is not true. Neither the administrator nor the
           impersonated user need have permissions to the data or sample.) ->
           structure: parameter "upa" of type "ws_upa" (A KBase Workspace
           service Unique Permanent Address (UPA). E.g. 5/6/7 where 5 is the
           workspace ID, 6 the object ID, and 7 the object version.),
           parameter "dataid" of type "data_id" (An id for a unit of data
           within a KBase Workspace object. A single object may contain many
           data units. A dataid is expected to be unique within a single
           object. Must be less than 255 characters.), parameter "id" of type
           "sample_id" (A Sample ID. Must be globally unique. Always assigned
           by the Sample service.), parameter "version" of type "version"
           (The version of a sample. Always > 0.), parameter "node" of type
           "node_id" (A SampleNode ID. Must be unique within a Sample and be
           less than 255 characters.), parameter "update" of type "boolean"
           (A boolean value, 0 for false, 1 for true.), parameter "as_admin"
           of type "boolean" (A boolean value, 0 for false, 1 for true.),
           parameter "as_user" of type "user" (A user's username.)
        :returns: instance of type "CreateDataLinkResults" (create_data_link
           results. new_link - the new link.) -> structure: parameter
           "new_link" of type "DataLink" (A data link from a KBase workspace
           object to a sample. upa - the workspace UPA of the linked object.
           dataid - the dataid of the linked data, if any, within the object.
           If omitted the entire object is linked to the sample. id - the
           sample id. version - the sample version. node - the sample node.
           createdby - the user that created the link. created - the time the
           link was created. expiredby - the user that expired the link, if
           any. expired - the time the link was expired, if at all.) ->
           structure: parameter "linkid" of type "link_id" (A link ID. Must
           be globally unique. Always assigned by the Sample service.
           Typically only of use to service admins.), parameter "upa" of type
           "ws_upa" (A KBase Workspace service Unique Permanent Address
           (UPA). E.g. 5/6/7 where 5 is the workspace ID, 6 the object ID,
           and 7 the object version.), parameter "dataid" of type "data_id"
           (An id for a unit of data within a KBase Workspace object. A
           single object may contain many data units. A dataid is expected to
           be unique within a single object. Must be less than 255
           characters.), parameter "id" of type "sample_id" (A Sample ID.
           Must be globally unique. Always assigned by the Sample service.),
           parameter "version" of type "version" (The version of a sample.
           Always > 0.), parameter "node" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "createdby" of type "user" (A user's username.),
           parameter "created" of type "timestamp" (A timestamp in epoch
           milliseconds.), parameter "expiredby" of type "user" (A user's
           username.), parameter "expired" of type "timestamp" (A timestamp
           in epoch milliseconds.)
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN create_data_link
        duid, sna, update = _create_data_link_params(params)
        as_admin, user = _get_admin_request_from_object(params, 'as_admin', 'as_user')
        _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.FULL,
            # pretty annoying to test ctx.log_info is working, do it manually
            'create_data_link', ctx.log_info, as_user=user, skip_check=not as_admin)
        link = self._samples.create_data_link(
            user if user else _UserID(ctx[_CTX_USER]),
            duid,
            sna,
            update,
            as_admin=as_admin)
        results = {'new_link': _links_to_dicts([link])[0]}
        #END create_data_link

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method create_data_link return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]

    def propagate_data_links(self, ctx, params):
        """
        Propagates data links from a previous sample to the current (latest) version
                The user must have admin permissions for the sample and write permissions for the
                Workspace object.
        :param params: instance of type "PropagateDataLinkParams"
           (propagate_data_links parameters. id - the sample id. version -
           the sample version. (data links are propagated to)
           previous_version - the previouse sample version. (data links are
           propagated from) ignore_types - the workspace data type ignored
           from propagating. default empty. update - if false (the default),
           fail if a link already exists from the data unit (the combination
           of the UPA and dataid). if true, expire the old link and create
           the new link unless the link is already to the requested sample
           node, in which case the operation is a no-op. effective_time - the
           effective time at which the query should be run - the default is
           the current time. Providing a time allows for reproducibility of
           previous results. as_admin - run the method as a service
           administrator. The user must have full administration permissions.
           as_user - create the link as a different user. Ignored if as_admin
           is not true. Neither the administrator nor the impersonated user
           need have permissions to the data or sample.) -> structure:
           parameter "id" of type "sample_id" (A Sample ID. Must be globally
           unique. Always assigned by the Sample service.), parameter
           "version" of type "version" (The version of a sample. Always >
           0.), parameter "previous_version" of type "version" (The version
           of a sample. Always > 0.), parameter "ignore_types" of list of
           type "ws_type_string" (A workspace type string. Specifies the
           workspace data type a single string in the format
           [module].[typename]: module - a string. The module name of the
           typespec containing the type. typename - a string. The name of the
           type as assigned by the typedef statement. Example:
           KBaseSets.SampleSet), parameter "update" of type "boolean" (A
           boolean value, 0 for false, 1 for true.), parameter
           "effective_time" of type "timestamp" (A timestamp in epoch
           milliseconds.), parameter "as_admin" of type "boolean" (A boolean
           value, 0 for false, 1 for true.), parameter "as_user" of type
           "user" (A user's username.)
        :returns: instance of type "PropagateDataLinkResults"
           (propagate_data_links results. links - the links.) -> structure:
           parameter "links" of list of type "DataLink" (A data link from a
           KBase workspace object to a sample. upa - the workspace UPA of the
           linked object. dataid - the dataid of the linked data, if any,
           within the object. If omitted the entire object is linked to the
           sample. id - the sample id. version - the sample version. node -
           the sample node. createdby - the user that created the link.
           created - the time the link was created. expiredby - the user that
           expired the link, if any. expired - the time the link was expired,
           if at all.) -> structure: parameter "linkid" of type "link_id" (A
           link ID. Must be globally unique. Always assigned by the Sample
           service. Typically only of use to service admins.), parameter
           "upa" of type "ws_upa" (A KBase Workspace service Unique Permanent
           Address (UPA). E.g. 5/6/7 where 5 is the workspace ID, 6 the
           object ID, and 7 the object version.), parameter "dataid" of type
           "data_id" (An id for a unit of data within a KBase Workspace
           object. A single object may contain many data units. A dataid is
           expected to be unique within a single object. Must be less than
           255 characters.), parameter "id" of type "sample_id" (A Sample ID.
           Must be globally unique. Always assigned by the Sample service.),
           parameter "version" of type "version" (The version of a sample.
           Always > 0.), parameter "node" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "createdby" of type "user" (A user's username.),
           parameter "created" of type "timestamp" (A timestamp in epoch
           milliseconds.), parameter "expiredby" of type "user" (A user's
           username.), parameter "expired" of type "timestamp" (A timestamp
           in epoch milliseconds.)
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN propagate_data_links
        sid = params.get('id')
        ver = params.get('version')

        get_links_params = {'id': sid,
                            'version': params.get('previous_version'),
                            'effective_time': params.get('effective_time'),
                            'as_admin': params.get('as_admin')}
        data_links = self.get_data_links_from_sample(ctx, get_links_params)[0].get('links')
        links = list()
        ignore_types = params.get('ignore_types', list())
        for data_link in data_links:
            upa = data_link['upa']

            ignored = False
            if ignore_types:
                wsClient = self._samples._ws._ws
                ret = wsClient.administer({'command': 'getObjectInfo',
                                           'params': {'objects': [{'ref': str(upa)}],
                                                      'ignoreErrors': 1}})

                if ret['infos'][0][2].split('-')[0] in ignore_types:
                    ignored = True

            if not ignored:
                create_link_params = {'upa': upa,
                                      'dataid': data_link.get('dataid') + '_' + str(ver),
                                      'node': data_link.get('node'),
                                      'id': sid,
                                      'version': ver,
                                      'update': params.get('update'),
                                      'as_admin': params.get('as_admin'),
                                      'as_user': params.get('as_user')
                                      }
                new_link = self.create_data_link(ctx, create_link_params)[0].get('new_link')
                links.append(new_link)

        results = {'links': links}
        #END propagate_data_links

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method propagate_data_links return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]

    def expire_data_link(self, ctx, params):
        """
        Expire a link from a KBase Workspace object.
                The user must have admin permissions for the sample and write permissions for the
                Workspace object.
        :param params: instance of type "ExpireDataLinkParams"
           (expire_data_link parameters. upa - the workspace upa of the
           object from which the link originates. dataid - the dataid, if
           any, of the data within the object from which the link originates.
           Omit for links where the link is from the entire object. as_admin
           - run the method as a service administrator. The user must have
           full administration permissions. as_user - expire the link as a
           different user. Ignored if as_admin is not true. Neither the
           administrator nor the impersonated user need have permissions to
           the link if a new version is saved.) -> structure: parameter "upa"
           of type "ws_upa" (A KBase Workspace service Unique Permanent
           Address (UPA). E.g. 5/6/7 where 5 is the workspace ID, 6 the
           object ID, and 7 the object version.), parameter "dataid" of type
           "data_id" (An id for a unit of data within a KBase Workspace
           object. A single object may contain many data units. A dataid is
           expected to be unique within a single object. Must be less than
           255 characters.), parameter "as_admin" of type "boolean" (A
           boolean value, 0 for false, 1 for true.), parameter "as_user" of
           type "user" (A user's username.)
        """
        # ctx is the context object
        #BEGIN expire_data_link
        duid = _get_data_unit_id_from_object(params)
        as_admin, user = _get_admin_request_from_object(params, 'as_admin', 'as_user')
        _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.FULL,
            # pretty annoying to test ctx.log_info is working, do it manually
            'expire_data_link', ctx.log_info, as_user=user, skip_check=not as_admin)
        self._samples.expire_data_link(
            user if user else _UserID(ctx[_CTX_USER]),
            duid,
            as_admin=as_admin)
        #END expire_data_link
        pass

    def get_data_links_from_sample(self, ctx, params):
        """
        Get data links to Workspace objects originating from a sample.
                The user must have read permissions to the sample. Only Workspace objects the user
                can read are returned.
        :param params: instance of type "GetDataLinksFromSampleParams"
           (get_data_links_from_sample parameters. id - the sample ID.
           version - the sample version. effective_time - the effective time
           at which the query should be run - the default is the current
           time. Providing a time allows for reproducibility of previous
           results. as_admin - run the method as a service administrator. The
           user must have read administration permissions.) -> structure:
           parameter "id" of type "sample_id" (A Sample ID. Must be globally
           unique. Always assigned by the Sample service.), parameter
           "version" of type "version" (The version of a sample. Always >
           0.), parameter "effective_time" of type "timestamp" (A timestamp
           in epoch milliseconds.), parameter "as_admin" of type "boolean" (A
           boolean value, 0 for false, 1 for true.)
        :returns: instance of type "GetDataLinksFromSampleResults"
           (get_data_links_from_sample results. links - the links.
           effective_time - the time at which the query was run. This
           timestamp, if saved, can be used when running the method again to
           ensure reproducible results. Note that changes to workspace
           permissions may cause results to change over time.) -> structure:
           parameter "links" of list of type "DataLink" (A data link from a
           KBase workspace object to a sample. upa - the workspace UPA of the
           linked object. dataid - the dataid of the linked data, if any,
           within the object. If omitted the entire object is linked to the
           sample. id - the sample id. version - the sample version. node -
           the sample node. createdby - the user that created the link.
           created - the time the link was created. expiredby - the user that
           expired the link, if any. expired - the time the link was expired,
           if at all.) -> structure: parameter "linkid" of type "link_id" (A
           link ID. Must be globally unique. Always assigned by the Sample
           service. Typically only of use to service admins.), parameter
           "upa" of type "ws_upa" (A KBase Workspace service Unique Permanent
           Address (UPA). E.g. 5/6/7 where 5 is the workspace ID, 6 the
           object ID, and 7 the object version.), parameter "dataid" of type
           "data_id" (An id for a unit of data within a KBase Workspace
           object. A single object may contain many data units. A dataid is
           expected to be unique within a single object. Must be less than
           255 characters.), parameter "id" of type "sample_id" (A Sample ID.
           Must be globally unique. Always assigned by the Sample service.),
           parameter "version" of type "version" (The version of a sample.
           Always > 0.), parameter "node" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "createdby" of type "user" (A user's username.),
           parameter "created" of type "timestamp" (A timestamp in epoch
           milliseconds.), parameter "expiredby" of type "user" (A user's
           username.), parameter "expired" of type "timestamp" (A timestamp
           in epoch milliseconds.), parameter "effective_time" of type
           "timestamp" (A timestamp in epoch milliseconds.)
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN get_data_links_from_sample
        sid, ver = _get_sample_address_from_object(params, version_required=True)
        dt = _get_datetime_from_epochmillseconds_in_object(params, 'effective_time')
        admin = _check_admin(
            self._user_lookup, ctx.get(_CTX_TOKEN), _AdminPermission.READ,
            # pretty annoying to test ctx.log_info is working, do it manually
            'get_data_links_from_sample', ctx.log_info, skip_check=not params.get('as_admin'))
        links, ts = self._samples.get_links_from_sample(
            _get_user_from_object(ctx, _CTX_USER), _SampleAddress(sid, ver), dt, as_admin=admin)
        results = {'links': _links_to_dicts(links),
                   'effective_time': _datetime_to_epochmilliseconds(ts)
                   }
        #END get_data_links_from_sample

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method get_data_links_from_sample return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]

    def get_data_links_from_sample_set(self, ctx, params):
        """
        Get all workspace object metadata linked to samples in a list of samples or sample set
        refs. Returns metadata about links to data objects. A batch version of
        get_data_links_from_sample.
        The user must have read permissions to the sample. A permissions error is thrown when a
        sample is found that the user has no access to.
        :param params: instance of type "GetDataLinksFromSampleSetParams"
           (get_data_links_from_sample_set parameters. sample_ids - a list of
           sample ids and versions effective_time - the time at which the
           query was run. This timestamp, if saved, can be used when running
           the method again to enqure reproducible results. Note that changes
           to workspace permissions may cause results to change over time.
           as_admin - run the method as a service administrator. The user
           must have read administration permissions.) -> structure:
           parameter "sample_ids" of list of type "SampleIdentifier" ->
           structure: parameter "id" of type "sample_id" (A Sample ID. Must
           be globally unique. Always assigned by the Sample service.),
           parameter "version" of type "version" (The version of a sample.
           Always > 0.), parameter "effective_time" of type "timestamp" (A
           timestamp in epoch milliseconds.), parameter "as_admin" of type
           "boolean" (A boolean value, 0 for false, 1 for true.)
        :returns: instance of type "GetDataLinksFromSampleResults"
           (get_data_links_from_sample results. links - the links.
           effective_time - the time at which the query was run. This
           timestamp, if saved, can be used when running the method again to
           ensure reproducible results. Note that changes to workspace
           permissions may cause results to change over time.) -> structure:
           parameter "links" of list of type "DataLink" (A data link from a
           KBase workspace object to a sample. upa - the workspace UPA of the
           linked object. dataid - the dataid of the linked data, if any,
           within the object. If omitted the entire object is linked to the
           sample. id - the sample id. version - the sample version. node -
           the sample node. createdby - the user that created the link.
           created - the time the link was created. expiredby - the user that
           expired the link, if any. expired - the time the link was expired,
           if at all.) -> structure: parameter "linkid" of type "link_id" (A
           link ID. Must be globally unique. Always assigned by the Sample
           service. Typically only of use to service admins.), parameter
           "upa" of type "ws_upa" (A KBase Workspace service Unique Permanent
           Address (UPA). E.g. 5/6/7 where 5 is the workspace ID, 6 the
           object ID, and 7 the object version.), parameter "dataid" of type
           "data_id" (An id for a unit of data within a KBase Workspace
           object. A single object may contain many data units. A dataid is
           expected to be unique within a single object. Must be less than
           255 characters.), parameter "id" of type "sample_id" (A Sample ID.
           Must be globally unique. Always assigned by the Sample service.),
           parameter "version" of type "version" (The version of a sample.
           Always > 0.), parameter "node" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "createdby" of type "user" (A user's username.),
           parameter "created" of type "timestamp" (A timestamp in epoch
           milliseconds.), parameter "expiredby" of type "user" (A user's
           username.), parameter "expired" of type "timestamp" (A timestamp
           in epoch milliseconds.), parameter "effective_time" of type
           "timestamp" (A timestamp in epoch milliseconds.)
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN get_data_links_from_sample_set
        if not 'sample_ids' in params:
            raise ValueError(
                'Missing "sample_ids" field - Must provide a list of valid sample ids.'
            )

        try:
            sample_ids = [_get_sample_address_from_object({
                "id": sample_id['id'],
                "version": sample_id['version'],
                "effective_time": params['effective_time'],
                "as_admin": params.get('as_admin')
            }, version_required=True) for sample_id in params['sample_ids']]
        except KeyError as e:
            if str(e) == "'effective_time'":
                raise ValueError('Missing "effective_time" parameter.')
            raise ValueError(
                "Malformed sample accessor - each sample must provide both an id and a version."
            )

        dt = _get_datetime_from_epochmillseconds_in_object(params, 'effective_time')

        admin = _check_admin(
            self._user_lookup, ctx.get(_CTX_TOKEN), _AdminPermission.READ,
            'get_data_links_from_sample', ctx.log_info, skip_check=not params.get('as_admin'))

        # cast tuple results to type SampleAddress
        sample_addresses = [_SampleAddress(sid, ver) for sid, ver in sample_ids]

        links, ts = self._samples.get_batch_links_from_sample_set(
            _get_user_from_object(ctx, _CTX_USER), sample_addresses, dt, as_admin=admin)

        results = {
            'links': _links_to_dicts(links),
            'effective_time': _datetime_to_epochmilliseconds(ts)
            }

        #END get_data_links_from_sample_set

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method get_data_links_from_sample_set return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]

    def get_data_links_from_data(self, ctx, params):
        """
        Get data links to samples originating from Workspace data.
                The user must have read permissions to the workspace data.
        :param params: instance of type "GetDataLinksFromDataParams"
           (get_data_links_from_data parameters. upa - the data UPA.
           effective_time - the effective time at which the query should be
           run - the default is the current time. Providing a time allows for
           reproducibility of previous results. as_admin - run the method as
           a service administrator. The user must have read administration
           permissions.) -> structure: parameter "upa" of type "ws_upa" (A
           KBase Workspace service Unique Permanent Address (UPA). E.g. 5/6/7
           where 5 is the workspace ID, 6 the object ID, and 7 the object
           version.), parameter "effective_time" of type "timestamp" (A
           timestamp in epoch milliseconds.), parameter "as_admin" of type
           "boolean" (A boolean value, 0 for false, 1 for true.)
        :returns: instance of type "GetDataLinksFromDataResults"
           (get_data_links_from_data results. links - the links.
           effective_time - the time at which the query was run. This
           timestamp, if saved, can be used when running the method again to
           ensure reproducible results.) -> structure: parameter "links" of
           list of type "DataLink" (A data link from a KBase workspace object
           to a sample. upa - the workspace UPA of the linked object. dataid
           - the dataid of the linked data, if any, within the object. If
           omitted the entire object is linked to the sample. id - the sample
           id. version - the sample version. node - the sample node.
           createdby - the user that created the link. created - the time the
           link was created. expiredby - the user that expired the link, if
           any. expired - the time the link was expired, if at all.) ->
           structure: parameter "linkid" of type "link_id" (A link ID. Must
           be globally unique. Always assigned by the Sample service.
           Typically only of use to service admins.), parameter "upa" of type
           "ws_upa" (A KBase Workspace service Unique Permanent Address
           (UPA). E.g. 5/6/7 where 5 is the workspace ID, 6 the object ID,
           and 7 the object version.), parameter "dataid" of type "data_id"
           (An id for a unit of data within a KBase Workspace object. A
           single object may contain many data units. A dataid is expected to
           be unique within a single object. Must be less than 255
           characters.), parameter "id" of type "sample_id" (A Sample ID.
           Must be globally unique. Always assigned by the Sample service.),
           parameter "version" of type "version" (The version of a sample.
           Always > 0.), parameter "node" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "createdby" of type "user" (A user's username.),
           parameter "created" of type "timestamp" (A timestamp in epoch
           milliseconds.), parameter "expiredby" of type "user" (A user's
           username.), parameter "expired" of type "timestamp" (A timestamp
           in epoch milliseconds.), parameter "effective_time" of type
           "timestamp" (A timestamp in epoch milliseconds.)
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN get_data_links_from_data
        upa = _get_upa_from_object(params)
        dt = _get_datetime_from_epochmillseconds_in_object(params, 'effective_time')
        admin = _check_admin(
            self._user_lookup, ctx.get(_CTX_TOKEN), _AdminPermission.READ,
            # pretty annoying to test ctx.log_info is working, do it manually
            'get_data_links_from_data', ctx.log_info, skip_check=not params.get('as_admin'))
        links, ts = self._samples.get_links_from_data(
            _get_user_from_object(ctx, _CTX_USER), upa, dt, as_admin=admin)
        results = {'links': _links_to_dicts(links),
                   'effective_time': _datetime_to_epochmilliseconds(ts)
                   }
        #END get_data_links_from_data

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method get_data_links_from_data return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]

    def get_sample_via_data(self, ctx, params):
        """
        Get a sample via a workspace object. Read permissions to a workspace object grants
        read permissions to all versions of any linked samples, whether the links are expired or
        not. This method allows for fetching samples when the user does not have explicit
        read access to the sample.
        :param params: instance of type "GetSampleViaDataParams"
           (get_sample_via_data parameters. upa - the workspace UPA of the
           target object. id - the target sample id. version - the target
           sample version.) -> structure: parameter "upa" of type "ws_upa" (A
           KBase Workspace service Unique Permanent Address (UPA). E.g. 5/6/7
           where 5 is the workspace ID, 6 the object ID, and 7 the object
           version.), parameter "id" of type "sample_id" (A Sample ID. Must
           be globally unique. Always assigned by the Sample service.),
           parameter "version" of type "version" (The version of a sample.
           Always > 0.)
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
           source_meta - the pre-transformation keys and values of the
           controlled metadata at the data source for controlled metadata
           keys. In some cases the source metadata may be transformed prior
           to ingestion by the Sample Service; the contents of this data
           structure allows for reconstructing the original representation.
           The metadata here is not validated other than basic size checks
           and is provided on an informational basis only. The metadata keys
           in the SourceMetadata data structure must be a subset of the
           meta_controlled mapping keys. meta_user - unrestricted metadata.)
           -> structure: parameter "id" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "parent" of type "node_id" (A SampleNode ID. Must be
           unique within a Sample and be less than 255 characters.),
           parameter "type" of type "samplenode_type" (The type of a sample
           node. One of: BioReplicate - a biological replicate. Always at the
           top of the sample tree. TechReplicate - a technical replicate.
           SubSample - a sub sample that is not a technical replicate.),
           parameter "meta_controlled" of type "metadata" (Metadata attached
           to a sample.) -> mapping from type "metadata_key" (A key in a
           metadata key/value pair. Less than 1000 unicode characters.) to
           type "metadata_value" (A metadata value, represented by a mapping
           of value keys to primitive values. An example for a location
           metadata key might be: { "name": "Castle Geyser", "lat":
           44.463816, "long": -110.836471 } "primitive values" means an int,
           float, string, or equivalent typedefs. Including any collection
           types is an error.) -> mapping from type "metadata_value_key" (A
           key for a value associated with a piece of metadata. Less than
           1000 unicode characters. Examples: units, value, species) to
           unspecified object, parameter "source_meta" of list of type
           "SourceMetadata" (Information about a metadata key as it appeared
           at the data source. The source key and value represents the
           original state of the metadata before it was tranformed for
           ingestion by the sample service. key - the metadata key. skey -
           the key as it appeared at the data source. svalue - the value as
           it appeared at the data source.) -> structure: parameter "key" of
           type "metadata_key" (A key in a metadata key/value pair. Less than
           1000 unicode characters.), parameter "skey" of type "metadata_key"
           (A key in a metadata key/value pair. Less than 1000 unicode
           characters.), parameter "svalue" of type "metadata_value" (A
           metadata value, represented by a mapping of value keys to
           primitive values. An example for a location metadata key might be:
           { "name": "Castle Geyser", "lat": 44.463816, "long": -110.836471 }
           "primitive values" means an int, float, string, or equivalent
           typedefs. Including any collection types is an error.) -> mapping
           from type "metadata_value_key" (A key for a value associated with
           a piece of metadata. Less than 1000 unicode characters. Examples:
           units, value, species) to unspecified object, parameter
           "meta_user" of type "metadata" (Metadata attached to a sample.) ->
           mapping from type "metadata_key" (A key in a metadata key/value
           pair. Less than 1000 unicode characters.) to type "metadata_value"
           (A metadata value, represented by a mapping of value keys to
           primitive values. An example for a location metadata key might be:
           { "name": "Castle Geyser", "lat": 44.463816, "long": -110.836471 }
           "primitive values" means an int, float, string, or equivalent
           typedefs. Including any collection types is an error.) -> mapping
           from type "metadata_value_key" (A key for a value associated with
           a piece of metadata. Less than 1000 unicode characters. Examples:
           units, value, species) to unspecified object, parameter "name" of
           type "sample_name" (A sample name. Must be less than 255
           characters.), parameter "save_date" of type "timestamp" (A
           timestamp in epoch milliseconds.), parameter "version" of type
           "version" (The version of a sample. Always > 0.)
        """
        # ctx is the context object
        # return variables are: sample
        #BEGIN get_sample_via_data
        upa = _get_upa_from_object(params)
        sid, ver = _get_sample_address_from_object(params, version_required=True)
        sample = self._samples.get_sample_via_data(
            _get_user_from_object(ctx, _CTX_USER), upa, _SampleAddress(sid, ver))
        sample = _sample_to_dict(sample)
        #END get_sample_via_data

        # At some point might do deeper type checking...
        if not isinstance(sample, dict):
            raise ValueError('Method get_sample_via_data return value ' +
                             'sample is not type dict as required.')
        # return the results
        return [sample]

    def get_data_link(self, ctx, params):
        """
        Get a link, expired or not, by its ID. This method requires read administration privileges
        for the service.
        :param params: instance of type "GetDataLinkParams" (get_data_link
           parameters. linkid - the link ID.) -> structure: parameter
           "linkid" of type "link_id" (A link ID. Must be globally unique.
           Always assigned by the Sample service. Typically only of use to
           service admins.)
        :returns: instance of type "DataLink" (A data link from a KBase
           workspace object to a sample. upa - the workspace UPA of the
           linked object. dataid - the dataid of the linked data, if any,
           within the object. If omitted the entire object is linked to the
           sample. id - the sample id. version - the sample version. node -
           the sample node. createdby - the user that created the link.
           created - the time the link was created. expiredby - the user that
           expired the link, if any. expired - the time the link was expired,
           if at all.) -> structure: parameter "linkid" of type "link_id" (A
           link ID. Must be globally unique. Always assigned by the Sample
           service. Typically only of use to service admins.), parameter
           "upa" of type "ws_upa" (A KBase Workspace service Unique Permanent
           Address (UPA). E.g. 5/6/7 where 5 is the workspace ID, 6 the
           object ID, and 7 the object version.), parameter "dataid" of type
           "data_id" (An id for a unit of data within a KBase Workspace
           object. A single object may contain many data units. A dataid is
           expected to be unique within a single object. Must be less than
           255 characters.), parameter "id" of type "sample_id" (A Sample ID.
           Must be globally unique. Always assigned by the Sample service.),
           parameter "version" of type "version" (The version of a sample.
           Always > 0.), parameter "node" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "createdby" of type "user" (A user's username.),
           parameter "created" of type "timestamp" (A timestamp in epoch
           milliseconds.), parameter "expiredby" of type "user" (A user's
           username.), parameter "expired" of type "timestamp" (A timestamp
           in epoch milliseconds.)
        """
        # ctx is the context object
        # return variables are: link
        #BEGIN get_data_link
        id_ = _get_id_from_object(params, 'linkid', required=True)
        _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.READ,
            # pretty annoying to test ctx.log_info is working, do it manually
            'get_data_link', ctx.log_info)
        dl = self._samples.get_data_link_admin(id_)
        link = _links_to_dicts([dl])[0]
        #END get_data_link

        # At some point might do deeper type checking...
        if not isinstance(link, dict):
            raise ValueError('Method get_data_link return value ' +
                             'link is not type dict as required.')
        # return the results
        return [link]

    def validate_samples(self, ctx, params):
        """
        :param params: instance of type "ValidateSamplesParams" (Provide
           sample and run through the validation steps, but without saving
           them. Allows all the samples to be evaluated for validity first so
           potential errors can be addressed.) -> structure: parameter
           "samples" of list of type "Sample" (A Sample, consisting of a tree
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
           source_meta - the pre-transformation keys and values of the
           controlled metadata at the data source for controlled metadata
           keys. In some cases the source metadata may be transformed prior
           to ingestion by the Sample Service; the contents of this data
           structure allows for reconstructing the original representation.
           The metadata here is not validated other than basic size checks
           and is provided on an informational basis only. The metadata keys
           in the SourceMetadata data structure must be a subset of the
           meta_controlled mapping keys. meta_user - unrestricted metadata.)
           -> structure: parameter "id" of type "node_id" (A SampleNode ID.
           Must be unique within a Sample and be less than 255 characters.),
           parameter "parent" of type "node_id" (A SampleNode ID. Must be
           unique within a Sample and be less than 255 characters.),
           parameter "type" of type "samplenode_type" (The type of a sample
           node. One of: BioReplicate - a biological replicate. Always at the
           top of the sample tree. TechReplicate - a technical replicate.
           SubSample - a sub sample that is not a technical replicate.),
           parameter "meta_controlled" of type "metadata" (Metadata attached
           to a sample.) -> mapping from type "metadata_key" (A key in a
           metadata key/value pair. Less than 1000 unicode characters.) to
           type "metadata_value" (A metadata value, represented by a mapping
           of value keys to primitive values. An example for a location
           metadata key might be: { "name": "Castle Geyser", "lat":
           44.463816, "long": -110.836471 } "primitive values" means an int,
           float, string, or equivalent typedefs. Including any collection
           types is an error.) -> mapping from type "metadata_value_key" (A
           key for a value associated with a piece of metadata. Less than
           1000 unicode characters. Examples: units, value, species) to
           unspecified object, parameter "source_meta" of list of type
           "SourceMetadata" (Information about a metadata key as it appeared
           at the data source. The source key and value represents the
           original state of the metadata before it was tranformed for
           ingestion by the sample service. key - the metadata key. skey -
           the key as it appeared at the data source. svalue - the value as
           it appeared at the data source.) -> structure: parameter "key" of
           type "metadata_key" (A key in a metadata key/value pair. Less than
           1000 unicode characters.), parameter "skey" of type "metadata_key"
           (A key in a metadata key/value pair. Less than 1000 unicode
           characters.), parameter "svalue" of type "metadata_value" (A
           metadata value, represented by a mapping of value keys to
           primitive values. An example for a location metadata key might be:
           { "name": "Castle Geyser", "lat": 44.463816, "long": -110.836471 }
           "primitive values" means an int, float, string, or equivalent
           typedefs. Including any collection types is an error.) -> mapping
           from type "metadata_value_key" (A key for a value associated with
           a piece of metadata. Less than 1000 unicode characters. Examples:
           units, value, species) to unspecified object, parameter
           "meta_user" of type "metadata" (Metadata attached to a sample.) ->
           mapping from type "metadata_key" (A key in a metadata key/value
           pair. Less than 1000 unicode characters.) to type "metadata_value"
           (A metadata value, represented by a mapping of value keys to
           primitive values. An example for a location metadata key might be:
           { "name": "Castle Geyser", "lat": 44.463816, "long": -110.836471 }
           "primitive values" means an int, float, string, or equivalent
           typedefs. Including any collection types is an error.) -> mapping
           from type "metadata_value_key" (A key for a value associated with
           a piece of metadata. Less than 1000 unicode characters. Examples:
           units, value, species) to unspecified object, parameter "name" of
           type "sample_name" (A sample name. Must be less than 255
           characters.), parameter "save_date" of type "timestamp" (A
           timestamp in epoch milliseconds.), parameter "version" of type
           "version" (The version of a sample. Always > 0.)
        :returns: instance of type "ValidateSamplesResults" -> structure:
           parameter "errors" of list of type "ValidateSamplesError" ->
           structure: parameter "message" of String, parameter "dev_message"
           of String, parameter "sample_name" of type "sample_name" (A sample
           name. Must be less than 255 characters.), parameter "node" of type
           "node_id" (A SampleNode ID. Must be unique within a Sample and be
           less than 255 characters.), parameter "key" of type "metadata_key"
           (A key in a metadata key/value pair. Less than 1000 unicode
           characters.), parameter "subkey" of String
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN validate_samples
        samples = _validate_samples_params(params)
        errors = []
        for sample in samples:
          error_detail = self._samples.validate_sample(sample)
          errors.extend(error_detail)
        results = {'errors': errors}
        #END validate_samples

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method validate_samples return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH,
                     'servertime': _datetime_to_epochmilliseconds(_datetime.datetime.now(
                         tz=_datetime.timezone.utc))}
        #END_STATUS
        return [returnVal]
