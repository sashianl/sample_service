# -*- coding: utf-8 -*-
#BEGIN_HEADER

import datetime as _datetime

from SampleService.core.config import build_samples as _build_samples
from SampleService.core.api_translation import (get_sample_address_from_object as
                                                _get_sample_address_from_object)
from SampleService.core.api_translation import get_id_from_object as _get_id_from_object
from SampleService.core.api_translation import acls_from_dict as _acls_from_dict
from SampleService.core.api_translation import acls_to_dict as _acls_to_dict
from SampleService.core.api_translation import sample_to_dict as _sample_to_dict
from SampleService.core.api_translation import create_sample_params as _create_sample_params
from SampleService.core.api_translation import check_admin as _check_admin
from SampleService.core.api_translation import (
    get_static_key_metadata_params as _get_static_key_metadata_params,
    create_data_link_params as _create_data_link_params,
    get_datetime_from_epochmilliseconds_in_object as _get_datetime_from_epochmillseconds_in_object,
    links_to_dicts as _links_to_dicts,
    get_upa_from_object as _get_upa_from_object,
    get_data_unit_id_from_object as _get_data_unit_id_from_object,
    get_admin_request_from_object as _get_admin_request_from_object,
    datetime_to_epochmilliseconds as _datetime_to_epochmilliseconds
    )
from SampleService.core.acls import AdminPermission as _AdminPermission
from SampleService.core.sample import SampleAddress as _SampleAddress
from SampleService.core.user import UserID as _UserID

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
    VERSION = "0.1.0-alpha12"
    GIT_URL = "https://github.com/mrcreosote/sample_service.git"
    GIT_COMMIT_HASH = "aa92b29ce2944ae2c1da66fd2eb768f71c8bb62f"

    #BEGIN_CLASS_HEADER
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self._samples, self._user_lookup = _build_samples(config)
        #END_CONSTRUCTOR

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
           0.), parameter "prior_version" of Long, parameter "as_admin" of
           type "boolean" (A boolean value, 0 for false, 1 for true.),
           parameter "as_user" of type "user" (A user's username.)
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
        s = self._samples.get_sample(id_, _UserID(ctx[_CTX_USER]), ver, as_admin=admin)
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
        admin = _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.READ,
            # pretty annoying to test ctx.log_info is working, do it manually
            'get_sample_acls', ctx.log_info, skip_check=not params.get('as_admin'))
        acls_ret = self._samples.get_sample_acls(id_, _UserID(ctx[_CTX_USER]), as_admin=admin)
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
           modify. acls - the ACLs to set on the sample. as_admin - replace
           the sample acls regardless of ACL contents as long as the user has
           full administration permissions.) -> structure: parameter "id" of
           type "sample_id" (A Sample ID. Must be globally unique. Always
           assigned by the Sample service.), parameter "acls" of type
           "SampleACLs" (Access control lists for a sample. Access levels
           include the privileges of the lower access levels. owner - the
           user that created and owns the sample. admin - users that can
           administrate (e.g. alter ACLs) the sample. write - users that can
           write (e.g. create a new version) to the sample. read - users that
           can view the sample.) -> structure: parameter "owner" of type
           "user" (A user's username.), parameter "admin" of list of type
           "user" (A user's username.), parameter "write" of list of type
           "user" (A user's username.), parameter "read" of list of type
           "user" (A user's username.), parameter "as_admin" of type
           "boolean" (A boolean value, 0 for false, 1 for true.)
        """
        # ctx is the context object
        #BEGIN replace_sample_acls
        id_ = _get_id_from_object(params, required=True)
        acls = _acls_from_dict(params)
        admin = _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.FULL,
            # pretty annoying to test ctx.log_info is working, do it manually
            'replace_sample_acls', ctx.log_info, skip_check=not params.get('as_admin'))
        self._samples.replace_sample_acls(id_, _UserID(ctx[_CTX_USER]), acls, as_admin=admin)
        #END replace_sample_acls

    def get_metadata_key_static_metadata(self, ctx, params):
        """
        Get static metadata for one or more metadata keys.
            The static metadata for a metadata key is metadata *about* the key - e.g. it may
            define the key's semantics or denote that the key is linked to an ontological ID.
            The static metadata does not change without the service being restarted. Client
            caching is recommended to improve performance.
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
           sample. The UnspecifiedObject map values MUST be a primitive type
           - either int, float, string, or equivalent typedefs.) -> mapping
           from type "metadata_key" (A key in a metadata key/value pair. Less
           than 1000 unicode characters.) to mapping from type
           "metadata_value_key" (A key for a value associated with a piece of
           metadata. Less than 1000 unicode characters. Examples: units,
           value, species) to unspecified object
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
        """
        # ctx is the context object
        #BEGIN create_data_link
        duid, sna, update = _create_data_link_params(params)
        as_admin, user = _get_admin_request_from_object(params, 'as_admin', 'as_user')
        _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.FULL,
            # pretty annoying to test ctx.log_info is working, do it manually
            'create_data_link', ctx.log_info, as_user=user, skip_check=not as_admin)
        self._samples.create_data_link(
            user if user else _UserID(ctx[_CTX_USER]),
            duid,
            sna,
            update,
            as_admin=as_admin)
        #END create_data_link

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
           ensure reproducible results.) -> structure: parameter "links" of
           list of type "DataLink" (A data link from a KBase workspace object
           to a sample. upa - the workspace UPA of the linked object. dataid
           - the dataid of the linked data, if any, within the object. If
           omitted the entire object is linked to the sample. id - the sample
           id. version - the sample version. node - the sample node.
           createdby - the user that created the link. created - the time the
           link was created. expiredby - the user that expired the link, if
           any. expired - the time the link was expired, if at all.) ->
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
           less than 255 characters.), parameter "createdby" of type "user"
           (A user's username.), parameter "created" of type "timestamp" (A
           timestamp in epoch milliseconds.), parameter "expiredby" of type
           "user" (A user's username.), parameter "expired" of type
           "timestamp" (A timestamp in epoch milliseconds.), parameter
           "effective_time" of type "timestamp" (A timestamp in epoch
           milliseconds.)
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN get_data_links_from_sample
        sid, ver = _get_sample_address_from_object(params, version_required=True)
        dt = _get_datetime_from_epochmillseconds_in_object(params, 'effective_time')
        admin = _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.READ,
            # pretty annoying to test ctx.log_info is working, do it manually
            'get_data_links_from_sample', ctx.log_info, skip_check=not params.get('as_admin'))
        links, ts = self._samples.get_links_from_sample(
            _UserID(ctx[_CTX_USER]), _SampleAddress(sid, ver), dt, as_admin=admin)
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
           less than 255 characters.), parameter "createdby" of type "user"
           (A user's username.), parameter "created" of type "timestamp" (A
           timestamp in epoch milliseconds.), parameter "expiredby" of type
           "user" (A user's username.), parameter "expired" of type
           "timestamp" (A timestamp in epoch milliseconds.), parameter
           "effective_time" of type "timestamp" (A timestamp in epoch
           milliseconds.)
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN get_data_links_from_data
        upa = _get_upa_from_object(params)
        dt = _get_datetime_from_epochmillseconds_in_object(params, 'effective_time')
        admin = _check_admin(
            self._user_lookup, ctx[_CTX_TOKEN], _AdminPermission.READ,
            # pretty annoying to test ctx.log_info is working, do it manually
            'get_data_links_from_data', ctx.log_info, skip_check=not params.get('as_admin'))
        links, ts = self._samples.get_links_from_data(
            _UserID(ctx[_CTX_USER]), upa, dt, as_admin=admin)
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
        #BEGIN get_sample_via_data
        upa = _get_upa_from_object(params)
        sid, ver = _get_sample_address_from_object(params, version_required=True)
        sample = self._samples.get_sample_via_data(
            _UserID(ctx[_CTX_USER]), upa, _SampleAddress(sid, ver))
        sample = _sample_to_dict(sample)
        #END get_sample_via_data

        # At some point might do deeper type checking...
        if not isinstance(sample, dict):
            raise ValueError('Method get_sample_via_data return value ' +
                             'sample is not type dict as required.')
        # return the results
        return [sample]

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
