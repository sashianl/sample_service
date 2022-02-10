/*
A KBase module: SampleService

Handles creating, updating, retriving samples and linking data to samples.

Note that usage of the administration flags will be logged by the service.
*/

module SampleService {

    /* A boolean value, 0 for false, 1 for true. */
    typedef int boolean;

    /* A timestamp in epoch milliseconds. */
    typedef int timestamp;

    /* A user's username. */
    typedef string user;

    /* A SampleNode ID. Must be unique within a Sample and be less than 255 characters.
     */
    typedef string node_id;

    /* The type of a sample node. One of:
        BioReplicate - a biological replicate. Always at the top of the sample tree.
        TechReplicate - a technical replicate.
        SubSample - a sub sample that is not a technical replicate.
     */
     typedef string samplenode_type;

    /* A Sample ID. Must be globally unique. Always assigned by the Sample service. */
    typedef string sample_id;

    /* A link ID. Must be globally unique. Always assigned by the Sample service.
        Typically only of use to service admins.
     */
    typedef string link_id;

    /* A sample name. Must be less than 255 characters. */
    typedef string sample_name;

    /* The version of a sample. Always > 0. */
    typedef int version;

    /* A key in a metadata key/value pair. Less than 1000 unicode characters. */
    typedef string metadata_key;

    /* A key for a value associated with a piece of metadata. Less than 1000 unicode characters.
        Examples: units, value, species
     */
    typedef string metadata_value_key;

    /* A workspace type string.
        Specifies the workspace data type a single string in the format
        [module].[typename]:

        module - a string. The module name of the typespec containing the type.
        typename - a string. The name of the type as assigned by the typedef
            statement.

        Example: KBaseSets.SampleSet
    */
    typedef string ws_type_string;

    /* A metadata value, represented by a mapping of value keys to primitive values. An example for
        a location metadata key might be:
        {
         "name": "Castle Geyser",
         "lat": 44.463816,
         "long": -110.836471
         }
        "primitive values" means an int, float, string, or equivalent typedefs. Including any
        collection types is an error.
     */
    typedef mapping<metadata_value_key, UnspecifiedObject> metadata_value;

    /* Metadata attached to a sample. */
    typedef mapping<metadata_key, metadata_value> metadata;

    /* Information about a metadata key as it appeared at the data source.
        The source key and value represents the original state of the metadata before it was
        tranformed for ingestion by the sample service.

        key - the metadata key.
        skey - the key as it appeared at the data source.
        svalue - the value as it appeared at the data source.
     */
    typedef structure {
        metadata_key key;
        metadata_key skey;
        metadata_value svalue;
    } SourceMetadata;

    /* A KBase Workspace service Unique Permanent Address (UPA). E.g. 5/6/7 where 5 is the
        workspace ID, 6 the object ID, and 7 the object version. */
    typedef string ws_upa;

    /* An id for a unit of data within a KBase Workspace object. A single object may contain
        many data units. A dataid is expected to be unique within a single object. Must be less
        than 255 characters. */
    typedef string data_id;

    /* A node in a sample tree.
        id - the ID of the node.
        parent - the id of the parent node for the current node. BioReplicate nodes, and only
            BioReplicate nodes, do not have a parent.
        type - the type of the node.
        meta_controlled - metadata restricted by the sample controlled vocabulary and validators.
        source_meta - the pre-transformation keys and values of the controlled metadata at the
            data source for controlled metadata keys. In some cases the source metadata
            may be transformed prior to ingestion by the Sample Service; the contents of this
            data structure allows for reconstructing the original representation. The metadata
            here is not validated other than basic size checks and is provided on an
            informational basis only. The metadata keys in the SourceMetadata data structure
            must be a subset of the meta_controlled mapping keys.
        meta_user - unrestricted metadata.
     */
    typedef structure {
        node_id id;
        node_id parent;
        samplenode_type type;
        metadata meta_controlled;
        list<SourceMetadata> source_meta;
        metadata meta_user;
    } SampleNode;

    /* A Sample, consisting of a tree of subsamples and replicates.
        id - the ID of the sample.
        user - the user that saved the sample.
        node_tree - the tree(s) of sample nodes in the sample. The the roots of all trees must
            be BioReplicate nodes. All the BioReplicate nodes must be at the start of the list,
            and all child nodes must occur after their parents in the list.
        name - the name of the sample. Must be less than 255 characters.
        save_date - the date the sample version was saved.
        version - the version of the sample.
     */
    typedef structure {
        sample_id id;
        user user;
        list<SampleNode> node_tree;
        sample_name name;
        timestamp save_date;
        version version;
    } Sample;

    /* Access control lists for a sample. Access levels include the privileges of the lower
        access levels.

        owner - the user that created and owns the sample.
        admin - users that can administrate (e.g. alter ACLs) the sample.
        write - users that can write (e.g. create a new version) to the sample.
        read - users that can view the sample.
        public_read - whether any user can read the sample, regardless of permissions.
     */
    typedef structure {
        user owner;
        list<user> admin;
        list<user> write;
        list<user> read;
        boolean public_read;
    } SampleACLs;

    /* A Sample ID and version.
        id - the ID of the sample.
        version - the version of the sample.
     */
    typedef structure {
        sample_id id;
        version version;
    } SampleAddress;


    /* Parameters for creating a sample.
        If Sample.id is null, a new Sample is created along with a new ID.
        Otherwise, a new version of Sample.id is created. If Sample.id does not exist, an error
          is returned.
        Any incoming user, version or timestamp in the incoming sample is ignored.

        sample - the sample to save.
        prior_version - if non-null, ensures that no other sample version is saved between
            prior_version and the version that is created by this save. If this is not the case,
            the sample will fail to save.
        as_admin - run the method as a service administrator. The user must have full
            administration permissions.
        as_user - create the sample as a different user. Ignored if as_admin is not true. Neither
            the administrator nor the impersonated user need have permissions to the sample if a
            new version is saved.
     */
    typedef structure {
        Sample sample;
        int prior_version;
        boolean as_admin;
        user as_user;
    } CreateSampleParams;

    /* Create a new sample or a sample version. */
    funcdef create_sample(CreateSampleParams params) returns(SampleAddress address)
        authentication required;

    /* get_sample parameters.
        id - the ID of the sample to retrieve.
        version - the version of the sample to retrieve, or the most recent sample if omitted.
        as_admin - get the sample regardless of ACLs as long as the user has administration read
            permissions.
     */
    typedef structure {
        sample_id id;
        version version;
        boolean as_admin;
    } GetSampleParams;

    /* Get a sample. If the version is omitted the most recent sample is returned. */
    funcdef get_sample(GetSampleParams params) returns (Sample sample) authentication optional;

    typedef structure {
        sample_id id;
        version version;
    } SampleIdentifier;

    typedef structure {
        list<SampleIdentifier> samples;
        boolean as_admin;
    } GetSamplesParams;

    funcdef get_samples(GetSamplesParams params) returns (list<Sample> samples) authentication optional;

    /* get_sample_acls parameters.
        id - the ID of the sample to retrieve.
        as_admin - get the sample acls regardless of ACL contents as long as the user has
            administration read permissions.
     */
    typedef structure {
        sample_id id;
        boolean as_admin;
    } GetSampleACLsParams;

    /* Get a sample's ACLs. */
    funcdef get_sample_acls(GetSampleACLsParams params) returns (SampleACLs acls)
        authentication optional;

    /* update_sample_acls parameters.

        id - the ID of the sample to modify.
        admin - a list of users that will receive admin privileges. Default none.
        write - a list of users that will receive write privileges. Default none.
        read - a list of users that will receive read privileges. Default none.
        remove - a list of users that will have all privileges removed. Default none.
        public_read - an integer that determines whether the sample will be set to publicly
            readable:
            > 0: public read.
            0: No change (the default).
            < 0: private.
        at_least - false, the default, indicates that the users should get the exact permissions
            as specified in the user lists, which may mean a reduction in permissions. If true,
            users that already exist in the sample ACLs will not have their permissions reduced
            as part of the ACL update unless they are in the remove list. E.g. if a user has
            write permissions and read permissions are specified in the update, no changes will
            be made to the user's permission.
        as_admin - update the sample acls regardless of sample ACL contents as long as the user has
            full service administration permissions.
     */
    typedef structure {
        sample_id id;
        list<user> admin;
        list<user> write;
        list<user> read;
        list<user> remove;
        int public_read;
        boolean at_least;
        boolean as_admin;
    } UpdateSampleACLsParams;

    /* Update a sample's ACLs.  */
     funcdef update_sample_acls(UpdateSampleACLsParams params) returns() authentication required;

    /* update_samples_acls parameters.

        These parameters are the same as update_sample_acls, except:
        ids - a list of IDs of samples to modify.
    */
    typedef structure {
        list<sample_id> ids;
        list<user> admin;
        list<user> write;
        list<user> read;
        list<user> remove;
        int public_read;
        boolean at_least;
        boolean as_admin;
    } UpdateSamplesACLsParams;

    /* Update the ACLs of many samples.  */
     funcdef update_samples_acls(UpdateSamplesACLsParams params) returns() authentication required;

    /* replace_sample_acls parameters.

        id - the ID of the sample to modify.
        acls - the ACLs to set on the sample.
        as_admin - replace the sample acls regardless of sample ACL contents as long as the user
            has full service administration permissions.
     */
    typedef structure {
        sample_id id;
        SampleACLs acls;
        boolean as_admin;
    } ReplaceSampleACLsParams;

    /* Completely overwrite a sample's ACLs. Any current ACLs are replaced by the provided
        ACLs, even if empty, and gone forever.

        The sample owner cannot be changed via this method.
     */
     funcdef replace_sample_acls(ReplaceSampleACLsParams params) returns() authentication required;

    /* get_metadata_key_static_metadata parameters.

        keys - the list of metadata keys to interrogate.
        prefix -
            0 (the default) to interrogate standard metadata keys.
            1 to interrogate prefix metadata keys, but require an exact match to the prefix key.
            2 to interrogate prefix metadata keys, but any keys which are a prefix of the
                provided keys will be included in the results.
     */
    typedef structure {
        list<metadata_key> keys;
        int prefix;
    } GetMetadataKeyStaticMetadataParams;

    /* get_metadata_key_static_metadata results.

        static_metadata - the static metadata for the requested keys.
     */
    typedef structure {
        metadata static_metadata;
    } GetMetadataKeyStaticMetadataResults;

    /* Get static metadata for one or more metadata keys.

        The static metadata for a metadata key is metadata *about* the key - e.g. it may
        define the key's semantics or denote that the key is linked to an ontological ID.

        The static metadata does not change without the service being restarted. Client caching is
        recommended to improve performance.

     */
    funcdef get_metadata_key_static_metadata(GetMetadataKeyStaticMetadataParams params)
        returns(GetMetadataKeyStaticMetadataResults results) authentication none;

    /* create_data_link parameters.

        upa - the workspace UPA of the object to be linked.
        dataid - the dataid of the data to be linked, if any, within the object. If omitted the
            entire object is linked to the sample.
        id - the sample id.
        version - the sample version.
        node - the sample node.
        update - if false (the default), fail if a link already exists from the data unit (the
            combination of the UPA and dataid). if true, expire the old link and create the new
            link unless the link is already to the requested sample node, in which case the
            operation is a no-op.
        as_admin - run the method as a service administrator. The user must have full
            administration permissions.
        as_user - create the link as a different user. Ignored if as_admin is not true. Neither
            the administrator nor the impersonated user need have permissions to the data or
            sample.
        */
    typedef structure {
        ws_upa upa;
        data_id dataid;
        sample_id id;
        version version;
        node_id node;
        boolean update;
        boolean as_admin;
        user as_user;
    } CreateDataLinkParams;

    /* A data link from a KBase workspace object to a sample.

        upa - the workspace UPA of the linked object.
        dataid - the dataid of the linked data, if any, within the object. If omitted the
            entire object is linked to the sample.
        id - the sample id.
        version - the sample version.
        node - the sample node.
        createdby - the user that created the link.
        created - the time the link was created.
        expiredby - the user that expired the link, if any.
        expired - the time the link was expired, if at all.
     */
    typedef structure {
        link_id linkid;
        ws_upa upa;
        data_id dataid;
        sample_id id;
        version version;
        node_id node;
        user createdby;
        timestamp created;
        user expiredby;
        timestamp expired;
    } DataLink;

    /* create_data_link results.

        new_link - the new link.
     */
    typedef structure {
        DataLink new_link;
    } CreateDataLinkResults;

    /* Create a link from a KBase Workspace object to a sample.

        The user must have admin permissions for the sample and write permissions for the
        Workspace object.
     */
    funcdef create_data_link(CreateDataLinkParams params) returns(CreateDataLinkResults results)
        authentication required;

    /* propagate_data_links parameters.

        id - the sample id.
        version - the sample version. (data links are propagated to)
        previous_version - the previouse sample version. (data links are propagated from)
        ignore_types - the workspace data type ignored from propagating. default empty.
        update - if false (the default), fail if a link already exists from the data unit (the
            combination of the UPA and dataid). if true, expire the old link and create the new
            link unless the link is already to the requested sample node, in which case the
            operation is a no-op.
        effective_time - the effective time at which the query should be run - the default is
            the current time. Providing a time allows for reproducibility of previous results.
        as_admin - run the method as a service administrator. The user must have full
            administration permissions.
        as_user - create the link as a different user. Ignored if as_admin is not true. Neither
            the administrator nor the impersonated user need have permissions to the data or
            sample.
        */
    typedef structure {
        sample_id id;
        version version;
        version previous_version;
        list<ws_type_string> ignore_types;
        boolean update;
        timestamp effective_time;
        boolean as_admin;
        user as_user;
    } PropagateDataLinkParams;

    /* propagate_data_links results.

        links - the links.
     */
    typedef structure {
        list<DataLink> links;
    } PropagateDataLinkResults;

    /* Propagates data links from a previous sample to the current (latest) version

        The user must have admin permissions for the sample and write permissions for the
        Workspace object.
     */
    funcdef propagate_data_links(PropagateDataLinkParams params) returns(PropagateDataLinkResults results)
        authentication required;

    /* expire_data_link parameters.

        upa - the workspace upa of the object from which the link originates.
        dataid - the dataid, if any, of the data within the object from which the link originates.
            Omit for links where the link is from the entire object.
        as_admin - run the method as a service administrator. The user must have full
            administration permissions.
        as_user - expire the link as a different user. Ignored if as_admin is not true. Neither
            the administrator nor the impersonated user need have permissions to the link if a
            new version is saved.
    */
    typedef structure {
        ws_upa upa;
        data_id dataid;
        boolean as_admin;
        user as_user;
    } ExpireDataLinkParams;

    /* Expire a link from a KBase Workspace object.

        The user must have admin permissions for the sample and write permissions for the
        Workspace object.
    */
    funcdef expire_data_link(ExpireDataLinkParams params) returns() authentication required;

    /* get_data_links_from_sample parameters.

        id - the sample ID.
        version - the sample version.
        effective_time - the effective time at which the query should be run - the default is
            the current time. Providing a time allows for reproducibility of previous results.
        as_admin - run the method as a service administrator. The user must have read
            administration permissions.
    */
    typedef structure {
        sample_id id;
        version version;
        timestamp effective_time;
        boolean as_admin;
    } GetDataLinksFromSampleParams;

    /* get_data_links_from_sample results.

        links - the links.
        effective_time - the time at which the query was run. This timestamp, if saved, can be
            used when running the method again to ensure reproducible results. Note that changes
            to workspace permissions may cause results to change over time.
    */
    typedef structure {
        list<DataLink> links;
        timestamp effective_time;
    } GetDataLinksFromSampleResults;

    /* Get data links to Workspace objects originating from a sample.

        The user must have read permissions to the sample. Only Workspace objects the user
        can read are returned.
     */
    funcdef get_data_links_from_sample(GetDataLinksFromSampleParams params)
        returns(GetDataLinksFromSampleResults results) authentication optional;

    /* get_data_links_from_sample_set parameters.
        sample_ids - a list of sample ids and versions
        effective_time - the time at which the query was run. This timestamp, if saved, can be
            used when running the method again to enqure reproducible results. Note that changes
            to workspace permissions may cause results to change over time.
        as_admin - run the method as a service administrator. The user must have read
            administration permissions.
    */

    typedef structure {
        list<SampleIdentifier> sample_ids;
        timestamp effective_time;
        boolean as_admin;
    } GetDataLinksFromSampleSetParams;

    /* Get all workspace object metadata linked to samples in a list of samples or sample set
        refs. Returns metadata about links to data objects. A batch version of
        get_data_links_from_sample.

        The user must have read permissions to the sample. A permissions error is thrown when a
        sample is found that the user has no access to.
    */
    funcdef get_data_links_from_sample_set(GetDataLinksFromSampleSetParams params)
        returns(GetDataLinksFromSampleResults results) authentication optional;

    /* get_data_links_from_data parameters.

        upa - the data UPA.
        effective_time - the effective time at which the query should be run - the default is
            the current time. Providing a time allows for reproducibility of previous results.
        as_admin - run the method as a service administrator. The user must have read
            administration permissions.
    */
    typedef structure {
        ws_upa upa;
        timestamp effective_time;
        boolean as_admin;
    } GetDataLinksFromDataParams;

    /* get_data_links_from_data results.

        links - the links.
        effective_time - the time at which the query was run. This timestamp, if saved, can be
            used when running the method again to ensure reproducible results.
    */
    typedef structure {
        list<DataLink> links;
        timestamp effective_time;
    } GetDataLinksFromDataResults;

    /* Get data links to samples originating from Workspace data.

        The user must have read permissions to the workspace data.
     */
    funcdef get_data_links_from_data(GetDataLinksFromDataParams params)
        returns(GetDataLinksFromDataResults results) authentication optional;

    /* get_sample_via_data parameters.

        upa - the workspace UPA of the target object.
        id - the target sample id.
        version - the target sample version.
    */
    typedef structure {
        ws_upa upa;
        sample_id id;
        version version;
    } GetSampleViaDataParams;

    /* Get a sample via a workspace object. Read permissions to a workspace object grants
        read permissions to all versions of any linked samples, whether the links are expired or
        not. This method allows for fetching samples when the user does not have explicit
        read access to the sample.
    */
    funcdef get_sample_via_data(GetSampleViaDataParams params) returns(Sample sample)
        authentication optional;

    /* get_data_link parameters.

        linkid - the link ID.
     */
    typedef structure {
        link_id linkid;
    } GetDataLinkParams;

    /* Get a link, expired or not, by its ID. This method requires read administration privileges
       for the service.
     */
    funcdef get_data_link(GetDataLinkParams params) returns(DataLink link) authentication required;

    /* Provide sample and run through the validation steps, but without saving them. Allows all the samples to be evaluated for validity first so potential errors can be addressed.
    */

    typedef structure {
        list<Sample> samples;
    } ValidateSamplesParams;

    typedef structure {
        string message;
        string dev_message;
        sample_name sample_name;
        node_id node;
        metadata_key key;
        string subkey;
    } ValidateSamplesError;

    typedef structure {
        list<ValidateSamplesError> errors;
    } ValidateSamplesResults;

    funcdef validate_samples(ValidateSamplesParams params) returns (ValidateSamplesResults results) authentication required;
};
