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

    /* Metadata attached to a sample.
        The UnspecifiedObject map values MUST be a primitive type - either int, float, string,
        or equivalent typedefs.
     */
    typedef mapping<metadata_key, mapping<metadata_value_key, UnspecifiedObject>> metadata;

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
        meta_user - unrestricted metadata.
     */
    typedef structure {
        node_id id;
        node_id parent;
        samplenode_type type;
        metadata meta_controlled;
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
     */
    typedef structure {
        user owner;
        list<user> admin;
        list<user> write;
        list<user> read;
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
        as_user - save the sample as a different user. The actual user must have full
            administration permissions.
     */
    typedef structure {
        Sample sample;
        int prior_version;
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
    funcdef get_sample(GetSampleParams params) returns (Sample sample) authentication required;

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
        authentication required;

    /* replace_sample_acls parameters.

        id - the ID of the sample to modify.
        acls - the ACLs to set on the sample.
        as_admin - replace the sample acls regardless of ACL contents as long as the user has
            full administration permissions.
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
        */
    typedef structure {
        ws_upa upa;
        data_id dataid;
        sample_id id;
        version version;
        node_id node;
        boolean update;
    } CreateDataLinkParams;

    /* Create a link from a KBase Workspace object to a sample.

        The user must have admin permissions for the sample and write permissions for the
        Workspace object.
     */
    funcdef create_data_link(CreateDataLinkParams params) returns() authentication required;

    /* get_data_links_from_sample parameters.

        id - the sample ID.
        version - the sample version.
        effective_time - the effective time at which the query should be run - the default is
            the current time. Providing a time allows for reproducibility of previous results.
    */
    typedef structure {
        sample_id id;
        version version;
        timestamp effective_time;
    } GetDataLinksFromSampleParams;

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

    /* get_data_links_from_sample_results output.

        links - the links.
    */
    typedef structure {
        list<DataLink> links;
    } GetDataLinksFromSampleResults;

    /* Get data links to Workspace objects originating from a sample.

        The user must have read permissions to the sample. Only Workspace objects the user
        can read are returned.
     */
    funcdef get_data_links_from_sample(GetDataLinksFromSampleParams params)
        returns(GetDataLinksFromSampleResults results) authentication required;

};
