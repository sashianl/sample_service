/*
A KBase module: SampleService

Handles creating, updating, retriving samples and linking data to samples.
*/

module SampleService {

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
        node_tree - the tree(s) of sample nodes in the sample. The the roots of all trees must
            be BioReplicate nodes. All the BioReplicate nodes must be at the start of the list,
            and all child nodes must occur after their parents in the list.
        name - the name of the sample. Must be less than 255 characters.
        save_date - the date the sample version was saved.
        version - the version of the sample.
     */
    typedef structure {
        sample_id id;
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
        Any incoming version or timestamp in the incoming sample is ignored.

        sample - the sample to save.
        prior_version - if non-null, ensures that no other sample version is saved between
            prior_version and the version that is created by this save. If this is not the case,
            the sample will fail to save.
     */
    typedef structure {
        Sample sample;
        int prior_version;
    } CreateSampleParams;

    /* Create a new sample or a sample version. */
    funcdef create_sample(CreateSampleParams params) returns(SampleAddress address)
        authentication required;

    /* get_sample parameters.
        id - the ID of the sample to retrieve.
        version - the version of the sample to retrieve, or the most recent sample if omitted.
     */
    typedef structure {
        sample_id id;
        version version;
    } GetSampleParams;

    /* Get a sample. If the version is omitted the most recent sample is returned. */
    funcdef get_sample(GetSampleParams params) returns (Sample sample) authentication required;

    /* get_sample_acls parameters. */
    typedef structure {
        sample_id id;
    } GetSampleACLsParams;

    /* Get a sample's ACLs. */
    funcdef get_sample_acls(GetSampleACLsParams params) returns (SampleACLs acls)
        authentication required;

    /* replace_sample_acls parameters.

        id - the ID of the sample to modify.
        acls - the ACLs to set on the sample.
     */
    typedef structure {
        sample_id id;
        SampleACLs acls;
    } ReplaceSampleACLsParams;

    /* Completely overwrite a sample's ACLs. Any current ACLs are replaced by the provided
        ACLs, even if empty, and gone forever.

        The sample owner cannot be changed via this method.
     */
     funcdef replace_sample_acls(ReplaceSampleACLsParams params) returns() authentication required;

};
