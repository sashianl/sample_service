/*
A KBase module: SampleService

Handles creating, updating, retriving samples and linking data to samples.
*/

module SampleService {

    /* A key in a metadata key/value pair. Less than 1000 unicode characters. */
    typedef string metadata_key;

    /* A value in a metadata key/value pair. Less than 1000 unicode characters. */
    typedef string metadata_value;
    
    /* Units for a quantity, e.g. km, mol/g, ppm, etc.
        'None' for unitless quantities.
        Less than 50 unicode characters.
     */
    typedef string units;

    /* A SampleNode ID. Must be unique within a Sample, contain only the characters
      [a-zA-Z0-9_], and be less than 255 characters.
     */
    typedef string node_id;

    /* A Sample ID. Must be globally unique. Always assigned by the Sample service. */
    typedef string sample_id;

    /* The version of a sample. Always > 0. */
    typedef int version;

    /* Metadata attached to a sample. */
    typedef mapping<metadata_key, tuple<metadata_value, units>> metadata;

    /* A node in a sample tree.
        id - the ID of the node. 
        meta_controlled - metadata restricted by the sample controlled vocabulary and validators.
        meta_user - unrestricted metadata.
     */
    typedef structure {
        node_id id;
        metadata meta_controlled;
        metadata meta_user;
    } SampleNode;

    /* A Sample, consisting of a tree of subsamples and replicates.
        id - the ID of the sample.
        single_node - the node in a single node tree.
     */
    typedef structure {
        sample_id id;
        SampleNode single_node;
    } Sample;

    /* A Sample ID and version.
        id - the ID of the sample.
        version - the version of the sample.
     */
    typedef structure {
        sample_id id;
        version version;
    } SampleAddress;

    /* Create a new sample or a sample version.
        If Sample.id is null, a new Sample is created along with a new ID.
        Otherwise, a new version of Sample.id is created. If Sample.id does not exist, an error
          is returned.
     */
    funcdef create_sample(Sample sample) returns(SampleAddress address) authentication required; 
};
