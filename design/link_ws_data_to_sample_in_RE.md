# Sample Service / Relation Engine data linking design document

## Background

See [link_ws_data_to_sample.md](link_ws_data_to_sample.md).

## Document purpose

Flesh out Option 1 of the Sample Service data linking options, e.g. link data to samples
in the RE database, not via workspace annotations.

## General design considerations

* A user having access to a sample **does not** mean they necessarily have access to data linked
  to that sample. They must have explicit workspace permissions for the data.
* A user having access to data linked to a sample **does** have read access to the sample.
* When creating a new version of a sample, data links are not automatically updated.
  * This could be a feature in the future, either automatically or on request.
* ~~Data can only be linked to a single sample.~~
  * ~~Although it can be linked to multiple versions of the same sample.~~
  * This is just plain wrong. A data object like a matrix will be linked to 1 sample per
    row / column.
    * Maybe somehow enforce that each single row / column is linked to a single sample

## Definitions

* SS - Sample Service
* WSS - Workspace Service
* RE - Relation Engine
* UPA - Unique Permanent Address. A unique identifier for an object in the workspace.

## Operations

### Link data to a sample

1. Make request to SS with the UPA of the object and the ID of the sample.
    * May want a bulk method, especially for linking columns of a matrix to multiple samples.
    * May need to associate metadata with the link, e.g. denoting to which column of a matrix
      the link refers.
2. SS checks that the user has admin access to the sample.
    * Links from data -> sample grant read permission to the sample, and thus creating links
      is equivalent to having admin privileges for the sample.
3. SS checks that the user has read access to the data.
    * Links grant no special privileges for linked data.
    * What if another stakeholder of the data doesn't want the data linked?
      * Perhaps write or admin access should be required.
    * How do sample sets play into this?
      * My assumption is that having access to a sample only via a sample set **does not** allow
        creating links to the sample.
4. SS adds a link from the WSS shadow object in the RE to the appropriate node in the sample.
    * This probably requires a transaction to ensure the object is not linked to any other samples.
      * Or in the case of objects containing data from multiple samples, ensure that
        each data subunit is not linked to any other samples
        * Links may need a unique ID field in this case that, in combination with the UPA,
          uniquely identifies the subdata within the object.
      * It can be linked to other versions of the same sample.
        * In this case, the old link should be expired (time traveling).
      * What about errors? Expire the old link and make a new link to the new sample?
      * May also need a method for expiring links without making new ones.
      * Not quite sure how to do this yet.

### View data linked to a sample

1. Make a request to the SS with the ID of the sample.
2. The SS checks that the user has read access to the sample.
3. The SS gets the list of workspaces to which the user has read access from the WSS.
4. The SS performs a traversal from the nodes in the sample to WSS shadow objects where the
   workspace ID is in the list of accessible workspaces.
5. The SS returns the list of UPAs.
   * How do we page through this? Could be very large.
     * Any sort of the results of a traversal presumably cannot be done by index, as the natural
       sort order is the order the nodes are encountered in the traversal.
       * That means we're subjecting arango to possible OOMs on these traversals.
     * Maybe return a cursor ID and page though that way.
   * May also want to query on workspace object properties. Node indexes on these properties
     could speed up the query.

### View samples linked from data

1. Make a request to the SS with the UPA of the data.
2. The SS checks that the user has read access to the UPA.
3. The SS performs a traversal from the WSS shadow object in the RE through connected sample nodes
   to the sample tree root, where the ACLs are stored. At minimum the user must be in the
   read ACL of the sample.
   * May need to restructure the ACL data structure in arango to make this doable.
   * May want to query on sample metadata.
     * May need to restructure the sample metadata to make this possible.
       * IIUC, only equality queries can be done on arrays of documents when using an index.
         * Maybe not using an index is ok.
       * On the other hand, if we separate metadata into separate documents that's going to make
         the document count explode and traversals may not be possible.
   * Same questions as above re paging.
   * How does the metadata of parent nodes of the linked node in the sample affect the query?
     * Is metadata inherited? Should we duplicate parent metadata to children in the DB?

### Other operations

* Missing any?

### More complex query examples

* Given a taxon and a sample, find all WSS objects that are linked to both
* Find all genomes with a specific gene linked to sample sets in a subset of workspaces.

## Design implications

* If a WSS object linked to a sample is copied, the copy will not be linked to the sample.


