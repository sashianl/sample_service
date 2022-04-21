#
# Test data
# TODO: Move into separate JSON files
#

# A blank slate sample, for manipulation.
CASE_00 = {
    'sample': {
        'name': 'mysample',
        'node_tree': [{
            'id': 'root',
            'type': 'BioReplicate',
            'meta_controlled': {},
            'meta_user': {},
            'source_meta': []
        }]
    }
}

CASE_01 = {
    'sample': {'name': 'mysample',
               'node_tree': [
                   {
                       'id': 'root',
                       'type': 'BioReplicate',
                       'meta_controlled': {
                           'foo': {'value': 'baz'}
                       },
                       'meta_user': {},
                       'source_meta': [
                           {'key': 'foo', 'skey': 'foo', 'svalue': {'value': 'baz'}}
                       ]
                   }]
               }
}

CASE_02 = {
    'sample': {'name': 'mysample',
               'node_tree': [
                   {
                       'id': 'root2',
                       'type': 'BioReplicate',
                       'meta_controlled': {
                           'stringlentest': {'value': 'baz'}
                       },
                       'meta_user': {},
                       'source_meta': [
                           {'key': 'stringlentest', 'skey': 'stringlentest', 'svalue': {'value': 'baz'}}
                       ]
                   }]
               }
}

# TODO: it doesn't seem right that key is not the prefix key, but the actual key.
# It should be that key is the resolved key, and skey is the original key.
# That is the purpose, after all.
CASE_03 = {
    'sample': {'name': 'mysample',
               'node_tree': [
                   {
                       'id': 'root',
                       'type': 'BioReplicate',
                       'meta_controlled': {
                           'bark': {'value': 'woof'}
                       },
                       'meta_user': {},
                       'source_meta': [
                           {'key': 'bark', 'skey': 'bark', 'svalue': {'value': 'woof'}}
                       ]
                   }]
               }
}

# A sample with multiple nodes -
# This is not a real-life use case, but it is here to exercise some of the
# tests.
CASE_04 = {
    'sample': {'name': 'mysample',
               'node_tree': [
                   {
                       'id': 'root',
                       'type': 'BioReplicate',
                       'meta_controlled': {
                           'foo': {'value': 'baz'}
                       },
                       'meta_user': {},
                       'source_meta': [
                           {'key': 'foo', 'skey': 'foo', 'svalue': {'value': 'baz'}}
                       ]
                   }, {
                       'id': 'subsample',
                       'parent': 'root',
                       'type': 'SubSample',
                       'meta_controlled': {
                           'bar': {'value': 'ter'}
                       },
                       'meta_user': {},
                       'source_meta': [
                           {'key': 'bar', 'skey': 'bar', 'svalue': {'value': 'ter'}}
                       ]
                   }]
               }
}
