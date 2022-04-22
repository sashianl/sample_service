# Validation

Each node in the sample tree accepted by the `create_sample` method may contain controlled and
user metadata. User metadata is not validated other than very basic size checks, while controlled
metadata is validated based on configured validation rules.

## All metadata

For all metadata, map keys are are limited to 256 characters and values are limited to 1024
characters. Keys may not contain any control characters, while values may contain tabs and
new lines.

## Controlled metadata

Controlled metadata is subject to validation - no metadata is allowed that does not pass
validation or does not have a validator assigned.

Metadata validators are modular and can be added to the service via configuration without
changing the service core code. Multiple validators can be assigned to each metadata key.

Sample metadata has the following structure (also see the service spec file):

```json
{
    "metadata_key_1": {
        "metadata_value_key_1_1": "metadata_value_1_1",
        "metadata_value_key_1_N": "metadata_value_1_N",
    },
    "metadata_key_N": {
        "metadata_value_key_N_1": "metadata_value_N_1",
        "metadata_value_key_N_N": "metadata_value_N_N",
    }
}
```

Metadata values are primitives: a string, float, integer, or boolean.

A simple example:

```json
{"temperature": {"measurement": 1.0,
                 "units": "Kelvin"
                 },
 "location": {"name": "Castle Geyser",
              "lat": 44.463816,
              "long": -110.836471
              }
}
```

In this case, a validator would need to be assigned to the `temperature` and `location`
metadata keys. Validators are `python` callables that accept the key and the value of the key as
callable parameters. E.g. in the case of the `temperature` key, the arguments to the function
would be:

```json
("temperature", {"measurement": 1.0, "units": "Kelvin"})
```

If the metadata is incorrect, the validator should return an error message as a string. Otherwise
it should return `None` unless the validator cannot validate the metadata due to some
uncontrollable error (e.g. it can't connect to an external server after a reasonable timeout),
in which case it should throw an exception.

 Validators are built by a builder function specified in the configuration (see below).
 The builder is passed any parameters specified in the configuration as a
 mapping. This allows the builder function to set up any necessary state for the validator
 before returning the validator for use. Examine the validators in
`SampleService.core.validator.builtin` for examples. A very simple example might be:

 ```python
 def enum_builder(params: Dict[str, str]
        ) -> Callable[[str, Dict[str, Union[float, int, bool, str]]], Optional[str]]:
    # should handle errors better here
    enums = set(params['enums'])
    valuekey = params['key']

    def validate_enum(key: str, value: Dict[str, Union[float, int, bool, str]]) -> Optional[str]:
        # key parameter not needed in this case
        if value.get(valuekey) not in enums:
            return f'Illegal value for key {valuekey}: {value.get(valuekey)}'
        return None

    return validate_enum
```

## Prefix validators

The sample service supports a special class of validators that will validate any keys that match
a specified prefix, as opposed to standard validators that only validate keys that match exactly.
Otherwise they behave similarly to standard validators except the validator function signature is:

```python
(prefix, key, value)
```

For the temperature example above, if the prefix for the validator was `temp`, the arguments
would be

```python
("temp", "temperature", {"measurement": 1.0, "units": "Kelvin"})
```

A particular metadata key can match one standard validator key (which may have many
validators associated with it) and up to `n` prefix validator keys, where `n` is the length of the
key in characters. Like standard metadata keys, prefix validator keys may have multiple
validators associated with them. The validators are run in the order of the list for a particular
prefix key, but the order the matching prefix keys are run against the metadata key is not
specified.

A toy example of a prefix validators builder function might be:

```python
def chemical_species_builder(params: Dict[str, str]
        ) -> Callable[[Dict[str, str, Union[float, int, bool, str]]], Optional[str]]:
    # or contact an external db or whatever
    chem_db = setup_sqlite_db_wrapper(params['sqlite_file'])
    valuekey = params['key']

    def validate_cs(prefix: str, key: str, value: Dict[str, Union[float, int, bool, str]]
            ) -> Optional[str]:
        species = key[len(prefix):]
        if value[valuekey] != species:
            return f'Species in key {species} does not match species in value {value[valuekey]}'
        if not chem_db.find_chem_species(species):
            return f'No such chemical species: {species}
        return None

    return validate_cs
```

## Source metadata

In some cases, metadata at the data source may be transformed prior to ingest into the
Sample Service - for instance, two samples from different sources may be associated with
metadata items that are semantically equivalent but have different names and are represented in
different units. Prior to storage in the Sample Service, those items may be transformed to use
the same metadata key and representation for the value.

The Sample Service allows storing these source keys and values along with the controlled
metadata such that the original metadata may be reconstructed. The data is not validated other
than basic size checks and is stored on an informational basis only.

See the API specification for more details.

## Static key metadata

A service administrator can define metadata associated with the metadata keys - e.g. metadata
*about* the keys. This might include a text definition, semantic information about the key,
an ontology ID if the key represents a particular node in an ontology, etc. This metadata is
defined in the validator configuration file (see below) and is accessible via the service API.
