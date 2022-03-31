# Built-In Validators

All built in validators are in the `SampleService.core.validator.builtin` module.

## noop

Example configuration:

```yaml
validators:
    metadatakey:
        validators:
            - module: SampleService.core.validator.builtin
              callable_builder: noop
```

This validator accepts any and all values.

## string

Example configuration:

```yaml
validators:
    metadatakey:
        validators:
            - module: SampleService.core.validator.builtin
              callable_builder: string
              parameters:
                  keys: ['key1', 'key2']
                  required: True
                  max-len: 10
```

* `keys` is either a string or a list of strings and determines which keys will be checked by the
  validator. If the key exists, its value must be a string or `None` (`null` in JSON-speak).
* `required` requires any keys in the `keys` field to exist in the map, although their value may
  still be `None`.
* `max-len` determines the maximum length in characters of the values of the keys listed in `keys`.
  If `keys` is not supplied, then it determines the maximum length of all keys and string values
  in the metadata value map.

## enum

Example configuration:

```yaml
validators:
    metadatakey:
        validators:
            - module: SampleService.core.validator.builtin
              callable_builder: enum
              parameters:
                  keys: ['key1', 'key2']
                  allowed-values: ['red', 'blue', 'green]
```

* `allowed-values` is a list of primitives - strings, integers, floats, or booleans - that are
  allowed metadata values. If `keys` is not supplied, all values in the metadata value mapping must
  be one of the allowed values.
* `keys` is either a string or a list of strings and determines which keys will be checked by the
  validator. The key must exist and its value must be one of the `allowed-values`.

## units

Example configuration:

```yaml
validators:
    metadatakey:
        validators:
            - module: SampleService.core.validator.builtin
              callable_builder: units
              parameters:
                  key: 'units'
                  units: 'mg/L'
```

* `key` is the metadata value key that will be checked against the `units` specification.
* `units` is a **unit specification in the form of an example**. Any units that can be converted
  to the given units will be accepted. For example, if `units` is `K`, then `degF`, `degC`, and
  `degR` are all acceptable input to the validator. Similarly, if `N` is given, `kg * m / s^2` and
  `lb * f / s^2` are both acceptable.

## number

Example configuration:

```yaml
validators:
    metadatakey:
        validators:
            - module: SampleService.core.validator.builtin
              callable_builder: number
              parameters:
                  keys: ['length', 'width']
                  type: int
                  required: True
                  gte: 42
                  lt: 77
```

Ensures all values are integers or floats.

* `keys`, which is either a string or a list of strings, determines which keys in the metadata value
  map are checked. If omitted, all keys are checked.
* If `required` is specified, the keys in the `keys` list must exist in the metadata value map,
  although their value may be `null`.
* `type` specifies that the number or numbers must be integers if set to `int` or any number if
  omitted or set to `float` or `null`.
* `gt`, `gte`, `lt`, and `lte` are respectively greater than, greater than or equal,
  less than, and less than or equal, and specify a range in which the number or numbers must exist.
  If `gt` or `lt` are specified, `gte` or `lte` cannot be specified, respectively, and vice versa.

## ontology_has_ancestor

Example configuration:

```yaml
validators:
    metadatakey:
        validators:
            - module: SampleService.core.validator.builtin
              callable-builder: ontology_has_ancestor
              parameters:
                  ontology: 'envo_ontology'
                  ancestor_term: 'ENVO:00010483'
                  srv_wiz_url: 'https://kbase.us/services/service_wizard'
```

* `ontology` is the ontology that the meta value will be checked against.
* `ancestor_term` is the ancestor ontology term that will be used to check whether meta value has such ancestor or not.
* `srv_wiz_url` is the kbase service wizard url for getting OntologyAPI service.