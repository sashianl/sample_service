# Configuration

The server has several startup parameters beyond the standard SDK-provided parameters that must be configured in the Catalog Service by a Catalog Service administrator in order for the service to run. These are documented in the `deploy.cfg` file.

## Configuration File

The `deploy.cfg` configuration file contains a key, `metadata-validator-config-repo`, that if provided must be a relative GitHub path that points to a validator configuration GitHub repo.

Setting `github-token` will help to avoid any rate limiting that may occur (1k/hr vs 60/hr requests.)

The configuration repo should have chronological releases containing a configuration file. This file's name can be specified with `metadata-validator-config-filename` (`metadata_validation.yml` by default).

The most recent release from the specified repo will be loaded. If pre-releases should also be included, set the `metadata-validator-config-prerelease` config variable to 'true'. 

A direct file URL override can also be provided with the `metadata-validator-config-url` key. With this form, the url begins with `file://`, followed by a path to the directory containing the validation config file, which should be named `metadata_validation.yml` (unless overridden as described above.) This is utilized by tests.

The configuration file is loaded on service startup and used to configure the metadata validators. If changes are made to the configuration file the service must be restarted to reconfigure the validators.

The configuration file uses the YAML format and is validated against the following JSONSchema:

```json
{
  "type": "object",
  "definitions": {
    "validator_set": {
      "type": "object",
      "additionalProperties": {
        "type": "object",
        "properties": {
          "key_metadata": {
            "type": "object",
            "additionalProperties": {
              "type": [
                "number",
                "boolean",
                "string",
                "null"
              ]
            }
          },
          "validators": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "module": {
                  "type": "string"
                },
                "callable_builder": {
                  "type": "string"
                },
                "parameters": {
                  "type": "object"
                }
              },
              "additionalProperties": false,
              "required": [
                "module",
                "callable_builder"
              ]
            }
          }
        },
        "required": [
          "validators"
        ]
      }
    },
    "additionalProperties": false
  },
  "properties": {
    "validators": {
      "$ref": "#/definitions/validator_set"
    },
    "prefix_validators": {
      "$ref": "#/definitions/validator_set"
    }
  },
  "additionalProperties": false
}
```

The configuration consists of a mapping of standard and prefix metadata keys to a further mapping of metadata key properties, including the list of validator specifications and static metadata about the key. Each validator is run against the metadata value in order. The `module` key is a python import path for the module containing a builder function for the validator, while the `callable_builder` key is the name of the function within the module that can be called to create the validator. `parameters` contains a mapping that is passed directly to the callable builder. The builder is expected to return a callable with the call signature as described previously.

A simple configuration might look like:

```yaml
validators:
    foo:
        validators:
            - module: SampleService.core.validator.builtin
              callable_builder: noop
        key_metadata:
            description: test key
            semantics: none really
    stringlen:
        validators:
            - module: SampleService.core.validator.builtin
              callable_builder: string
              parameters:
                  max-len: 5
            - module: SampleService.core.validator.builtin
              callable_builder: string
              parameters:
                  keys: spcky
                  max-len: 2
        key_metadata:
            description: check that no strings are longer than 5 characters and spcky is <2
prefix_validators:
    gene_ontology_:
        validators:
            - module: geneontology.plugins.kbase
              callable_builder: go_builder
              parameters: 
                  url: https://fake.go.service.org/api/go
                  apitoken: abcdefg-hijklmnop
        key_metadata:
            description: The key value contains a GO ontology ID that is linked to the sample.
            go_url: https://fake.go.service.org/api/go
            date_added_to_service: 2020/3/8
```

In this case any value for the `foo` key is allowed, as the `noop` validator is assigned to the key. Note that if no validator was assigned to `foo`, including that key in the metadata would cause a validation error. The `stringlen` key has two validators assigned and any metadata under that key must pass both validators.

The first validator ensures that no keys or value strings in in the metadata map are longer than 5 characters, and the second ensures that the value of the `spcky` key is a string of no more than two characters. See the documentation for the `string` validator (below) for more information.

> TODO: we are not using prefix validators; should not encourage their use?

Finally, the wholly fabricated `gene_ontology_` prefix validator will match **any** key starting with `gene_ontology_`. The validator code might look up the suffix of the key, say `GO_0099593`, at the provided url to ensure the suffix matches a legitimate ontology term. Without a prefix validator, a validator would have to be written for each individual ontology term, which is infeasible.

All the metadata keys have static metadata describing the semantics of the keys and other properties that service users might need to properly use the keys.
