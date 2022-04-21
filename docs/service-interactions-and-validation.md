# Interactions with Sample Uploader and validation

Various apps and services rely on SampleService for saving and validating of samples in addition to saving and retrieving samples and data linked to samples the SampleService has a rigorous validation schema that is used to verify user specified values according to specific templates. The SampleService can currently validate int, float, text, and ontological term values. Depending on on the user's sample template format (SESAR, ENIGMA, or KBase), the SampleService will pull the appropriate validation template accordingly.

## How SampleService Pulls Validation Config

This passage is about the current implementation of how the sample service is loaded, and steps required to update it. For information on the schema itself, refer to the top-level `README.md` under the section called "Metadata Validation".

The SampleService pulls its validator config as a static file from [This hosted GitHub URL]("https://raw.githubusercontent.com/kbase/sample_service_validator_config/master/metadata_validation.yml") and the actual validator config can be updated [via GitHub](https://github.com/kbase/sample_service_validator_config).

Currently, the URL for the validator file is set as a hardcoded value inside `dev-server-env.sh`, which will set the above raw content URL as the retrieval source upon starting the service. The value can also be set in `deploy.cfg` under the value `metadata-validator-config-url`. When the service is started, the configuration file will be loaded and transformed into the appropriate validator class.

All validation via this schema for outside apps can be accessed through the exposed `SampleService.validate_samples` method. Additionally, SampleService has built-in class called `MetadataValidatorSet` which handles validation internally for other methods. The internal validation, found at `MetadataValidatorSet._validate_metadata` is currently called inside of `SampleService.create_sample` and `SampleService.save_sample` to ensure new samples or updated samples aren't saved before validating.

## Services that rely on `SampleService.validate_samples`

Currently, sample_uploader is the only repo that calls the `validate_samples` method directly. It is called when importing samples, as well as when it imports samples from IGSN or NCBI. Sample_uploader's prevalidation scheme is optional and can be turned off with the `prevalidate` flag. For more information, refer to the (sample_uploader docs)[https://github.com/kbaseapps/sample_uploader/blob/master/README.md].

# Where to Update SampleService Validator Config

To add new validation templates, go to the Validator Config's [Github](https://github.com/kbase/sample_service_validator_config). To fully add a new validation type, you will need to update in 2 places: in the templates folder and in the vocabularies folder. The templates are what the sample_uploader will use to generate a template with which the user can download and fill out their information. The vocabularies are where the actual new terms are being defined and added to the main collection of terms with which the SampleService validates.

## Adding a Vocabulary Set

All vocabularies are expected to reside within the `vocabularies` folder. Each validator requires a `Terms` key, which is a list of all the terms to be added to the new validation. The schema for a list of terms would look like this:

```yaml
namespace: example
terms:
  nitrate_concentration:
    description: Concentration of Nitrate in Mg/Kg
    examples:
    - 0.0939
    minimum: 0.0
    title: Concentration of Nitrate
    type: number
    units: mg/kg
  description_link:
    description: Link to item description
    examples:
    - http://example.com/i_love_examples
    title: Description Link
    type: string
```

The above configuration would add 2 new validators to the validation shema, assuming that all the required fields are filled out correctly. 

The `namespace` field is for differentiating your own type from the core validators. Setting a namespace will prefix all of your validators with your own namespace to disambiguate it from other validators in the collection.

It is important to run `make update` once you have added your validators or the validator will fail during a build. This command will merge the new validators with the already existing ones at the top level `metadata_validators.yaml`.

## Adding a Template

Templates are what the user will download to populate their information when using the sample_uploader app. The user will see a template generated to the specification of this file, and should map to actual values that are defined within your validator YAML file. If you are familiar with KBase app development, this file is somewhat analogous to a `display.yaml` while the validator file itself play is the business logic role of the `spec.json`

Using the example above, here is how the 2 newly created validators' template columns could look:

```yaml
Columns:
  Concentration of Nitrate in Mg/Kg:
    aliases:
    - nitrate_concentration_mg_kg
    - concentration of nitrate (mg/kg)
    category: measurement
    definition: concentration of nitrate in milligrams per kilogram
    example: '0.0939'
    order: 0
    required: true
    transformations:
    - parameters:
      - example:nitrate_concentration # maps to validator YAML
      - mg/kg
      transform: unit_measurement_fixed
  Link to Item Description:
    aliases:
    - Item description link
    category: description
    definition: Item description link
    example: https://example.com/i_love_examples
    order: 1
    required: false
    transformations:
    - parameters:
      - example:description_link
      transform: map
```

All items within a template fit above schema. To map values to their respective validators, the `transformations.parameters` field is able to map different types to their respective validator. The `transform` property is either `map` for regular values or `unit_measurement_fixed` if unit validation is required. The `order` field is useful for displaying which columns appear first within the template.

It is important to remember to generate this template if you would like to expose it to users, as running any of the build commands will not check for a template file corresponding to a validator.

## Steps for adding new validators outside of the config repo

The only other place required to add a new validator would be within `sample_uploader`. Sample uploader's `import_samples` method requires a user to pick a specific template format with which to use. You will need to add your new template information there for it to appear within the UI.