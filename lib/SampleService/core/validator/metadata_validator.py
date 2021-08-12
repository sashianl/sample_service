'''
Contains a Sample Service metadata validator class.
'''

import maps as _maps
from typing import Dict, List, Callable, Optional, Tuple as _Tuple
from pygtrie import CharTrie as _CharTrie
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.core_types import PrimitiveType
from SampleService.core.errors import MetadataValidationError as _MetadataValidationError
from SampleService.core.errors import IllegalParameterError as _IllegalParameterError
from SampleService.core.validator.builtin import ValidatorMessage


class MetadataValidator:
    '''
    A validator for a unit of metadata.

    Only one of validators and prefix_validators will be populated; the other will be an
    empty tuple.

    The arguments to each standard validator are the metadata key and the value mapped from
    the key.

    The return value of the validators is an error string if the validation failed or None if
    it passed.

    The arguments to each prefix validator are the metadata key, the prefix key that
    matched the key, and the value mapped from the metadata key.

    :ivar key: The metadata key to which the validator is assigned.
    :ivar validators: The list of validators.
    :ivar prefix_validators: The list of prefix validators. These validators match any
        metadata key for which the provided key is a prefix, and their validators will be run on
        any matching key.
    :ivar metadata: Metadata about the key; e.g. defining the key semantics or relation to
        an ontology.
    '''

    def __init__(
            self,
            key: str,
            validators: List[Callable[[str, Dict[str, PrimitiveType]], Optional[ValidatorMessage]]] = None,
            prefix_validators: List[
                Callable[[str, str, Dict[str, PrimitiveType]], Optional[ValidatorMessage]]] = None,
            metadata: Dict[str, PrimitiveType] = None):
        '''
        Create the validator. Exactly one of the validators or prefix_validators arguments
        must be supplied and must contain at least one validator.

        The arguments to each standard validator are the metadata key and the value mapped
        from the key.

        The arguments to each prefix validator are the metadata key, the prefix key that
        matched the key, and the value mapped from the metadata key.

        The return value of the validators is an error string if the validation failed or None if
        it passed.

        :param key: The metadata key that this validator will validate.
        :param validators: The metadata validator callables.
        :param prefix_validators: The metadata prefix validators. These validators match any
            metadata key for which the provided key is a prefix, and their validators will be
            run on any matching key.
        :param metadata: Metadata about the key; e.g. defining the key semantics or relation to
            an ontology.
        '''
        # may want a builder for this?
        self.key = _not_falsy(key, 'key')
        if not (bool(validators) ^ bool(prefix_validators)):  # xor
            raise ValueError('Exactly one of validators or prefix_validators must be supplied ' +
                             'and must contain at least one validator')
        self.validators = tuple(validators if validators else [])
        self.prefix_validators = tuple(prefix_validators if prefix_validators else [])
        self.metadata = metadata if metadata else {}

    def is_prefix_validator(self):
        '''
        Check if this validator is a prefix validator.

        :returns: True if this validator is a prefix validator, False otherwise.
        '''
        return bool(self.prefix_validators)


class MetadataValidatorSet:
    '''
    A set of validators of metadata.
    '''

    def __init__(self, validators: List[MetadataValidator] = None):
        '''
        Create the validator set.

        :param validators: The validators.
        '''
        vals: Dict[str, _Tuple[Callable[[str, Dict[str, PrimitiveType]], Optional[ValidatorMessage]], ...]] = {}
        pvals: Dict[
            str, _Tuple[Callable[[str, str, Dict[str, PrimitiveType]], Optional[ValidatorMessage]], ...]] = {}
        self._vals_meta = {}
        self._prefix_vals_meta = {}
        for v in (validators if validators else []):
            if v.is_prefix_validator():
                if v.key in pvals:
                    raise ValueError(f'Duplicate prefix validator: {v.key}')
                pvals[v.key] = v.prefix_validators
                self._prefix_vals_meta[v.key] = v.metadata
            else:
                if v.key in vals:
                    raise ValueError(f'Duplicate validator: {v.key}')
                vals[v.key] = v.validators
                self._vals_meta[v.key] = v.metadata
        self._vals = dict(vals)
        self._prefix_vals = _CharTrie(pvals)

    def keys(self):
        '''
        Get the keys with assigned metadata validators.
        :returns: the metadata keys.
        '''
        return list(self._vals.keys())

    def prefix_keys(self):
        '''
        Get the keys with assigned prefix metadata validators.
        :returns: the metadata keys.
        '''
        return self._prefix_vals.keys()

    def key_metadata(self, keys: List[str]) -> Dict[str, Dict[str, PrimitiveType]]:
        '''
        Get any metdata associated with the specified keys.

        :param keys: The keys to query.
        :returns: A mapping of keys to their metadata.
        :raises IllegalParameterError: if one of the provided keys does not exist in this
            validator.
        '''
        return self._key_metadata(keys, self._vals_meta, '')

    def _key_metadata(self, keys, meta, name_):
        if keys is None:
            raise ValueError('keys cannot be None')
        ret = {}
        for k in keys:
            if k not in meta:
                raise _IllegalParameterError(f'No such {name_}metadata key: {k}')
            ret[k] = meta[k]
        return ret

    def prefix_key_metadata(
            self,
            keys: List[str],
            exact_match: bool = True
            ) -> Dict[str, Dict[str, PrimitiveType]]:
        '''
        Get any metdata associated with the specified prefix keys.

        :param keys: The keys to query.
        :param exact_match: If False, any metadata keys that match a prefix of the given keys
            will be included. If True, the given keys must match metadata keys exactly.
        :returns: A mapping of keys to their metadata.
        :raises IllegalParameterError: if one of the provided keys does not exist in this
            validator.
        '''
        if exact_match:
            return self._key_metadata(keys, self._prefix_vals_meta, 'prefix ')
        else:
            if keys is None:
                raise ValueError('keys cannot be None')
            ret = {}
            for k in keys:
                if not self._prefix_vals.shortest_prefix(k):
                    raise _IllegalParameterError(f'No prefix metadata keys matching key {k}')
                for p in self._prefix_vals.prefixes(k):
                    ret[p.key] = self._prefix_vals_meta[p.key]
            return ret

    def validator_count(self, key: str):
        '''
        Get the number of validators assigned to a key.
        :param key: the key to query.
        :returns: the number of validators assigned to the key.
        '''
        if not self._vals.get(key):
            raise ValueError(f'No validators for key {key}')
        return len(self._vals[key])

    def prefix_validator_count(self, prefix: str):
        '''
        Get the number of validators assigned to a prefix key.
        :param prefix: the key to query.
        :returns: the number of validators assigned to the key.
        '''
        if not self._prefix_vals.get(prefix):
            raise ValueError(f'No prefix validators for prefix {prefix}')
        return len(self._prefix_vals[prefix])

    def call_validator(
            self,
            key: str,
            index: int,
            value: Dict[str, PrimitiveType]
            ) -> Optional[ValidatorMessage]:
        '''
        Call a particular validator for a metadata key.
        :param key: the key for which a validator should be called.
        :param index: the index of the validator in the list of validators.
        :param value: the metdata value to pass to the validator as an argument.
        :returns: the return value of the validator.
        '''
        if not self._vals.get(key):
            raise ValueError(f'No validators for key {key}')
        if index >= len(self._vals[key]):
            raise IndexError(
                f'Requested validator index {index} for key {key} but maximum index ' +
                f'is {len(self._vals[key]) - 1}')
        return self._vals[key][index](key, value)

    def call_prefix_validator(
            self,
            prefix: str,
            index: int,
            key: str,
            value: Dict[str, PrimitiveType]
            ) -> Optional[ValidatorMessage]:
        '''
        Call a particular validator for a metadata key prefix.
        :param prefix: the prefix for which a valiator should be called.
        :param index: the index of the validator in the list of validators.
        :param key: the key to to pass to the validator as an argument.
        :param value: the metdata value to pass to the validator as an argument.
        :returns: the return value of the validator.
        '''
        if not self._prefix_vals.get(prefix):
            raise ValueError(f'No prefix validators for prefix {prefix}')
        if index >= len(self._prefix_vals[prefix]):
            raise IndexError(
                f'Requested validator index {index} for prefix {prefix} but maximum ' +
                f'index is {len(self._prefix_vals[prefix]) - 1}')
        return self._prefix_vals[prefix][index](prefix, key, value)

    def build_error_detail(self, message, dev_message=None, node=None, key=None, subkey=None, sample=None):
        return {
            'message': message,
            'dev_message': dev_message if dev_message!=None else message,
            'key': key,
            'subkey': subkey,
            'node': node,
            'sample_name': sample
        }

    def validate_metadata(
            self,
            metadata: Dict[str, Dict[str, PrimitiveType]],
            return_error_detail: bool=False
        ):
        '''
        Validate a set of metadata key/value pairs.

        :param metadata: the metadata.
        :raises MetadataValidationError: if the metadata is invalid.

        :returns: list of errors raised during validation
        '''
        # if not isinstance(metadata, dict):  # doesn't work
        if type(metadata) != dict and type(metadata) != _maps.FrozenMap:
            raise ValueError('metadata must be a dict')
        errors = []
        for k in metadata:
            sp = self._prefix_vals.shortest_prefix(k)
            if not self._vals.get(k) and not (sp and sp.value):
                if return_error_detail:
                    errors.append(
                        self.build_error_detail(
                            f'Cannot validate controlled field "{k}", no matching validator found',
                            key=k
                        )
                    )
                else:
                    raise _MetadataValidationError(
                        f'No validator available for metadata key {k}')
            for valfunc in self._vals.get(k, []):
                ret = valfunc(k, metadata[k])
                if ret:
                    try:
                        msg: str = ret['message']
                        subkey: Optional[str] = ret['subkey']
                    except:
                        msg = str(ret)
                        subkey = None
                    if return_error_detail:
                        errors.append(
                            self.build_error_detail(
                                f'Validation failed: "{msg}"',
                                dev_message=f'Key {k}: {msg}',
                                key=k,
                                subkey=subkey
                            )
                        )
                    else:
                        raise _MetadataValidationError(f'Key {k}: ' + msg)
            for p in self._prefix_vals.prefixes(k):
                for f in p.value:
                    error = f(p.key, k, metadata[k])
                    if error:
                        if return_error_detail:
                            errors.append(
                                self.build_error_detail(
                                    f'Validation failed: "{error}" from validator for prefix "{p.key}"',
                                    dev_message=f'Prefix validator {p.key}, key {k}: {error}',
                                    key=k
                                )
                            )
                        else:
                            raise _MetadataValidationError(
                                f'Prefix validator {p.key}, key {k}: {error}')
        return errors
