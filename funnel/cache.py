import enum
import json
import typing

import fsspec
import pydantic


class DuplicateKeyEnum(str, enum.Enum):
    skip = 'skip'
    overwrite = 'overwrite'
    check_collision = 'check_collision'
    raise_error = 'raise_error'


@pydantic.dataclasses.dataclass
class CacheStore:
    path: str
    read: bool = True
    write: bool = True
    on_duplicate_key: DuplicateKeyEnum = 'skip'
    storage_options: typing.Dict = None

    def __post_init_post_parse__(self):
        self.storage_options = {} if self.storage_options is None else self.storage_options
        self.mapper = fsspec.get_mapper(self.path, **self.storage_options)
        self.fs = self.mapper.fs
        self.raw_path = self.fs._strip_protocol(self.path)
        self.protocol = self.fs.protocol

    def get(self, key: str):
        if self.protocol == 'memory':
            data = self.mapper[key]
            return json.loads(data)

    def __contains__(self, key: str):
        return key in self.mapper

    def keys(self):
        return list(self.mapper.keys())

    def put(self, key, value, serializer=None):
        method = getattr(self, f'_put_{self.on_duplicate_key.value}')
        return method(key, value)

    def _put_skip(self, key, value, serializer=None):
        if key not in self:
            self._put_overwrite(key, value, serializer)

    def _put_overwrite(self, key, value, serializer=None):
        with self.fs.transaction:
            with self.fs.open(key, 'w') as f:
                f.write(json.dumps(value))
