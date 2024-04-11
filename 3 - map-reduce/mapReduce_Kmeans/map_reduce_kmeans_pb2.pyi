from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Data(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, data: _Optional[_Iterable[float]] = ...) -> None: ...

class MapRequest(_message.Message):
    __slots__ = ("inputStartIndex", "inputEndIndex", "inputFile", "dfHasHeader", "centroids", "numReducers")
    INPUTSTARTINDEX_FIELD_NUMBER: _ClassVar[int]
    INPUTENDINDEX_FIELD_NUMBER: _ClassVar[int]
    INPUTFILE_FIELD_NUMBER: _ClassVar[int]
    DFHASHEADER_FIELD_NUMBER: _ClassVar[int]
    CENTROIDS_FIELD_NUMBER: _ClassVar[int]
    NUMREDUCERS_FIELD_NUMBER: _ClassVar[int]
    inputStartIndex: int
    inputEndIndex: int
    inputFile: str
    dfHasHeader: bool
    centroids: _containers.RepeatedCompositeFieldContainer[Data]
    numReducers: int
    def __init__(self, inputStartIndex: _Optional[int] = ..., inputEndIndex: _Optional[int] = ..., inputFile: _Optional[str] = ..., dfHasHeader: bool = ..., centroids: _Optional[_Iterable[_Union[Data, _Mapping]]] = ..., numReducers: _Optional[int] = ...) -> None: ...

class Reply(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class KeyValueData(_message.Message):
    __slots__ = ("key", "data")
    KEY_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    key: int
    data: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, key: _Optional[int] = ..., data: _Optional[_Iterable[float]] = ...) -> None: ...

class AllKeyValueData(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedCompositeFieldContainer[KeyValueData]
    def __init__(self, data: _Optional[_Iterable[_Union[KeyValueData, _Mapping]]] = ...) -> None: ...

class ReduceInitRequest(_message.Message):
    __slots__ = ("numMappers", "numReducers")
    NUMMAPPERS_FIELD_NUMBER: _ClassVar[int]
    NUMREDUCERS_FIELD_NUMBER: _ClassVar[int]
    numMappers: int
    numReducers: int
    def __init__(self, numMappers: _Optional[int] = ..., numReducers: _Optional[int] = ...) -> None: ...

class ReduceInputRequest(_message.Message):
    __slots__ = ("reducerId",)
    REDUCERID_FIELD_NUMBER: _ClassVar[int]
    reducerId: int
    def __init__(self, reducerId: _Optional[int] = ...) -> None: ...

class Empty(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...
