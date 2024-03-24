from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class AppendEntriesMsg(_message.Message):
    __slots__ = ("prefixLen", "leaderCommit", "suffix", "leaderId", "term", "prefixTerm", "leaseDuration")
    PREFIXLEN_FIELD_NUMBER: _ClassVar[int]
    LEADERCOMMIT_FIELD_NUMBER: _ClassVar[int]
    SUFFIX_FIELD_NUMBER: _ClassVar[int]
    LEADERID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    PREFIXTERM_FIELD_NUMBER: _ClassVar[int]
    LEASEDURATION_FIELD_NUMBER: _ClassVar[int]
    prefixLen: int
    leaderCommit: int
    suffix: _containers.RepeatedScalarFieldContainer[str]
    leaderId: int
    term: int
    prefixTerm: int
    leaseDuration: int
    def __init__(self, prefixLen: _Optional[int] = ..., leaderCommit: _Optional[int] = ..., suffix: _Optional[_Iterable[str]] = ..., leaderId: _Optional[int] = ..., term: _Optional[int] = ..., prefixTerm: _Optional[int] = ..., leaseDuration: _Optional[int] = ...) -> None: ...

class AppendEntriesReply(_message.Message):
    __slots__ = ("term", "success", "ack", "senderId")
    TERM_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ACK_FIELD_NUMBER: _ClassVar[int]
    SENDERID_FIELD_NUMBER: _ClassVar[int]
    term: int
    success: bool
    ack: int
    senderId: int
    def __init__(self, term: _Optional[int] = ..., success: bool = ..., ack: _Optional[int] = ..., senderId: _Optional[int] = ...) -> None: ...

class RequestVoteMsg(_message.Message):
    __slots__ = ("cId", "cTerm", "cLogLength", "cLogTerm")
    CID_FIELD_NUMBER: _ClassVar[int]
    CTERM_FIELD_NUMBER: _ClassVar[int]
    CLOGLENGTH_FIELD_NUMBER: _ClassVar[int]
    CLOGTERM_FIELD_NUMBER: _ClassVar[int]
    cId: int
    cTerm: int
    cLogLength: int
    cLogTerm: int
    def __init__(self, cId: _Optional[int] = ..., cTerm: _Optional[int] = ..., cLogLength: _Optional[int] = ..., cLogTerm: _Optional[int] = ...) -> None: ...

class RequestVoteReply(_message.Message):
    __slots__ = ("term", "voteGranted", "oldLeaderRemainingLease")
    TERM_FIELD_NUMBER: _ClassVar[int]
    VOTEGRANTED_FIELD_NUMBER: _ClassVar[int]
    OLDLEADERREMAININGLEASE_FIELD_NUMBER: _ClassVar[int]
    term: int
    voteGranted: bool
    oldLeaderRemainingLease: int
    def __init__(self, term: _Optional[int] = ..., voteGranted: bool = ..., oldLeaderRemainingLease: _Optional[int] = ...) -> None: ...

class ServeClientArgs(_message.Message):
    __slots__ = ("Request",)
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    Request: str
    def __init__(self, Request: _Optional[str] = ...) -> None: ...

class ServeClientReply(_message.Message):
    __slots__ = ("data", "leaderId", "success")
    DATA_FIELD_NUMBER: _ClassVar[int]
    LEADERID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    data: str
    leaderId: int
    success: bool
    def __init__(self, data: _Optional[str] = ..., leaderId: _Optional[int] = ..., success: bool = ...) -> None: ...
