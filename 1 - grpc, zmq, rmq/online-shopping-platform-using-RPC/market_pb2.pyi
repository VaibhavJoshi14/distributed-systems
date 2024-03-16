from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SellerRegisterRequest(_message.Message):
    __slots__ = ("uid", "seller_address", "notification_address")
    UID_FIELD_NUMBER: _ClassVar[int]
    SELLER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    NOTIFICATION_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    uid: str
    seller_address: str
    notification_address: str
    def __init__(self, uid: _Optional[str] = ..., seller_address: _Optional[str] = ..., notification_address: _Optional[str] = ...) -> None: ...

class Reply(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class Item(_message.Message):
    __slots__ = ("item_id", "ELECTRONICS", "FASHION", "OTHERS", "product_name", "quantity", "description", "price_per_unit", "rating", "seller_address")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    ELECTRONICS_FIELD_NUMBER: _ClassVar[int]
    FASHION_FIELD_NUMBER: _ClassVar[int]
    OTHERS_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_NAME_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PRICE_PER_UNIT_FIELD_NUMBER: _ClassVar[int]
    RATING_FIELD_NUMBER: _ClassVar[int]
    SELLER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    item_id: int
    ELECTRONICS: bool
    FASHION: bool
    OTHERS: bool
    product_name: str
    quantity: int
    description: str
    price_per_unit: float
    rating: str
    seller_address: str
    def __init__(self, item_id: _Optional[int] = ..., ELECTRONICS: bool = ..., FASHION: bool = ..., OTHERS: bool = ..., product_name: _Optional[str] = ..., quantity: _Optional[int] = ..., description: _Optional[str] = ..., price_per_unit: _Optional[float] = ..., rating: _Optional[str] = ..., seller_address: _Optional[str] = ...) -> None: ...

class NewItem(_message.Message):
    __slots__ = ("seller_uid", "ELECTRONICS", "FASHION", "OTHERS", "product_name", "quantity", "description", "price_per_unit_INR", "seller_address")
    SELLER_UID_FIELD_NUMBER: _ClassVar[int]
    ELECTRONICS_FIELD_NUMBER: _ClassVar[int]
    FASHION_FIELD_NUMBER: _ClassVar[int]
    OTHERS_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_NAME_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PRICE_PER_UNIT_INR_FIELD_NUMBER: _ClassVar[int]
    SELLER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    seller_uid: str
    ELECTRONICS: bool
    FASHION: bool
    OTHERS: bool
    product_name: str
    quantity: int
    description: str
    price_per_unit_INR: float
    seller_address: str
    def __init__(self, seller_uid: _Optional[str] = ..., ELECTRONICS: bool = ..., FASHION: bool = ..., OTHERS: bool = ..., product_name: _Optional[str] = ..., quantity: _Optional[int] = ..., description: _Optional[str] = ..., price_per_unit_INR: _Optional[float] = ..., seller_address: _Optional[str] = ...) -> None: ...

class NewItemReply(_message.Message):
    __slots__ = ("item_id", "fail")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    FAIL_FIELD_NUMBER: _ClassVar[int]
    item_id: int
    fail: str
    def __init__(self, item_id: _Optional[int] = ..., fail: _Optional[str] = ...) -> None: ...

class UpdateItem(_message.Message):
    __slots__ = ("seller_uid", "item_id", "new_price", "new_quantity", "seller_address")
    SELLER_UID_FIELD_NUMBER: _ClassVar[int]
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    NEW_PRICE_FIELD_NUMBER: _ClassVar[int]
    NEW_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    SELLER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    seller_uid: str
    item_id: int
    new_price: float
    new_quantity: int
    seller_address: str
    def __init__(self, seller_uid: _Optional[str] = ..., item_id: _Optional[int] = ..., new_price: _Optional[float] = ..., new_quantity: _Optional[int] = ..., seller_address: _Optional[str] = ...) -> None: ...

class DeleteItem(_message.Message):
    __slots__ = ("seller_uid", "item_id", "seller_address")
    SELLER_UID_FIELD_NUMBER: _ClassVar[int]
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    SELLER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    seller_uid: str
    item_id: int
    seller_address: str
    def __init__(self, seller_uid: _Optional[str] = ..., item_id: _Optional[int] = ..., seller_address: _Optional[str] = ...) -> None: ...

class DisplayAllRequest(_message.Message):
    __slots__ = ("seller_uid", "seller_address")
    SELLER_UID_FIELD_NUMBER: _ClassVar[int]
    SELLER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    seller_uid: str
    seller_address: str
    def __init__(self, seller_uid: _Optional[str] = ..., seller_address: _Optional[str] = ...) -> None: ...

class Item2(_message.Message):
    __slots__ = ("item_id", "type", "name", "quantity", "description", "price", "rating", "seller_address", "seller_uid")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    RATING_FIELD_NUMBER: _ClassVar[int]
    SELLER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    SELLER_UID_FIELD_NUMBER: _ClassVar[int]
    item_id: int
    type: str
    name: str
    quantity: int
    description: str
    price: float
    rating: int
    seller_address: str
    seller_uid: str
    def __init__(self, item_id: _Optional[int] = ..., type: _Optional[str] = ..., name: _Optional[str] = ..., quantity: _Optional[int] = ..., description: _Optional[str] = ..., price: _Optional[float] = ..., rating: _Optional[int] = ..., seller_address: _Optional[str] = ..., seller_uid: _Optional[str] = ...) -> None: ...

class ItemList(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[Item2]
    def __init__(self, items: _Optional[_Iterable[_Union[Item2, _Mapping]]] = ...) -> None: ...

class SearchRequest(_message.Message):
    __slots__ = ("item_name", "client_address", "ELECTRONICS", "FASHION", "OTHERS", "ANY")
    ITEM_NAME_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    ELECTRONICS_FIELD_NUMBER: _ClassVar[int]
    FASHION_FIELD_NUMBER: _ClassVar[int]
    OTHERS_FIELD_NUMBER: _ClassVar[int]
    ANY_FIELD_NUMBER: _ClassVar[int]
    item_name: str
    client_address: str
    ELECTRONICS: bool
    FASHION: bool
    OTHERS: bool
    ANY: bool
    def __init__(self, item_name: _Optional[str] = ..., client_address: _Optional[str] = ..., ELECTRONICS: bool = ..., FASHION: bool = ..., OTHERS: bool = ..., ANY: bool = ...) -> None: ...

class Buy(_message.Message):
    __slots__ = ("item_id", "quantity", "buyer_address")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    BUYER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    item_id: int
    quantity: int
    buyer_address: str
    def __init__(self, item_id: _Optional[int] = ..., quantity: _Optional[int] = ..., buyer_address: _Optional[str] = ...) -> None: ...

class Wish(_message.Message):
    __slots__ = ("item_id", "buyer_address", "notification_address", "buyer_uid")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    BUYER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    NOTIFICATION_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    BUYER_UID_FIELD_NUMBER: _ClassVar[int]
    item_id: int
    buyer_address: str
    notification_address: str
    buyer_uid: str
    def __init__(self, item_id: _Optional[int] = ..., buyer_address: _Optional[str] = ..., notification_address: _Optional[str] = ..., buyer_uid: _Optional[str] = ...) -> None: ...

class Rate(_message.Message):
    __slots__ = ("item_id", "rating", "client_address")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    RATING_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    item_id: int
    rating: int
    client_address: str
    def __init__(self, item_id: _Optional[int] = ..., rating: _Optional[int] = ..., client_address: _Optional[str] = ...) -> None: ...
