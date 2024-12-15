from typing import Any, Protocol


class CRDT(Protocol):
    @staticmethod
    def from_serializable(value: Any) -> "CRDT": ...

    def to_serializable(self) -> Any: ...

    def read(self) -> Any: ...

    def merge(self, value: Any) -> "CRDT": ...

    def add(self, element: Any) -> "CRDT": ...
