from typing import Protocol


class Message(Protocol):

    def error(self) -> str | None:
        ...

    def value(self) -> bytes:
        ...

    def key(self) -> str:
        ...

    def topic(self) -> str:
        ...
