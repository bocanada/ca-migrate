from typing import Protocol

from ca_migrate.xml import Element


class Xoggable(Protocol):
    def as_xml(self) -> Element: ...


class Migrator(Protocol):
    async def migrate(self, content: Xoggable): ...
