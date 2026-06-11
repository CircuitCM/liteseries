from collections.abc import Sequence
from typing import NamedTuple

class Rollback(NamedTuple):
    adjust_columns: Sequence[str]
    included_keys: dict[str, set[str | None]] | None = ...
    rebuild: bool = ...
