"""Principal resolver — normalizes static strings and zero-arg callables."""

from __future__ import annotations

from collections.abc import Callable
from typing import Union

Principal = Union[str, Callable[[], "str | None"], None]  # noqa: UP007


def resolve_principal(p: Principal) -> str | None:
    """Resolve a Principal to its current string value (or None).

    - ``None`` → ``None``
    - ``str`` → returned unchanged (no case normalization, no trimming)
    - ``Callable`` → invoked and its return value returned unchanged

    A callable that raises propagates the exception — broken identity
    wiring should fail loudly, not silently downgrade to "no caller".
    """
    if p is None:
        return None
    if callable(p):
        return p()  # type: ignore[call-arg]  # ty: ignore[call-top-callable]
    return p
