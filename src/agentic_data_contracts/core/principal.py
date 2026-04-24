"""Principal resolver — normalizes static strings and zero-arg callables."""

from __future__ import annotations

from collections.abc import Callable

Principal = str | Callable[[], "str | None"] | None


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
        # ty can't narrow `p` after the `callable()` guard when Principal
        # is a union including str; the call is safe because the guard
        # above confirmed p is a Callable.
        return p()  # ty: ignore[call-top-callable]
    return p
