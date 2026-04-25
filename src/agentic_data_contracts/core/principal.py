"""Principal resolver and access-policy helpers.

Two concerns separated for clarity:

- ``resolve_principal`` — Layer 1: normalises a static string or zero-arg
  callable into the current identity. Stays neutral about access policy;
  passes ``""`` through unchanged.
- ``principal_in_scope`` — Layer 2: the shared allow/block-list predicate
  used by both ``DataContract.allowed_table_names_for`` and per-rule
  principal scoping in ``Validator``. Collapses ``""`` to ``None`` so
  unauthenticated callers fail closed.
"""

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


def principal_in_scope(
    resolved: str | None,
    allowed: list[str] | None,
    blocked: list[str] | None,
) -> bool:
    """Return True if ``resolved`` is permitted by the given allow/block lists.

    Single source of truth for the access-policy semantics also documented in
    ``DataContract.allowed_table_names_for``:

    - Both lists ``None`` → unrestricted, returns True.
    - ``resolved`` empty/None against any restriction → fail closed (False).
      Empty string is treated as unauthenticated; this preserves the
      two-layer invariant where the resolver stays neutral and policy
      lives here.
    - ``allowed`` set → caller must be present in the list.
    - ``blocked`` set → caller must be absent from the list.

    Callers may pass either ``allowed`` or ``blocked`` (or neither), but the
    schema-level mutual-exclusion validators on ``AllowedTable`` and
    ``SemanticRule`` ensure both are never set simultaneously in practice.
    """
    if allowed is None and blocked is None:
        return True
    # Empty string means unauthenticated — fail closed even if "" appears in
    # the allow list. Mirrors the collapse in allowed_table_names_for.
    principal = resolved if resolved else None
    if principal is None:
        return False
    if allowed is not None and principal not in allowed:
        return False
    if blocked is not None and principal in blocked:
        return False
    return True
