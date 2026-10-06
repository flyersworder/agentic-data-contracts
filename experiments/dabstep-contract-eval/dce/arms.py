"""Compatibility re-exports. DABStep's arms live in `dce.benchmarks.dabstep`;
the machinery every benchmark shares lives in `dce.tools`. Kept so the
imports in `analysis/`, `deploy/README.md` and older commands keep working.
"""

from dce.benchmarks.dabstep import (  # noqa: F401
    ALL_ARMS,
    ARMS,
    BASE_PROMPT,
    DATA_NOTE,
    EXTRA_ARMS,
    GOVERNED_ARMS,
    build_arm,
)
from dce.tools import (  # noqa: F401
    HARNESS_MEMORY_LIMIT,
    HARNESS_QUERY_SECONDS,
    MAX_ROWS,
    ArmSetup,
    IntegrityCheck,
    _BoundedDuckDBAdapter,
    _governed_tools,
    _ungoverned_tools,
    check_and_restore,
    make_working_copy,
)
