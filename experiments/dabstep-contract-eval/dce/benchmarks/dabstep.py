"""DABStep (Adyen/DABStep, `dce.data.DATASET_REVISION`) as a `Benchmark`.

Every DABStep-specific decision the harness used to make inline lives here:
the arms and their prompts, the frozen contracts each governed arm loads,
the reconstructed golds and their checks, which tasks a sweep runs, and the
scorer. The modules this draws on (`dce.frozen`, `dce.grade`, `dce.golds`,
`dce.data`, `dce.hollow`, `dce.uninterpreted`) stay where they are, so the
commands in README.md and FINDINGS.md keep working.
"""

from __future__ import annotations

import json
from pathlib import Path

from dce.benchmark import Grade, Task
from dce.data import DATASET_REVISION
from dce.frozen import (
    digest,
    hollow_digest,
    load_contract,
    load_hollow_contract,
    load_uninterpreted_contract,
    uninterpreted_digest,
)
from dce.golds import PLURALITY_THRESHOLD, VERIFIED_WRONG_GOLDS, golds_sha256
from dce.tools import ArmSetup, _governed_tools, _ungoverned_tools

#: Where `dce.prepare` puts the vendor's context files; relative to the
#: experiment directory, as the runner's other default paths are.
CONTEXT_DIR = Path("data/hf/data/context")

ARMS: tuple[str, ...] = (
    "schema_only",
    "manual_prompt",
    "contract",
    "contract_hollow",
)

#: Arms run on purpose, one model at a time, and never by default. Kept out of
#: `ARMS` so that a sweep without `--arms`, and every report on the four-arm
#: ablation, stay exactly what they were.
#:
#: `contract_uninterpreted` is the fifth arm the paper's Threats section names:
#: the contract with its `INTERPRETATION` clauses stripped and the empty-list
#: wildcard reverted to the manual's reading (`dce.uninterpreted`).
#:
#: `manual_resolved` is `manual_prompt` plus `DATA_NOTE`, the one fact the
#: contract's author took from the data rather than from the manual. It gives
#: both arms the same knowledge, so what separates it from `contract` is
#: delivery alone.
EXTRA_ARMS: tuple[str, ...] = ("contract_uninterpreted", "manual_resolved")
ALL_ARMS: tuple[str, ...] = ARMS + EXTRA_ARMS

#: What the contract knows that the manual does not. The manual says a null
#: field applies to all values; the annexed `fees` data never uses null for its
#: list-typed fields and stores "all" as an empty list instead (720 of 1,000
#: rules for `account_type`). The contract states this as an INTERPRETATION of
#: `fee_rule_matches_transaction` and builds it into that metric's SQL. The note
#: carries the fact only, once, in words: writing the SQL for it is part of what
#: the contract delivers, and repeating it is a treatment of its own (see
#: `dce.uninterpreted`). Fixed before the arm's first run; never reworded after.
DATA_NOTE = (
    "In the `fees` table, the list-typed fields `account_type`, `aci` and "
    "`merchant_category_code` are never null. A rule that applies to all values "
    "of such a field stores an empty list instead. Treat an empty list the way "
    "the manual treats null: the rule applies to all values of that field."
)

#: Every arm built through `_governed_tools`, and so carrying a contract, a
#: session and the governed-tool counters.
GOVERNED_ARMS: frozenset[str] = frozenset(
    {"contract", "contract_hollow", "contract_uninterpreted"}
)

BASE_PROMPT = (
    "You are a data analyst answering questions over a DuckDB database.\n"
    "Explore the schema, write SQL, and verify your result before answering.\n"
    "Your final message must be the answer alone, formatted exactly as the "
    "question's guidelines require. Do not show working in the final message."
)


def build_arm(arm: str, db_path: Path, docs: dict[str, str]) -> ArmSetup:
    if arm == "schema_only":
        return ArmSetup(BASE_PROMPT, _ungoverned_tools(db_path), None)

    if arm in ("manual_prompt", "manual_resolved"):
        prompt = (
            f"{BASE_PROMPT}\n\n## Domain manual\n\n{docs['manual']}\n\n"
            f"## Payments table reference\n\n{docs['payments_readme']}"
        )
        if arm == "manual_resolved":
            prompt += f"\n\n## Data note\n\n{DATA_NOTE}"
        return ArmSetup(prompt, _ungoverned_tools(db_path), None)

    if arm in GOVERNED_ARMS:
        # IDENTICAL EXCEPT FOR THE CONTRACT OBJECT. The procedural sentence
        # below is the thing `contract_hollow` exists to control for, so it is
        # written once and shared rather than copied — a divergence here would
        # silently reintroduce the confound the control was built to remove.
        contract = {
            "contract": load_contract,
            "contract_hollow": load_hollow_contract,
            "contract_uninterpreted": load_uninterpreted_contract,
        }[arm]()
        tools, session, adapter = _governed_tools(db_path, contract=contract)
        prompt = (
            f"{BASE_PROMPT}\n\n"
            "A data contract governs this database. Look up the domain and the "
            "metrics that apply before writing SQL, and validate every query "
            "with inspect_query before running it.\n\n"
            f"{contract.to_system_prompt()}"
        )
        return ArmSetup(prompt, tools, session, adapter)

    raise ValueError(f"unknown arm: {arm!r}; expected one of {ALL_ARMS}")


def arm_digest(arm: str) -> str:
    """The digest of the contract artifact `arm` actually loads.

    `contract_hollow` loads the mechanically derived hollow contract, so its
    rows must be pinned to `hollow_digest()`. Stamping `digest()` on every row
    regardless -- which this harness did for runs A, B and C -- leaves arm D's
    rows carrying tamper-evidence for a file that arm never read, which is the
    one provenance claim the stamp exists to support. The ungoverned arms load
    no contract at all and keep the real digest as a record of which frozen
    experiment they belong to.
    """
    if arm == "contract_hollow":
        return hollow_digest()
    if arm == "contract_uninterpreted":
        return uninterpreted_digest()
    return digest()


#: How `select_tasks` treats a task with no reconstructed gold.
#: `"skip"` is every scoring sweep: unscoreable tasks are not run.
#: `"run"` is a leaderboard submission, which needs an answer for all 450.
UNGOLDED_MODES: tuple[str, ...] = ("skip", "run")


def select_tasks(tasks: list[dict], golds: dict, ungolded: str = "skip") -> list[dict]:
    """The tasks a sweep will run, given the golds it can score against.

    This used to be an inline comprehension in `main`, and it silently
    decided something load-bearing: 49 of DABStep's 450 tasks have no
    reconstructable gold (`dce.golds`), so a scored sweep runs 401. That is
    right for the ablation and wrong for a leaderboard submission, which is
    graded on all 450 by DABStep's own withheld answers.

    `ungolded="run"` admits them. They are still not scored — `_run_group`
    passes `None` rather than `""`, and `run_task` records `ungraded` — so
    admitting them cannot move an accuracy figure, only fill in answers.
    """
    if ungolded not in UNGOLDED_MODES:
        raise ValueError(f"ungolded must be one of {UNGOLDED_MODES}, got {ungolded!r}")
    if ungolded == "run":
        return list(tasks)
    return [t for t in tasks if t["task_id"] in golds]


def _load_golds(path: Path) -> tuple[dict[str, str], str]:
    """Read the golds envelope and return (task_id -> answer map, gold hash).

    `data/golds.json` is an envelope
    (`{"revision", "threshold", "count", "golds", "submissions_expected",
    "submissions_consumed", "manifest_sha256"}`), not a bare mapping — the
    task -> answer map lives under `"golds"`. Reading it as a bare mapping
    would silently iterate its handful of envelope keys instead of ~406
    tasks; checked explicitly here (raising `SystemExit`, not letting a
    bare mapping fail later with `KeyError('revision')`) so that mistake is
    loud and immediate instead of a confusing crash deep in the sweep.

    The `revision` check is what catches a smoke run and a full sweep being
    scored against two different ground-truth snapshots on the DATASET
    axis. The `threshold` check is its counterpart on the RECONSTRUCTION
    axis: Ruling 8 requires re-running `dce.prepare` at 0.60 / 0.75 / 0.90
    to publish the sensitivity table, and `data/golds.json` is gitignored,
    so an in-place overwrite at another threshold would otherwise be
    invisible to the sweep, to git, and to the results file alike.

    `golds_hash` — stamped into EVERY result row — is
    `dce.golds.golds_sha256`, a fingerprint of the gold mapping itself. It
    used to be `manifest_sha256`, which fingerprints the SUBMISSION CORPUS:
    identical across two gold sets that differ in every answer, because
    they were reconstructed from the same corpus. The stored value is
    verified against a recomputation here rather than trusted, so a
    hand-edited envelope (hash kept, answers changed) is caught too; an
    envelope written before this field existed is simply hashed on the fly.
    `manifest_sha256` stays in the envelope — it still records which corpus
    was consumed, which is a different and also-necessary fact.
    """
    envelope = json.loads(path.read_text())
    if (
        not isinstance(envelope, dict)
        or "golds" not in envelope
        or "revision" not in envelope
    ):
        raise SystemExit(
            f"{path} does not look like a golds envelope (expected top-level "
            '"revision" and "golds" keys) — passed the bare task->answer '
            "mapping instead of the envelope it lives under?"
        )
    if envelope["revision"] != DATASET_REVISION:
        raise SystemExit(
            f"golds revision {envelope['revision']!r} does not match "
            f"dce.data.DATASET_REVISION {DATASET_REVISION!r}; refusing to "
            "score a sweep against a different dataset snapshot than the "
            "one golds.json was reconstructed from"
        )
    if envelope.get("threshold") != PLURALITY_THRESHOLD:
        raise SystemExit(
            f"golds threshold {envelope.get('threshold')!r} does not match "
            f"dce.golds.PLURALITY_THRESHOLD {PLURALITY_THRESHOLD!r}; this "
            "golds.json was reconstructed under a different consensus rule "
            "(a sensitivity run, most likely — see Ruling 8). Re-run "
            "`python -m dce.prepare` to restore the primary gold set before "
            "scoring anything against it"
        )
    computed = golds_sha256(envelope["golds"])
    stored = envelope.get("golds_sha256")
    if stored is not None and stored != computed:
        raise SystemExit(
            f"golds.json's stored golds_sha256 {stored!r} does not match the "
            f"hash of the golds it contains ({computed!r}) — the file has "
            "been edited since it was written; refusing to score against it"
        )
    return envelope["golds"], computed


def load_docs(context_dir: Path = CONTEXT_DIR) -> dict[str, str]:
    """The two vendor documents the manual arms carry verbatim."""
    return {
        "manual": (context_dir / "manual.md").read_text(encoding="utf-8"),
        "payments_readme": (context_dir / "payments-readme.md").read_text(
            encoding="utf-8"
        ),
    }


class DABStep:
    name = "dabstep"
    arms = ARMS
    extra_arms = EXTRA_ARMS
    governed_arms = GOVERNED_ARMS
    roles = {
        "manual": "manual_prompt",
        "manual_plus": "manual_resolved",
        "contract": "contract",
    }
    #: The one pre-registered confirmatory comparison.
    primary = ("deepseek/deepseek-v4-pro-0813", "manual_prompt", "contract")
    group_label = "level"
    #: Golds verified wrong cannot be answered correctly by construction;
    #: the analysis drops their tasks before counting anything.
    excluded_tasks = frozenset(VERIFIED_WRONG_GOLDS)

    def __init__(
        self,
        *,
        db: Path,
        golds: dict[str, str],
        golds_hash: str,
        docs: dict[str, str],
        records: list[dict] | tuple = (),
    ) -> None:
        self._db = db
        self._golds = golds
        self._golds_hash = golds_hash
        self._docs = docs
        self._records = list(records)

    @classmethod
    def from_files(
        cls,
        *,
        db: Path,
        golds_path: Path,
        tasks_path: Path,
        context_dir: Path = CONTEXT_DIR,
        ungolded: str = "skip",
    ) -> DABStep:
        golds, golds_hash = _load_golds(golds_path)
        records = select_tasks(
            json.loads(tasks_path.read_text(encoding="utf-8")),
            golds,
            ungolded=ungolded,
        )
        return cls(
            db=db,
            golds=golds,
            golds_hash=golds_hash,
            docs=load_docs(context_dir),
            records=records,
        )

    @staticmethod
    def task(record: dict) -> Task:
        # The exact user message `run_task` sent before the refactor.
        prompt = (
            f"{record['question']}\n\nAnswer guidelines: {record.get('guidelines', '')}"
        )
        return Task(
            task_id=record["task_id"],
            prompt=prompt,
            group=record.get("level", "unknown"),
            meta=record,
        )

    def tasks(self) -> list[Task]:
        return [self.task(record) for record in self._records]

    def pristine_db(self, task: Task) -> Path:
        return self._db

    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup:
        return build_arm(arm, db, self._docs)

    def arm_digest(self, arm: str, task: Task) -> str:
        return arm_digest(arm)

    def gold_ref(self, task: Task) -> str | None:
        # `.get` WITHOUT a default: a task with no reconstructed gold must
        # read as `None` (recorded `ungraded`), never as `""`, which would
        # be scored and manufacture an `incorrect`. See `select_tasks`.
        return self._golds.get(task.task_id)

    def grade(self, task: Task, answer: str) -> Grade:
        from dce.grade import score

        gold = self.gold_ref(task)
        if gold is None:
            # No gold EXISTS for this task (49 of DABStep's 450 -- see
            # `dce.golds`). Not a wrong answer: `ungraded` stays out of every
            # accuracy denominator in `dce.stats`.
            return Grade("ungraded")
        return Grade("correct" if score(answer, gold) else "incorrect")

    def normalize(self, answer: str) -> str:
        from dce.grade import _clean

        return _clean(answer)

    def golds_hash(self) -> str:
        return self._golds_hash

    def row_fields(self, task: Task) -> dict:
        return {"level": task.meta.get("level", "unknown")}

    @staticmethod
    def scorer() -> str:
        from dce.grade import active_scorer

        return active_scorer()

    @staticmethod
    def rescore(row: dict) -> str | None:
        from dce.grade import score

        if score(row.get("answer", ""), row.get("gold", "")):
            return "correct"
        return "incorrect"

    @staticmethod
    def e2e_correct(row: dict) -> bool:
        from dce.grade import score_final_paragraph

        return bool(score_final_paragraph(row.get("answer"), row.get("gold", "")))
