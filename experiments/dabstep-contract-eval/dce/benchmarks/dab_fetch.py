"""Fetch DataAgentBench's phase-1 files at the pinned commit.

Experiment 7's materials (docs/superpowers/specs/
2026-11-01-experiment-7-dataagentbench-design.md, "Materials"). Everything is
read from one commit of `github.com/ucbepic/DataAgentBench`: the files Git LFS
stores from `media.githubusercontent.com`, every other file from
`raw.githubusercontent.com`, both at the commit's full SHA. Each LFS file is
checked against the sha256 and size `dataset_manifest.tsv` gives for it, and
the manifest itself against the sha256 pinned here, so a moved or re-uploaded
object stops the fetch. The one data file Git stores directly
(music_brainz_20k's `sales.duckdb`) is pinned here by its sha256.

Files land under `<out>/src/` at their repository paths, and
`<out>/src/fetch-manifest.json` records every file's sha256. None of it is
committed: the golds, validators, query texts and hints stay in the data
directory (`dce.benchmarks.dab_data`).
"""

from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path

REPO = "ucbepic/DataAgentBench"
COMMIT = "bcc2e1b06c708abfde5fa3c69e1e4e514eaaf716"
RAW_URL = f"https://raw.githubusercontent.com/{REPO}/{COMMIT}/"
LFS_URL = f"https://media.githubusercontent.com/media/{REPO}/{COMMIT}/"

#: `dataset_manifest.tsv` at `COMMIT`.
MANIFEST_SHA256 = "b9fc770629644862ac5354fca82253b601f1fc85fbbc9e708918cfe09418eb39"

#: Phase 1's datasets and their query counts (spec, "Benchmark, pinned"):
#: the leaderboard's 54-query set without MongoDB and without PATENTS.
QUERY_COUNTS: dict[str, int] = {
    "bookreview": 3,
    "crmarenapro": 13,
    "DEPS_DEV_V1": 2,
    "GITHUB_REPOS": 4,
    "googlelocal": 4,
    "music_brainz_20k": 3,
    "PANCANCER_ATLAS": 3,
    "stockindex": 3,
    "stockmarket": 5,
}
DATASETS: tuple[str, ...] = tuple(QUERY_COUNTS)

#: Data files Git stores directly rather than in LFS, so the manifest has no
#: hash for them: pinned here, from a checkout at `COMMIT`.
NON_LFS_DATA: dict[str, str] = {
    "query_music_brainz_20k/query_dataset/sales.duckdb": (
        "52ce48c0027eab1b3f85932139425107dd5cf7422217bd8adedfc3edf21ad0e2"
    ),
}

#: DataAgentBench's own helper that some validators import
#: (`from common_scaffold.validate.levenshtein import levenshtein`): a pure
#: standard-library edit distance, pinned here and fetched beside the data.
HELPER_FILES: dict[str, str] = {
    "common_scaffold/validate/levenshtein.py": (
        "77c2cc868169345908261ad9c14bec48f29f6e25f4b690607c2936fefefa67c1"
    ),
}

#: Per dataset, and per query.
DATASET_FILES: tuple[str, ...] = (
    "db_description.txt",
    "db_description_withhint.txt",
    "db_config.yaml",
)
QUERY_FILES: tuple[str, ...] = ("query.json", "validate.py", "ground_truth.csv")

FETCH_MANIFEST = "fetch-manifest.json"

_CHUNK = 1 << 20


def dataset_dir(dataset: str) -> str:
    return f"query_{dataset}"


def query_paths(dataset: str) -> list[str]:
    """Every per-query file of `dataset`, as repository paths."""
    return [
        f"{dataset_dir(dataset)}/query{n}/{name}"
        for n in range(1, QUERY_COUNTS[dataset] + 1)
        for name in QUERY_FILES
    ]


def parse_manifest(text: str) -> dict[str, tuple[str, int]]:
    """`dataset_manifest.tsv`: path, sha256 and size, tab-separated."""
    entries = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        path, sha, size = line.split("\t")
        entries[path] = (sha, int(size))
    return entries


def lfs_files(manifest: dict[str, tuple[str, int]], dataset: str) -> dict:
    """The manifest's entries for one dataset's files."""
    prefix = dataset_dir(dataset) + "/"
    return {p: v for p, v in manifest.items() if p.startswith(prefix)}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(
    url: str,
    dest: Path,
    *,
    sha256: str | None = None,
    size: int | None = None,
    retries: int = 4,
) -> str:
    """Stream `url` to `dest` and return its sha256. A file already at
    `dest` with the expected hash is kept. A hash or size that does not
    match stops the fetch; a network error is retried with backoff."""
    if dest.is_file() and sha256 is not None and sha256_file(dest) == sha256:
        return sha256
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    for attempt in range(retries + 1):
        try:
            digest = hashlib.sha256()
            n = 0
            with (
                urllib.request.urlopen(url, timeout=120) as resp,
                open(part, "wb") as fh,
            ):
                for chunk in iter(lambda: resp.read(_CHUNK), b""):
                    fh.write(chunk)
                    digest.update(chunk)
                    n += len(chunk)
            break
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            if isinstance(exc, urllib.error.HTTPError) and exc.code == 404:
                raise SystemExit(f"not found at the pinned commit: {url}") from exc
            if attempt == retries:
                raise
            time.sleep(2 ** (attempt + 1))
    got = digest.hexdigest()
    if size is not None and n != size:
        part.unlink(missing_ok=True)
        raise SystemExit(f"{url}: {n} bytes, the manifest says {size}")
    if sha256 is not None and got != sha256:
        part.unlink(missing_ok=True)
        raise SystemExit(f"{url}: sha256 {got}, expected {sha256}")
    part.replace(dest)
    return got


def exists(url: str) -> bool:
    request = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=60):
            return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def fetch(
    out: Path,
    datasets: tuple[str, ...] = DATASETS,
    *,
    get: Callable[..., str] = download,
    head: Callable[[str], bool] = exists,
    log: Callable[[str], None] = print,
) -> dict[str, str]:
    """Fetch `datasets` into `out/src/` and return {repository path: sha256},
    also written to `out/src/fetch-manifest.json` (merged with what an
    earlier fetch recorded)."""
    src = out / "src"
    manifest_path = src / "dataset_manifest.tsv"
    get(RAW_URL + "dataset_manifest.tsv", manifest_path, sha256=MANIFEST_SHA256)
    manifest = parse_manifest(manifest_path.read_text(encoding="utf-8"))
    record_path = src / FETCH_MANIFEST
    record: dict[str, str] = (
        json.loads(record_path.read_text(encoding="utf-8"))
        if record_path.is_file()
        else {}
    )
    record["dataset_manifest.tsv"] = MANIFEST_SHA256
    for path, sha in HELPER_FILES.items():
        record[path] = get(RAW_URL + path, src / path, sha256=sha)
    for dataset in datasets:
        if dataset not in QUERY_COUNTS:
            raise SystemExit(f"not a phase-1 dataset: {dataset!r}; one of {DATASETS}")
        # The query count is the spec's; one more query folder at the commit
        # would mean the task set is not the one the spec counted.
        extra = f"{dataset_dir(dataset)}/query{QUERY_COUNTS[dataset] + 1}/query.json"
        if head(RAW_URL + extra):
            raise SystemExit(f"{dataset}: more than {QUERY_COUNTS[dataset]} queries")
        texts = [f"{dataset_dir(dataset)}/{name}" for name in DATASET_FILES]
        for path in texts + query_paths(dataset):
            record[path] = get(RAW_URL + path, src / path)
        for path, (sha, size) in sorted(lfs_files(manifest, dataset).items()):
            log(f"{dataset}: {path} ({size / 1e6:.1f} MB)")
            record[path] = get(LFS_URL + path, src / path, sha256=sha, size=size)
        for path, sha in NON_LFS_DATA.items():
            if path.startswith(dataset_dir(dataset) + "/"):
                record[path] = get(RAW_URL + path, src / path, sha256=sha)
    record_path.write_text(
        json.dumps(dict(sorted(record.items())), indent=1) + "\n", encoding="utf-8"
    )
    return record
