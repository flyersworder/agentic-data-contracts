# Experiment 7: recovery

Scored per `prep/dataagentbench/RECOVERY-RUBRIC.md` after all 45 author
sessions (`dce.benchmarks.dab_author`, claudeopus5.5; per dataset three draws
without hints, one with them, one without the data), before any agent run.
One checker per dataset, briefed by `prep/dataagentbench/CHECKER-BRIEF.md`,
saw no question, gold, validator or result. Each checker wrote its dataset's
references first; they were committed before the checker opened any draw
(7cdd8c0, 3956c60, 06aea24, c14ddaf, 5471a67, 9f1942d, a3a1916, a7b1339,
933cec1). Then it scored the five draws: `recovery-work/<D>.scores.json`.
`analysis/dab_recovery.py --write` merges them into `recovery.json`; every
figure below is computed from it.

All 45 sessions ended with an accepted contract, and every metric in every
draw runs on the built data. Draw A3's process was killed by the host's
out-of-memory killer during its fourth session (GITHUB_REPOS), which wrote
no contract; the six remaining A3 sessions ran afresh afterwards. Authoring
cost $22.75.

## By item

| item | class | draw-a1 | draw-a2 | draw-a3 | draw-b | no-data |
|---|---|---|---|---|---|---|
| DEPS_DEV_V1-1 | data-discoverable | recovered | recovered | recovered | recovered | recovered |
| DEPS_DEV_V1-2 | data-discoverable | partial | partial | partial | partial | stated, not built |
| DEPS_DEV_V1-3 | data-discoverable | partial | partial | partial | partial | missed |
| GITHUB_REPOS-1 | guidance | recovered | recovered | recovered | recovered | recovered |
| GITHUB_REPOS-2 | described | recovered | recovered | recovered | recovered | recovered |
| GITHUB_REPOS-3 | definition | recovered | recovered | recovered | recovered | missed |
| GITHUB_REPOS-4 | described | recovered | recovered | recovered | recovered | recovered |
| GITHUB_REPOS-5 | guidance | partial | recovered | recovered | recovered | stated, not built |
| PANCANCER_ATLAS-1 | definition | partial | partial | missed | recovered | missed |
| PANCANCER_ATLAS-2 | data-discoverable | recovered | recovered | recovered | recovered | missed |
| PANCANCER_ATLAS-3 | data-discoverable | partial | partial | partial | recovered | missed |
| PANCANCER_ATLAS-4 | definition | missed | missed | missed | recovered | missed |
| PANCANCER_ATLAS-5 | world | recovered | recovered | recovered | recovered | missed |
| bookreview-1 | described | recovered | recovered | recovered | recovered | recovered |
| bookreview-2 | described | recovered | recovered | recovered | recovered | recovered |
| bookreview-3 | data-discoverable | recovered | recovered | recovered | recovered | missed |
| bookreview-4 | guidance | recovered | recovered | recovered | recovered | partial |
| crmarenapro-1 | data-discoverable | partial | partial | partial | partial | missed |
| crmarenapro-2 | data-discoverable | partial | partial | partial | partial | missed |
| crmarenapro-3 | data-discoverable | partial | partial | partial | partial | missed |
| crmarenapro-4 | guidance | recovered | recovered | recovered | recovered | missed |
| crmarenapro-5 | guidance | partial | partial | partial | partial | partial |
| googlelocal-1 | described | recovered | recovered | recovered | recovered | recovered |
| googlelocal-2 | guidance | missed | recovered | recovered | recovered | missed |
| music_brainz_20k-1 | described | recovered | recovered | recovered | recovered | recovered |
| music_brainz_20k-2 | guidance | partial | partial | partial | recovered | stated, not built |
| music_brainz_20k-3 | data-discoverable | stated, not built | stated, not built | stated, not built | stated, not built | missed |
| music_brainz_20k-4 | data-discoverable | stated, not built | stated, not built | stated, not built | stated, not built | missed |
| stockindex-1 | data-discoverable | recovered | recovered | recovered | recovered | stated, not built |
| stockindex-2 | guidance | recovered | recovered | recovered | recovered | partial |
| stockindex-3 | world | missed | missed | missed | recovered | missed |
| stockindex-4 | definition | missed | missed | missed | recovered | missed |
| stockindex-5 | definition | missed | missed | missed | recovered | missed |
| stockmarket-1 | world | recovered | recovered | recovered | recovered | missed |
| stockmarket-2 | world | recovered | recovered | recovered | recovered | missed |
| stockmarket-3 | definition | partial | partial | partial | recovered | missed |
| stockmarket-4 | world | recovered | recovered | recovered | recovered | missed |

## By class

Item-draws; draw A pools its three draws.

| variant | class | recovered | partial | stated, not built | missed |
|---|---|---|---|---|---|
| draw A | data-discoverable | 12 | 18 | 6 | 0 |
| draw A | definition | 3 | 5 | 0 | 10 |
| draw A | world | 12 | 0 | 0 | 3 |
| draw A | guidance | 16 | 7 | 0 | 1 |
| draw A | described | 18 | 0 | 0 | 0 |
| draw B | data-discoverable | 5 | 5 | 2 | 0 |
| draw B | definition | 6 | 0 | 0 | 0 |
| draw B | world | 5 | 0 | 0 | 0 |
| draw B | guidance | 7 | 1 | 0 | 0 |
| draw B | described | 6 | 0 | 0 | 0 |
| no-data | data-discoverable | 1 | 0 | 2 | 9 |
| no-data | definition | 0 | 0 | 0 | 6 |
| no-data | world | 0 | 0 | 0 | 5 |
| no-data | guidance | 1 | 3 | 2 | 2 |
| no-data | described | 6 | 0 | 0 | 0 |

Per draw, A1, A2 and A3 each recover 4 of the 12 data-discoverable items;
the three draws give the same category on every data-discoverable item.

## The offline predictions

- **(43) fails.** Draw A recovers 12 of 36 data-discoverable item-draws
  (33%), against at least half predicted. Every data-discoverable item-draw
  of draw A is at least partly right: none is missed. Counting *stated, not
  built* with *recovered* (exploratory) gives 18 of 36. The shortfall is
  record-level strictness more than missed knowledge:
  - crmarenapro-1 to -3 (9 item-draws, partial): every value a metric
    touches is cleaned correctly, but recovery needs a metric over all 59 id
    columns and all 31 padded text columns.
  - DEPS_DEV_V1-2 and -3 (6, partial): every draw misses one record of
    8,382 (and of 770), a project named "under the organization ...".
  - PANCANCER_ATLAS-3 (3, partial): the UUID is named but not extracted.
  - music_brainz_20k-3 and -4 (6, stated, not built): each draw lists the
    stored country and store values exactly, but no metric reads them.
- **(43c) holds.** The no-data control recovers 1 of 12 data-discoverable
  items (DEPS_DEV_V1-1, a three-column equality join the schema shows),
  against 4 per draw for draw A.
- **(43w) fails as pooled.** Draw A recovers 31 of 57 definition, world and
  guidance item-draws (54%) against 12 of 36 data-discoverable (33%). By
  class: definition 3 of 15 (20%), world 12 of 15 (80%), guidance 16 of 24
  (67%). Definition items are the hardest for an author without hints;
  world and guidance items, mostly code lists, joins and where a fact lives,
  are recovered often.
- **Described items** (apart): recovered in every draw, the no-data control
  included (18 of 18, 6 of 6, 6 of 6).

What draw B adds is mostly definitions and world knowledge: draw B
recovers 6 of 6 definition and 5 of 5 world item-draws against draw A's 20%
and 80%, and the same data-discoverable items as draw A, plus
PANCANCER_ATLAS-3.

## Rulings

Made before any draw was opened (the brief, and the checkers' notes in the
committed references):

- A join is built when the contract makes it executable: a declared
  relationship with its keys, or a metric's SQL that performs it. Prose alone
  is *stated, not built*.
- music_brainz_20k-2 is scored on its method (a key on normalized title and
  artist across the five source layouts); the grouping's difference from the
  reference is reported, not tested at 1e-6.
- crmarenapro-4: a draw whose built joins strip `#` on some relationships and
  not others is partial.
- stockmarket-1: "Q = NASDAQ" and the item's "Q = NASDAQ Global Select" both
  state the item. On this data Listing Exchange Q covers all of NASDAQ; the
  item's wording is wrong here.

Made in assembly, after reading the scores:

- *Stated, not built* stays as the rubric defines it ("or no metric reads the
  records"), although it makes a value-list item recoverable only through a
  metric (music_brainz_20k-3 and -4). The rubric was fixed before the draws;
  the recovered-or-stated count is reported beside (43) as exploratory.
- A correctable error is a fault in a contract's SQL or declared structure (a
  metric, a `from_sql`, a relationship, a rule, a table). A false statement
  in prose about data no item touches is listed as an addition confirmed
  "no". The checkers had split on this; the rule is applied by
  `dab_recovery.structural` and changes no score. It moved five prose errors
  in draws A1 and B (DEPS_DEV_V1 A1 twice and B, GITHUB_REPOS A1, stockmarket
  B), so neither draw's prose was edited, and three in draws not corrected
  (GITHUB_REPOS A3 twice, crmarenapro A2).

## Correctable errors and corrections

Seven, in SQL or structure, none touching a hint item:

- **draw B, GITHUB_REPOS:** `commit_author_name`'s regex fallback cut the
  name short on the two malformed author values. Corrected by a reviewer who
  saw no question, gold, validator or result; see `draw-b/CORRECTIONS.md`.
  The contract still passes the author's acceptance check.
- draw A2: bookreview `book_publication_year` (a year pattern drops one
  1885 date); music_brainz_20k `track_language` (tie-breaking maps
  abbreviations to regional variants). Not corrected (A2 is not).
- no-data: DEPS_DEV_V1 `upstream_published_at` (microseconds read as
  milliseconds); GITHUB_REPOS `truncated_difference_share` (a column NULL in
  every row); music_brainz_20k `track_length_seconds` (units by source);
  stockindex `trading_date` (one of three date formats). Not corrected.

Draw A1 had none and feeds `dab_contract` and `dab_contract_text` as
authored (`draw-a1/CORRECTIONS.md`). Recovery is scored on every draw as
written; the correction changes no score.

## Not committed

Record-level references and comparisons are in
`~/data/dce-held-results/exp7/recovery/<D>/`; DataAgentBench's data is not
ours to publish.
