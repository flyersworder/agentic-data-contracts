# Notice: material derived from DataAgentBench

The files in this directory, in `prep/dataagentbench/` and in
`results/dataagentbench/`, and the adapted system prompt in
`dce/benchmarks/dataagentbench.py`, are derived from DataAgentBench:

> Ruiying Ma, Shreya Shankar, Ruiqi Chen, Yiming Lin, Sepanta Zeighami,
> Rajoshi Ghosh, Abhinav Gupta, Anushrut Gupta, Tanmai Gopal and Aditya G.
> Parameswaran. *Can AI Agents Answer Your Data Questions? A Benchmark for
> Data Agents.* 2026. https://arxiv.org/abs/2603.20576
>
> Benchmark: https://github.com/ucbepic/DataAgentBench, used at commit
> `bcc2e1b06c708abfde5fa3c69e1e4e514eaaf716`.

What they contain:

- `prep/dataagentbench/hint-items.json` and
  `contracts-dataagentbench/recovery-work/*.items.json` quote DataAgentBench's
  hints (`db_description_withhint.txt`), split into 37 short statements, so
  that a contract can be scored against each one.
- `dce/benchmarks/dataagentbench.py` carries DataAgentBench's system prompt,
  adapted to this harness's tools.
- The contracts, author logs, checker references and result rows hold SQL
  written against the benchmark's datasets, small samples of its query
  results, and agents' answers to its queries.

These files are not covered by this repository's MIT license. The
DataAgentBench text quoted here belongs to its authors, and each underlying
dataset keeps the license of its original source. They are published only to
document and reproduce the experiment reported in the accompanying paper. The
benchmark's descriptions, queries, ground truths and validators are not
included; build them from the DataAgentBench repository at the commit above
(`dce.benchmarks.dab_data`).

If you hold rights in any of this material and want it changed or removed,
open an issue in this repository and we will act on it.
